/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.storage.format.backup;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.storage.backup.BackupEnvelope;
import io.confluent.connect.storage.backup.BackupReferenceParser;
import io.confluent.connect.storage.backup.EnvelopeSchemaBuilder;
import io.confluent.connect.storage.backup.SchemaBackupStore;
import io.confluent.connect.storage.backup.SchemaManifest;
import io.confluent.connect.storage.format.backup.BackupWrapperExtractor.Unwrapped;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Transforms SinkRecords into envelope-wrapped SinkRecords for backup mode.
 *
 * <p>This transformer wraps each record in an envelope struct that captures
 * the full record context (key, value, headers, topic, partition, offset,
 * timestamp) along with schema metadata. It also backs up schema files
 * (.entry.json + .avsc/.proto/.json) to object storage.
 *
 * <p>The resulting envelope SinkRecord has a consistent, non-null schema
 * that changes only when the key or value schema changes — including
 * tombstone transitions. This allows {@code TopicPartitionWriter} to
 * handle file rotation naturally via its existing schema change detection.
 *
 * <p>Created in {@code BackupSinkTask.start()}, used in {@code put()},
 * released in {@code stop()}.
 */
public class EnvelopeTransformer {

  private static final Logger log = LoggerFactory.getLogger(EnvelopeTransformer.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  private final SchemaBackupStore backupStore;
  private final String keySchemaTypeDefault;
  private final String valueSchemaTypeDefault;

  public EnvelopeTransformer(
      SchemaBackupStore backupStore,
      String keySchemaTypeDefault,
      String valueSchemaTypeDefault) {
    this.backupStore = backupStore;
    this.keySchemaTypeDefault = keySchemaTypeDefault;
    this.valueSchemaTypeDefault = valueSchemaTypeDefault;
  }

  /**
   * Wraps a batch of SinkRecords in envelope structs.
   * Also backs up schema files for any new schemas encountered.
   */
  public Collection<SinkRecord> wrapAll(Collection<SinkRecord> records) {
    List<SinkRecord> wrapped = new ArrayList<>(records.size());
    for (SinkRecord record : records) {
      wrapped.add(wrap(record));
    }
    return wrapped;
  }

  /**
   * Wraps a single SinkRecord in an envelope struct.
   * Backs up schema files as a side effect (idempotent).
   */
  public SinkRecord wrap(SinkRecord record) {
    Unwrapped key = BackupWrapperExtractor.unwrap(
        record.key(), record.keySchema(), true, keySchemaTypeDefault);
    Unwrapped value = BackupWrapperExtractor.unwrap(
        record.value(), record.valueSchema(), false, valueSchemaTypeDefault);

    if (key.getData() == null && value.getData() == null) {
      throw new DataException(
          "Both key and value are null for record at topic=" + record.topic()
              + ", partition=" + record.kafkaPartition()
              + ", offset=" + record.kafkaOffset()
              + ". Envelope backup requires at least one non-null.");
    }

    log.debug("Envelope: topic={}, offset={}, keyType={}, valType={}, keyId={}, valId={}",
        record.topic(), record.kafkaOffset(),
        key.getSchemaType(), value.getSchemaType(),
        key.getSchemaId(), value.getSchemaId());

    backupSchemaIfNeeded(record.topic(), key);
    backupSchemaIfNeeded(record.topic(), value);

    Schema envelopeSchema = EnvelopeSchemaBuilder
        .buildEnvelopeSchema(key.getSchema(), value.getSchema(),
            key.getSchemaType(), value.getSchemaType());

    Struct envelope = EnvelopeSchemaBuilder
        .buildEnvelopeStruct(envelopeSchema, record,
            key.getData(), value.getData(),
            key.getSchemaId(), value.getSchemaId(),
            key.getSchemaType(), value.getSchemaType(),
            key.getSubject(), value.getSubject());

    return new SinkRecord(
        record.topic(), record.kafkaPartition(),
        null, null,
        envelopeSchema, envelope,
        record.kafkaOffset(),
        record.timestamp(),
        record.timestampType());
  }

  private void backupSchemaIfNeeded(String topic, Unwrapped unwrapped) {
    if (unwrapped.getSchemaId() == null || unwrapped.getRawSchema() == null) {
      return;
    }
    backupReferenceSchemas(topic, unwrapped.getReferenceTreeJson());
    List<SchemaManifest.SchemaReferenceEntry> refs =
        BackupReferenceParser.parseDirectRefsToManifestEntries(
            unwrapped.getDirectRefsJson(),
            unwrapped.getReferenceTreeJson());
    backupStore.backupIfNeeded(
        topic,
        unwrapped.getSchemaId(),
        unwrapped.getSchemaVersion() != null ? unwrapped.getSchemaVersion() : 0,
        unwrapped.getSchemaType(),
        unwrapped.getSubject(),
        unwrapped.getRawSchema(),
        refs);
  }

  @SuppressWarnings("unchecked")
  private void backupReferenceSchemas(String topic, String referenceTreeJson) {
    if (referenceTreeJson == null || referenceTreeJson.isEmpty()) {
      return;
    }
    try {
      Map<String, Map<String, Object>> tree = JSON.readValue(
          referenceTreeJson,
          new TypeReference<Map<String, Map<String, Object>>>() {});
      Set<Integer> visited = new HashSet<>();
      for (Map.Entry<String, Map<String, Object>> entry : tree.entrySet()) {
        backupSingleReference(topic, entry.getValue(), tree, visited);
      }
    } catch (DataException de) {
      throw de;
    } catch (Exception e) {
      throw new DataException(
          "Failed to backup reference schemas for topic="
          + topic + ". Cannot guarantee pristine restore.", e);
    }
  }

  @SuppressWarnings("unchecked")
  private void backupSingleReference(
      String topic, Map<String, Object> node,
      Map<String, Map<String, Object>> tree, Set<Integer> visited) {
    int globalId = node.get(BackupEnvelope.REF_FIELD_GLOBAL_ID) instanceof Number
        ? ((Number) node.get(BackupEnvelope.REF_FIELD_GLOBAL_ID)).intValue() : 0;
    if (globalId <= 0 || !visited.add(globalId)) {
      if (globalId <= 0) {
        log.warn("Skipping reference with invalid globalId={} in topic={}",
            globalId, topic);
      }
      return;
    }
    String schema = (String) node.get(BackupEnvelope.REF_FIELD_SCHEMA);
    String subject = (String) node.get(BackupEnvelope.REF_FIELD_SUBJECT);
    int version = node.get(BackupEnvelope.REF_FIELD_VERSION) instanceof Number
        ? ((Number) node.get(BackupEnvelope.REF_FIELD_VERSION)).intValue() : 0;
    String schemaType = node.get(BackupEnvelope.REF_FIELD_SCHEMA_TYPE) instanceof String
        ? (String) node.get(BackupEnvelope.REF_FIELD_SCHEMA_TYPE) : null;
    if (schema == null || schemaType == null) {
      throw new DataException(
          "Cannot backup reference schema: missing schema text or type"
          + " for globalId=" + globalId + ", subject=" + subject
          + ", topic=" + topic + ". Cannot guarantee pristine restore.");
    }
    List<SchemaManifest.SchemaReferenceEntry> childRefs =
        extractChildRefs(node, tree);
    backupStore.backupIfNeeded(
        topic, globalId, version, schemaType, subject, schema, childRefs);
  }

  @SuppressWarnings("unchecked")
  private List<SchemaManifest.SchemaReferenceEntry> extractChildRefs(
      Map<String, Object> node, Map<String, Map<String, Object>> tree) {
    Object refsObj = node.get(BackupEnvelope.REF_FIELD_REFERENCES);
    if (!(refsObj instanceof List)) {
      return Collections.emptyList();
    }
    List<SchemaManifest.SchemaReferenceEntry> childRefs = new ArrayList<>();
    for (Object item : (List<?>) refsObj) {
      if (!(item instanceof Map)) {
        continue;
      }
      Map<String, Object> m = (Map<String, Object>) item;
      String refName = (String) m.get(BackupEnvelope.REF_FIELD_NAME);
      String refSubject = (String) m.get(BackupEnvelope.REF_FIELD_SUBJECT);
      int refVersion = m.get(BackupEnvelope.REF_FIELD_VERSION) instanceof Number
          ? ((Number) m.get(BackupEnvelope.REF_FIELD_VERSION)).intValue() : 0;
      Map<String, Object> childNode = tree.get(refName);
      int refGlobalId = childNode != null
          && childNode.get(BackupEnvelope.REF_FIELD_GLOBAL_ID) instanceof Number
          ? ((Number) childNode.get(BackupEnvelope.REF_FIELD_GLOBAL_ID)).intValue() : 0;
      childRefs.add(new SchemaManifest.SchemaReferenceEntry(
          refName, refSubject, refVersion, refGlobalId));
    }
    return childRefs;
  }
}
