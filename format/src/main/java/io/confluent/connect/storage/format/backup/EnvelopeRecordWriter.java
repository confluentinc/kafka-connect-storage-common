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

import io.confluent.connect.storage.backup.BackupReferenceParser;
import io.confluent.connect.storage.backup.EnvelopeSchemaBuilder;
import io.confluent.connect.storage.backup.SchemaBackupStore;
import io.confluent.connect.storage.backup.SchemaManifest;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.backup.BackupWrapperExtractor.Unwrapped;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EnvelopeRecordWriter implements RecordWriter {

  private static final com.fasterxml.jackson.databind.ObjectMapper JSON =
      new com.fasterxml.jackson.databind.ObjectMapper();
  private static final Logger log =
      LoggerFactory.getLogger(EnvelopeRecordWriter.class);

  private final RecordWriter delegate;
  private final SchemaBackupStore backupStore;
  private final String keySchemaTypeDefault;
  private final String valueSchemaTypeDefault;

  public EnvelopeRecordWriter(
      RecordWriter delegate, SchemaBackupStore backupStore,
      String keySchemaTypeDefault,
      String valueSchemaTypeDefault) {
    this.delegate = delegate;
    this.backupStore = backupStore;
    this.keySchemaTypeDefault = keySchemaTypeDefault;
    this.valueSchemaTypeDefault = valueSchemaTypeDefault;
  }

  @Override
  public void write(SinkRecord record) {
    Unwrapped key = BackupWrapperExtractor.unwrap(
        record.key(), record.keySchema(), true, keySchemaTypeDefault);
    Unwrapped value = BackupWrapperExtractor.unwrap(
        record.value(), record.valueSchema(), false, valueSchemaTypeDefault);

    // Validate that at least one of key or value is non-null
    // While Kafka allows such records, they are semantically meaningless in envelope mode
    // (empty envelope with no data to preserve)
    if (key.getData() == null && value.getData() == null) {
      throw new org.apache.kafka.connect.errors.DataException(
          "Both key and value are null for record at topic=" + record.topic()
              + ", partition=" + record.kafkaPartition()
              + ", offset=" + record.kafkaOffset()
              + ". Envelope backup requires at least one of key or value to be non-null.");
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

    SinkRecord envelopeRecord = new SinkRecord(
        record.topic(), record.kafkaPartition(),
        null, null,
        envelopeSchema, envelope,
        record.kafkaOffset(),
        record.timestamp(),
        record.timestampType());

    delegate.write(envelopeRecord);
  }

  @SuppressWarnings("unchecked")
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
        refs,
        null);
  }

  @SuppressWarnings("unchecked")
  private void backupReferenceSchemas(String topic, String referenceTreeJson) {
    if (referenceTreeJson == null || referenceTreeJson.isEmpty()) {
      return;
    }
    try {
      java.util.Map<String, java.util.Map<String, Object>> tree =
          JSON.readValue(
              referenceTreeJson,
              new com.fasterxml.jackson.core.type.TypeReference<
                  java.util.Map<String, java.util.Map<String, Object>>>() {});

      Set<Integer> visited = new HashSet<>();
      for (java.util.Map.Entry<String, java.util.Map<String, Object>> entry
          : tree.entrySet()) {
        backupSingleReference(topic, entry.getValue(), tree, visited);
      }
    } catch (Exception e) {
      throw new org.apache.kafka.connect.errors.DataException(
          "Failed to backup reference schemas for topic="
          + topic + ". Cannot guarantee pristine restore.", e);
    }
  }

  @SuppressWarnings("unchecked")
  private void backupSingleReference(
      String topic, java.util.Map<String, Object> node,
      java.util.Map<String, java.util.Map<String, Object>> tree,
      Set<Integer> visited) {
    int globalId = node.get("globalId") instanceof Number
        ? ((Number) node.get("globalId")).intValue() : 0;
    if (globalId <= 0 || !visited.add(globalId)) {
      return;
    }
    String schema = (String) node.get("schema");
    String subject = (String) node.get("subject");
    int version = node.get("version") instanceof Number
        ? ((Number) node.get("version")).intValue() : 0;
    String schemaType = node.get("schemaType") instanceof String
        ? (String) node.get("schemaType") : null;
    if (schema == null || schemaType == null) {
      throw new org.apache.kafka.connect.errors.DataException(
          "Cannot backup reference schema: missing schema text or "
          + "type for globalId=" + globalId + ", subject=" + subject
          + ", topic=" + topic
          + ". Cannot guarantee pristine restore.");
    }
    java.util.List<SchemaManifest.SchemaReferenceEntry> childRefs =
        extractChildRefs(node, tree);
    backupStore.backupIfNeeded(
        topic, globalId, version, schemaType, subject, schema,
        childRefs, null);
  }

  @SuppressWarnings("unchecked")
  private java.util.List<SchemaManifest.SchemaReferenceEntry> extractChildRefs(
      java.util.Map<String, Object> node,
      java.util.Map<String, java.util.Map<String, Object>> tree) {
    Object refsObj = node.get("references");
    if (!(refsObj instanceof java.util.List)) {
      return java.util.Collections.emptyList();
    }
    java.util.List<SchemaManifest.SchemaReferenceEntry> childRefs =
        new java.util.ArrayList<>();
    for (Object item : (java.util.List<?>) refsObj) {
      if (!(item instanceof java.util.Map)) {
        continue;
      }
      java.util.Map<String, Object> m = (java.util.Map<String, Object>) item;
      String refName = (String) m.get("name");
      String refSubject = (String) m.get("subject");
      int refVersion = m.get("version") instanceof Number
          ? ((Number) m.get("version")).intValue() : 0;
      java.util.Map<String, Object> childNode = tree.get(refName);
      int refGlobalId = childNode != null
          && childNode.get("globalId") instanceof Number
          ? ((Number) childNode.get("globalId")).intValue() : 0;
      childRefs.add(new SchemaManifest.SchemaReferenceEntry(
          refName, refSubject, refVersion, refGlobalId));
    }
    return childRefs;
  }

  @Override
  public void commit() {
    delegate.commit();
  }

  @Override
  public void close() {
    delegate.close();
  }
}
