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
import io.confluent.connect.storage.backup.SchemaBackupStore;
import io.confluent.connect.storage.backup.SchemaManifest;
import io.confluent.connect.storage.format.backup.BackupWrapperExtractor.Unwrapped;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Orchestrates schema file backup during envelope wrapping.
 *
 * <p>Determines which schemas need to be backed up, traverses reference
 * trees to back up transitive references, and delegates the actual
 * file I/O to {@link SchemaBackupStore}.
 *
 * <p>Handles both root schemas and their reference chains. Reference
 * trees are parsed from the Wrapper's JSON metadata and each referenced
 * schema is backed up with its entry file.
 */
final class SchemaBackupOrchestrator {

  private static final Logger log =
      LoggerFactory.getLogger(SchemaBackupOrchestrator.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  private final SchemaBackupStore backupStore;

  SchemaBackupOrchestrator(SchemaBackupStore backupStore) {
    this.backupStore = backupStore;
  }

  /**
   * Backs up schema and entry files for an unwrapped record if needed.
   * Handles both the root schema and any transitive references.
   *
   * @param topic the Kafka topic name
   * @param unwrapped the unwrapped record metadata
   */
  void backupIfNeeded(String topic, Unwrapped unwrapped) {
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
