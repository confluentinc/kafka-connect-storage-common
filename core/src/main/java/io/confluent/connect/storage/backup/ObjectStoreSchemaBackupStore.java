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

package io.confluent.connect.storage.backup;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Backs up schemas as per-schema files: {id}.{ext} + {id}.entry.json.
 * Three-level dedup: in-memory ConcurrentSet → exists() → write.
 * No shared manifest. Zero contention between tasks.
 */
public class ObjectStoreSchemaBackupStore implements SchemaBackupStore {

  private static final Logger log =
      LoggerFactory.getLogger(ObjectStoreSchemaBackupStore.class);
  private final StorageWriter writer;
  private final String topicsDir;
  private final String dirDelim;
  private final Set<String> backedUpKeys =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  public ObjectStoreSchemaBackupStore(
      StorageWriter writer, String topicsDir, String dirDelim) {
    this.writer = writer;
    this.topicsDir = topicsDir;
    this.dirDelim = dirDelim;
  }

  @Override
  public void backupIfNeeded(
      String topic, int schemaId, int version,
      String schemaType, String subject, String rawSchema,
      List<SchemaManifest.SchemaReferenceEntry> references) {
    if (schemaId <= 0) {
      log.warn("Invalid schemaId={} for topic={}, skipping backup", schemaId, topic);
      return;
    }
    String key = topic + ":" + schemaId;

    // Level 1: in-memory dedup (hot path, zero I/O)
    if (backedUpKeys.contains(key)) {
      return;
    }

    String entryPath = schemasPath(topic) + schemaId + BackupEnvelope.ENTRY_FILE_SUFFIX;

    // Level 2: storage exists check (cold path, after restart)
    if (writer.exists(entryPath)) {
      backedUpKeys.add(key);
      log.debug("Schema already backed up (exists check): topic={}, id={}",
          topic, schemaId);
      return;
    }

    // Level 3: write schema file + entry file
    String ext = extensionFor(schemaType);
    String schemaPath = schemasPath(topic) + schemaId + ext;
    writer.write(schemaPath, rawSchema);

    SchemaManifest.SchemaEntry entry = new SchemaManifest.SchemaEntry(
        schemaId, schemaType, subject, version,
        schemaId + ext, references);
    String entryJson;
    try {
      entryJson = entry.toJsonString();
    } catch (JsonProcessingException e) {
      throw new DataException(
          "Failed to serialize schema entry for schemaId=" + schemaId, e);
    }
    writer.write(entryPath, entryJson);

    backedUpKeys.add(key);
    log.info("Backed up schema: topic={}, id={}, subject={}, refs={}",
        topic, schemaId, subject,
        references != null ? references.size() : 0);
  }

  private String schemasPath(String topic) {
    StringBuilder sb = new StringBuilder();
    if (topicsDir != null && !topicsDir.isEmpty()) {
      sb.append(topicsDir).append(dirDelim);
    }
    sb.append(topic).append(dirDelim)
        .append(BackupEnvelope.METADATA_DIR).append(dirDelim)
        .append(BackupEnvelope.SCHEMAS_DIR).append(dirDelim);
    return sb.toString();
  }

  private String extensionFor(String type) {
    return BackupEnvelope.extensionForType(type);
  }
}
