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

import io.confluent.connect.storage.backup.ConverterTypeDetector;
import io.confluent.connect.storage.backup.ObjectStoreSchemaBackupStore;
import io.confluent.connect.storage.backup.SchemaBackupStore;
import io.confluent.connect.storage.backup.StorageWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;

import java.util.Map;

/**
 * Factory for creating the envelope writer pipeline. Encapsulates the wiring
 * that was previously in each backend's SinkTask, making it backend-agnostic.
 *
 * <p>Each backend provides its own {@link StorageWriter} implementation
 * (S3StorageWriter, GcsStorageWriter, etc.) and calls this factory.
 */
public final class BackupEnvelopeWriterFactory {

  private BackupEnvelopeWriterFactory() {
  }

  /**
   * Create an EnvelopeRecordWriterProvider wrapping the given format writer.
   *
   * @param formatWriter the base format writer (Avro, JSON, Parquet)
   * @param storageWriter the backend-specific storage writer
   * @param taskConfig the task configuration map
   * @param topicsDir the topics directory prefix
   * @param dirDelim the directory delimiter
   * @param <C> the connector config type
   * @return a decorated provider that wraps records in envelopes
   */
  public static <C> RecordWriterProvider<C> create(
      RecordWriterProvider<C> formatWriter,
      StorageWriter storageWriter,
      Map<String, String> taskConfig,
      String topicsDir,
      String dirDelim) {
    SchemaBackupStore backupStore =
        new ObjectStoreSchemaBackupStore(storageWriter, topicsDir, dirDelim);
    String keyType = ConverterTypeDetector.detectSchemaType(
        taskConfig.get("key.converter"), taskConfig, "key.converter.");
    String valueType = ConverterTypeDetector.detectSchemaType(
        taskConfig.get("value.converter"), taskConfig, "value.converter.");
    return new EnvelopeRecordWriterProvider<>(
        formatWriter, backupStore, keyType, valueType);
  }
}
