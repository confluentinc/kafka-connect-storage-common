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

import io.confluent.connect.storage.backup.EnvelopeSchemaBuilder;
import io.confluent.connect.storage.backup.SchemaBackupStore;
import io.confluent.connect.storage.format.backup.BackupWrapperExtractor.Unwrapped;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Transforms SinkRecords into envelope-wrapped SinkRecords for backup mode.
 *
 * <p>Wraps each record in a {@code KafkaRecordEnvelope} struct that captures
 * the full record context (key, value, headers, topic, partition, offset,
 * timestamp) along with schema metadata. Schema file backup is delegated
 * to {@link SchemaBackupOrchestrator}.
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

  private final SchemaBackupOrchestrator schemaBackup;
  private final String keySchemaTypeDefault;
  private final String valueSchemaTypeDefault;

  public EnvelopeTransformer(
      SchemaBackupStore backupStore,
      String keySchemaTypeDefault,
      String valueSchemaTypeDefault) {
    this.schemaBackup = new SchemaBackupOrchestrator(backupStore);
    this.keySchemaTypeDefault = keySchemaTypeDefault;
    this.valueSchemaTypeDefault = valueSchemaTypeDefault;
  }

  /**
   * Wraps a batch of SinkRecords in envelope structs.
   * Also backs up schema files for any new schemas encountered.
   *
   * @param records the records from {@code put()}
   * @return wrapped records ready for {@code TopicPartitionWriter}
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
   *
   * @param record the original SinkRecord
   * @return a new SinkRecord with envelope schema and struct as value
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

    schemaBackup.backupIfNeeded(record.topic(), key);
    schemaBackup.backupIfNeeded(record.topic(), value);

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
}
