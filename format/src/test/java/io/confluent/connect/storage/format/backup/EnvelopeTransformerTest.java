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

import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.connect.storage.backup.BackupEnvelope;
import io.confluent.connect.storage.backup.SchemaBackupStore;
import io.confluent.connect.storage.backup.SchemaManifest;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class EnvelopeTransformerTest {

  private static final String TOPIC = "test-topic";
  private static final int PARTITION = 0;
  private static final long OFFSET = 100L;
  private static final long TIMESTAMP = 1234567890L;
  private static final String SCHEMA_TYPE_AVRO = "AVRO";
  private static final String SCHEMA_TYPE_STRING = "STRING";
  private static final String RAW_SCHEMA = "{\"type\":\"record\",\"name\":\"User\","
      + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";

  private SchemaBackupStore backupStore;
  private EnvelopeTransformer transformer;

  @Before
  public void setUp() {
    backupStore = mock(SchemaBackupStore.class);
    transformer = new EnvelopeTransformer(
        backupStore, SCHEMA_TYPE_STRING, SCHEMA_TYPE_AVRO);
  }

  @Test
  public void wrapRecordWithAvroKeyAndValue() {
    Schema dataSchema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA).build();
    Schema wrapperSchema = BackupWrapper.buildSchema(dataSchema);
    Struct data = new Struct(dataSchema).put("name", "Alice");
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        42, 1, SCHEMA_TYPE_AVRO, "test-value", RAW_SCHEMA, null, null);
    Struct wrapper = BackupWrapper.buildWrapper(wrapperSchema, data, fields);

    SinkRecord record = new SinkRecord(
        TOPIC, PARTITION,
        Schema.STRING_SCHEMA, "key1",
        wrapperSchema, wrapper,
        OFFSET, TIMESTAMP, TimestampType.CREATE_TIME);

    SinkRecord result = transformer.wrap(record);

    assertNotNull(result);
    assertEquals(TOPIC, result.topic());
    assertEquals(PARTITION, (int) result.kafkaPartition());
    assertEquals(OFFSET, (long) result.kafkaOffset());
    assertNotNull(result.valueSchema());
    assertEquals(BackupEnvelope.NAME, result.valueSchema().name());
    assertTrue(result.value() instanceof Struct);
  }

  @Test
  public void wrapRecordWithNullKey() {
    Schema dataSchema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA).build();
    Schema wrapperSchema = BackupWrapper.buildSchema(dataSchema);
    Struct data = new Struct(dataSchema).put("name", "Alice");
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        42, 1, SCHEMA_TYPE_AVRO, "test-value", RAW_SCHEMA, null, null);
    Struct wrapper = BackupWrapper.buildWrapper(wrapperSchema, data, fields);

    SinkRecord record = new SinkRecord(
        TOPIC, PARTITION, null, null,
        wrapperSchema, wrapper, OFFSET);

    SinkRecord result = transformer.wrap(record);

    assertNotNull(result);
    Struct envelope = (Struct) result.value();
    assertNotNull(envelope.get(BackupEnvelope.FIELD_VALUE));
  }

  @Test
  public void wrapRecordWithNullValue() {
    SinkRecord record = new SinkRecord(
        TOPIC, PARTITION,
        Schema.STRING_SCHEMA, "key1",
        null, null, OFFSET);

    SinkRecord result = transformer.wrap(record);

    assertNotNull(result);
    Struct envelope = (Struct) result.value();
    assertEquals("key1", envelope.get(BackupEnvelope.FIELD_KEY));
  }

  @Test(expected = DataException.class)
  public void wrapRecordWithBothNullThrows() {
    SinkRecord record = new SinkRecord(
        TOPIC, PARTITION, null, null, null, null, OFFSET);

    transformer.wrap(record);
  }

  @Test
  public void wrapAllEmptyCollection() {
    Collection<SinkRecord> result = transformer.wrapAll(Collections.emptyList());

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void wrapAllMultipleRecords() {
    List<SinkRecord> records = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      records.add(new SinkRecord(
          TOPIC, PARTITION,
          Schema.STRING_SCHEMA, "key" + i,
          Schema.STRING_SCHEMA, "value" + i, i));
    }

    Collection<SinkRecord> results = transformer.wrapAll(records);

    assertEquals(3, results.size());
    List<SinkRecord> resultList = new ArrayList<>(results);
    for (int i = 0; i < 3; i++) {
      assertEquals(TOPIC, resultList.get(i).topic());
      assertEquals((long) i, (long) resultList.get(i).kafkaOffset());
    }
  }

  @Test
  public void wrapPreservesTopicPartitionOffset() {
    SinkRecord record = new SinkRecord(
        TOPIC, PARTITION,
        Schema.STRING_SCHEMA, "key1",
        Schema.STRING_SCHEMA, "value1",
        OFFSET, TIMESTAMP, TimestampType.CREATE_TIME);

    SinkRecord result = transformer.wrap(record);

    assertEquals(TOPIC, result.topic());
    assertEquals(PARTITION, (int) result.kafkaPartition());
    assertEquals(OFFSET, (long) result.kafkaOffset());
    assertEquals(TIMESTAMP, (long) result.timestamp());
    assertEquals(TimestampType.CREATE_TIME, result.timestampType());
  }

  @Test
  public void backupStoreCalledForSchema() {
    Schema dataSchema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA).build();
    Schema wrapperSchema = BackupWrapper.buildSchema(dataSchema);
    Struct data = new Struct(dataSchema).put("name", "Alice");
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        42, 1, SCHEMA_TYPE_AVRO, "test-value", RAW_SCHEMA, null, null);
    Struct wrapper = BackupWrapper.buildWrapper(wrapperSchema, data, fields);

    SinkRecord record = new SinkRecord(
        TOPIC, PARTITION, null, null,
        wrapperSchema, wrapper, OFFSET);

    transformer.wrap(record);

    verify(backupStore).backupIfNeeded(
        anyString(), anyString(), anyInt(), anyString(),
        anyString(), anyString(), anyList());
  }
}
