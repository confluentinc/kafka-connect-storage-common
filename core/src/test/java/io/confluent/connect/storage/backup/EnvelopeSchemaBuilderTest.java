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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import java.util.List;
import org.junit.Test;

public class EnvelopeSchemaBuilderTest {

  @Test
  public void testBuildWithStructSchemas() {
    Schema keySchema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA).build();
    Schema valueSchema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA).build();

    Schema envelope = EnvelopeSchemaBuilder.buildEnvelopeSchema(
        keySchema, valueSchema, "AVRO", "AVRO");

    assertEquals(BackupEnvelope.NAME, envelope.name());
    assertEquals(keySchema,
        envelope.field(BackupEnvelope.FIELD_KEY).schema());
    assertEquals(valueSchema,
        envelope.field(BackupEnvelope.FIELD_VALUE).schema());
  }

  @Test
  public void testBuildWithNullSchemas() {
    Schema envelope = EnvelopeSchemaBuilder.buildEnvelopeSchema(
        null, null, "JSON_SCHEMALESS", "JSON_SCHEMALESS");

    assertEquals(Schema.OPTIONAL_STRING_SCHEMA,
        envelope.field(BackupEnvelope.FIELD_KEY).schema());
    assertEquals(Schema.OPTIONAL_STRING_SCHEMA,
        envelope.field(BackupEnvelope.FIELD_VALUE).schema());
  }

  @Test
  public void testBuildWithMixedSchemas() {
    Schema valueSchema = SchemaBuilder.struct()
        .field("data", Schema.STRING_SCHEMA).build();

    Schema envelope = EnvelopeSchemaBuilder.buildEnvelopeSchema(
        Schema.STRING_SCHEMA, valueSchema, "STRING", "AVRO");

    assertEquals(Schema.STRING_SCHEMA,
        envelope.field(BackupEnvelope.FIELD_KEY).schema());
    assertEquals(valueSchema,
        envelope.field(BackupEnvelope.FIELD_VALUE).schema());
  }

  @Test
  public void testBuildWithPrimitiveSchemas() {
    Schema envelope = EnvelopeSchemaBuilder.buildEnvelopeSchema(
        Schema.STRING_SCHEMA, Schema.INT32_SCHEMA,
        "STRING", "INT32");

    assertEquals(Schema.STRING_SCHEMA,
        envelope.field(BackupEnvelope.FIELD_KEY).schema());
    assertEquals(Schema.INT32_SCHEMA,
        envelope.field(BackupEnvelope.FIELD_VALUE).schema());
  }

  @Test
  public void testEnvelopeHasAllMetadataFields() {
    Schema envelope = EnvelopeSchemaBuilder.buildEnvelopeSchema(
        null, null, "NONE", "NONE");

    assertNotNull(envelope.field(BackupEnvelope.FIELD_TOPIC));
    assertNotNull(envelope.field(BackupEnvelope.FIELD_PARTITION));
    assertNotNull(envelope.field(BackupEnvelope.FIELD_OFFSET));
    assertNotNull(envelope.field(BackupEnvelope.FIELD_TIMESTAMP));
    assertNotNull(
        envelope.field(BackupEnvelope.FIELD_TIMESTAMP_TYPE));
    assertNotNull(
        envelope.field(BackupEnvelope.FIELD_KEY_SCHEMA_ID));
    assertNotNull(
        envelope.field(BackupEnvelope.FIELD_VALUE_SCHEMA_ID));
    assertNotNull(
        envelope.field(BackupEnvelope.FIELD_KEY_SCHEMA_TYPE));
    assertNotNull(
        envelope.field(BackupEnvelope.FIELD_VALUE_SCHEMA_TYPE));
    assertNotNull(
        envelope.field(BackupEnvelope.FIELD_KEY_SCHEMA_SUBJECT));
    assertNotNull(
        envelope.field(BackupEnvelope.FIELD_VALUE_SCHEMA_SUBJECT));
    assertNotNull(
        envelope.field(BackupEnvelope.FIELD_KEY_SCHEMA_GUID));
    assertNotNull(
        envelope.field(BackupEnvelope.FIELD_VALUE_SCHEMA_GUID));
    assertNotNull(envelope.field(BackupEnvelope.FIELD_HEADERS));
  }

  @Test
  public void testBuildEnvelopeStructAllFields() {
    Schema keySchema = Schema.STRING_SCHEMA;
    Schema valueSchema = SchemaBuilder.struct()
        .field("x", Schema.INT32_SCHEMA).build();
    Schema envSchema = EnvelopeSchemaBuilder.buildEnvelopeSchema(
        keySchema, valueSchema, "STRING", "AVRO");

    Struct value = new Struct(valueSchema).put("x", 99);
    ConnectHeaders headers = new ConnectHeaders();
    headers.addString("h1", "val1");

    SinkRecord record = new SinkRecord(
        "test-topic", 0, keySchema, "key1",
        valueSchema, value, 100L,
        1234567890L,
        TimestampType.CREATE_TIME,
        headers);

    Struct envelope = EnvelopeSchemaBuilder.buildEnvelopeStruct(
        envSchema, record, "key1", value,
        new EnvelopeSchemaBuilder.SchemaDescriptor(null, "STRING", null, null),
        new EnvelopeSchemaBuilder.SchemaDescriptor(42, "AVRO", "test-value", null));

    assertEquals("key1",
        envelope.get(BackupEnvelope.FIELD_KEY));
    assertEquals(value,
        envelope.get(BackupEnvelope.FIELD_VALUE));
    assertEquals("test-topic",
        envelope.getString(BackupEnvelope.FIELD_TOPIC));
    assertEquals(0,
        (int) envelope.getInt32(BackupEnvelope.FIELD_PARTITION));
    assertEquals(100L,
        (long) envelope.getInt64(BackupEnvelope.FIELD_OFFSET));
    assertEquals(1234567890L,
        (long) envelope.getInt64(BackupEnvelope.FIELD_TIMESTAMP));
    assertEquals("CREATE_TIME",
        envelope.getString(
            BackupEnvelope.FIELD_TIMESTAMP_TYPE));
    assertNull(
        envelope.getInt32(BackupEnvelope.FIELD_KEY_SCHEMA_ID));
    assertEquals(42,
        (int) envelope.getInt32(
            BackupEnvelope.FIELD_VALUE_SCHEMA_ID));
    assertEquals("STRING",
        envelope.getString(
            BackupEnvelope.FIELD_KEY_SCHEMA_TYPE));
    assertEquals("AVRO",
        envelope.getString(
            BackupEnvelope.FIELD_VALUE_SCHEMA_TYPE));
    assertNull(
        envelope.getString(
            BackupEnvelope.FIELD_KEY_SCHEMA_SUBJECT));
    assertEquals("test-value",
        envelope.getString(
            BackupEnvelope.FIELD_VALUE_SCHEMA_SUBJECT));

    @SuppressWarnings("unchecked")
    List<Struct> hdrs = (List<Struct>) envelope.get(
        BackupEnvelope.FIELD_HEADERS);
    assertEquals(1, hdrs.size());
    assertEquals("h1", hdrs.get(0).getString(
        BackupEnvelope.FIELD_HEADER_KEY));
    assertEquals("val1", hdrs.get(0).getString(
        BackupEnvelope.FIELD_HEADER_VALUE));
    assertEquals("STRING", hdrs.get(0).getString(
        BackupEnvelope.FIELD_HEADER_SCHEMA_TYPE));
  }

  @Test
  public void testBuildEnvelopeStructNullOptionals() {
    Schema envSchema = EnvelopeSchemaBuilder.buildEnvelopeSchema(
        null, null, "NONE", "NONE");

    SinkRecord record = new SinkRecord(
        "t", 0, null, null, null, null, 0L);

    Struct envelope = EnvelopeSchemaBuilder.buildEnvelopeStruct(
        envSchema, record, null, null,
        new EnvelopeSchemaBuilder.SchemaDescriptor(null, "NONE", null, null),
        new EnvelopeSchemaBuilder.SchemaDescriptor(null, "NONE", null, null));

    assertNull(envelope.get(BackupEnvelope.FIELD_KEY));
    assertNull(envelope.get(BackupEnvelope.FIELD_VALUE));
    assertNull(
        envelope.getInt32(BackupEnvelope.FIELD_KEY_SCHEMA_ID));
    assertNull(
        envelope.getInt32(BackupEnvelope.FIELD_VALUE_SCHEMA_ID));
    assertNull(
        envelope.getString(
            BackupEnvelope.FIELD_KEY_SCHEMA_SUBJECT));
    assertNull(
        envelope.getString(
            BackupEnvelope.FIELD_VALUE_SCHEMA_SUBJECT));

    @SuppressWarnings("unchecked")
    List<Struct> hdrs = (List<Struct>) envelope.get(
        BackupEnvelope.FIELD_HEADERS);
    assertTrue(hdrs.isEmpty());
  }

  @Test
  public void testBuildHeadersMultipleTypes() {
    Schema keySchema = Schema.STRING_SCHEMA;
    Schema envSchema = EnvelopeSchemaBuilder.buildEnvelopeSchema(
        keySchema, null, "STRING", "NONE");

    ConnectHeaders headers = new ConnectHeaders();
    headers.addString("str", "text");
    headers.addInt("num", 42);
    headers.addBoolean("flag", true);

    SinkRecord record = new SinkRecord(
        "t", 0, keySchema, "k", null, null, 0L,
        null, null, headers);

    Struct envelope = EnvelopeSchemaBuilder.buildEnvelopeStruct(
        envSchema, record, "k", null,
        new EnvelopeSchemaBuilder.SchemaDescriptor(null, "STRING", null, null),
        new EnvelopeSchemaBuilder.SchemaDescriptor(null, "NONE", null, null));

    @SuppressWarnings("unchecked")
    List<Struct> hdrs = (List<Struct>) envelope.get(
        BackupEnvelope.FIELD_HEADERS);
    assertEquals(3, hdrs.size());

    assertEquals("str", hdrs.get(0).getString(
        BackupEnvelope.FIELD_HEADER_KEY));
    assertEquals("text", hdrs.get(0).getString(
        BackupEnvelope.FIELD_HEADER_VALUE));
    assertEquals("STRING", hdrs.get(0).getString(
        BackupEnvelope.FIELD_HEADER_SCHEMA_TYPE));

    assertEquals("num", hdrs.get(1).getString(
        BackupEnvelope.FIELD_HEADER_KEY));
    assertEquals("42", hdrs.get(1).getString(
        BackupEnvelope.FIELD_HEADER_VALUE));
    assertEquals("INT32", hdrs.get(1).getString(
        BackupEnvelope.FIELD_HEADER_SCHEMA_TYPE));

    assertEquals("flag", hdrs.get(2).getString(
        BackupEnvelope.FIELD_HEADER_KEY));
    assertEquals("true", hdrs.get(2).getString(
        BackupEnvelope.FIELD_HEADER_VALUE));
    assertEquals("BOOLEAN", hdrs.get(2).getString(
        BackupEnvelope.FIELD_HEADER_SCHEMA_TYPE));
  }
}
