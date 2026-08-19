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

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ConverterTypeDetectorTest {

  private static final String KEY_CONVERTER = BackupEnvelope.KEY_CONVERTER_CONFIG;
  private static final String VALUE_CONVERTER = BackupEnvelope.VALUE_CONVERTER_CONFIG;

  private static final String AVRO_CONVERTER = "io.confluent.connect.avro.AvroConverter";
  private static final String PROTOBUF_CONVERTER = "io.confluent.connect.protobuf.ProtobufConverter";
  private static final String JSON_SCHEMA_CONVERTER = "io.confluent.connect.json.JsonSchemaConverter";
  private static final String STRING_CONVERTER = "org.apache.kafka.connect.storage.StringConverter";
  private static final String JSON_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
  private static final String INTEGER_CONVERTER = "org.apache.kafka.connect.converters.IntegerConverter";
  private static final String LONG_CONVERTER = "org.apache.kafka.connect.converters.LongConverter";
  private static final String BYTE_ARRAY_CONVERTER = "org.apache.kafka.connect.converters.ByteArrayConverter";

  @Test
  public void testAvroConverter() {
    assertEquals(BackupEnvelope.TYPE_AVRO,
        ConverterTypeDetector.detectSchemaType(
            AVRO_CONVERTER, Collections.emptyMap(), VALUE_CONVERTER));
  }

  @Test
  public void testProtobufConverter() {
    assertEquals(BackupEnvelope.TYPE_PROTOBUF,
        ConverterTypeDetector.detectSchemaType(
            PROTOBUF_CONVERTER, Collections.emptyMap(), VALUE_CONVERTER));
  }

  @Test
  public void testJsonSchemaConverter() {
    assertEquals(BackupEnvelope.TYPE_JSON_SCHEMA,
        ConverterTypeDetector.detectSchemaType(
            JSON_SCHEMA_CONVERTER, Collections.emptyMap(), VALUE_CONVERTER));
  }

  @Test
  public void testStringConverter() {
    assertEquals(BackupEnvelope.TYPE_STRING,
        ConverterTypeDetector.detectSchemaType(
            STRING_CONVERTER, Collections.emptyMap(), VALUE_CONVERTER));
  }

  @Test
  public void testJsonConverterSchemaless() {
    Map<String, String> config = new HashMap<>();
    config.put(VALUE_CONVERTER + ".schemas.enable", "false");
    assertEquals(BackupEnvelope.TYPE_JSON_SCHEMALESS,
        ConverterTypeDetector.detectSchemaType(
            JSON_CONVERTER, config, VALUE_CONVERTER));
  }

  @Test
  public void testJsonConverterWithSchema() {
    Map<String, String> config = new HashMap<>();
    config.put(VALUE_CONVERTER + ".schemas.enable", "true");
    assertEquals(BackupEnvelope.TYPE_JSON_EMBEDDED_SCHEMA,
        ConverterTypeDetector.detectSchemaType(
            JSON_CONVERTER, config, VALUE_CONVERTER));
  }

  @Test
  public void testIntegerConverter() {
    assertEquals(BackupEnvelope.TYPE_INT32,
        ConverterTypeDetector.detectSchemaType(
            INTEGER_CONVERTER, Collections.emptyMap(), KEY_CONVERTER));
  }

  @Test
  public void testLongConverter() {
    assertEquals(BackupEnvelope.TYPE_INT64,
        ConverterTypeDetector.detectSchemaType(
            LONG_CONVERTER, Collections.emptyMap(), KEY_CONVERTER));
  }

  @Test
  public void testBytesConverter() {
    assertEquals(BackupEnvelope.TYPE_BYTES,
        ConverterTypeDetector.detectSchemaType(
            BYTE_ARRAY_CONVERTER, Collections.emptyMap(), VALUE_CONVERTER));
  }

  @Test
  public void testNullConverterClass() {
    assertEquals(BackupEnvelope.TYPE_NONE,
        ConverterTypeDetector.detectSchemaType(
            null, Collections.emptyMap(), VALUE_CONVERTER));
  }

  @Test
  public void testUnknownConverter() {
    assertEquals(BackupEnvelope.TYPE_UNKNOWN,
        ConverterTypeDetector.detectSchemaType(
            "com.example.CustomConverter", Collections.emptyMap(), VALUE_CONVERTER));
  }

}
