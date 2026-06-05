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

  @Test
  public void testAvroConverter() {
    assertEquals(BackupEnvelope.TYPE_AVRO,
        ConverterTypeDetector.detectSchemaType(
            "io.confluent.connect.avro.AvroConverter",
            Collections.emptyMap(), "value.converter."));
  }

  @Test
  public void testProtobufConverter() {
    assertEquals(BackupEnvelope.TYPE_PROTOBUF,
        ConverterTypeDetector.detectSchemaType(
            "io.confluent.connect.protobuf.ProtobufConverter",
            Collections.emptyMap(), "value.converter."));
  }

  @Test
  public void testJsonSchemaConverter() {
    assertEquals(BackupEnvelope.TYPE_JSON_SCHEMA,
        ConverterTypeDetector.detectSchemaType(
            "io.confluent.connect.json.JsonSchemaConverter",
            Collections.emptyMap(), "value.converter."));
  }

  @Test
  public void testStringConverter() {
    assertEquals(BackupEnvelope.TYPE_STRING,
        ConverterTypeDetector.detectSchemaType(
            "org.apache.kafka.connect.storage.StringConverter",
            Collections.emptyMap(), "value.converter."));
  }

  @Test
  public void testJsonConverterSchemaless() {
    Map<String, String> config = new HashMap<>();
    config.put("value.converter.schemas.enable", "false");
    assertEquals(BackupEnvelope.TYPE_JSON_SCHEMALESS,
        ConverterTypeDetector.detectSchemaType(
            "org.apache.kafka.connect.json.JsonConverter",
            config, "value.converter."));
  }

  @Test
  public void testJsonConverterWithSchema() {
    Map<String, String> config = new HashMap<>();
    config.put("value.converter.schemas.enable", "true");
    assertEquals(BackupEnvelope.TYPE_JSON_EMBEDDED_SCHEMA,
        ConverterTypeDetector.detectSchemaType(
            "org.apache.kafka.connect.json.JsonConverter",
            config, "value.converter."));
  }

  @Test
  public void testIntegerConverter() {
    assertEquals(BackupEnvelope.TYPE_INT32,
        ConverterTypeDetector.detectSchemaType(
            "org.apache.kafka.connect.converters.IntegerConverter",
            Collections.emptyMap(), "key.converter."));
  }

  @Test
  public void testLongConverter() {
    assertEquals(BackupEnvelope.TYPE_INT64,
        ConverterTypeDetector.detectSchemaType(
            "org.apache.kafka.connect.converters.LongConverter",
            Collections.emptyMap(), "key.converter."));
  }

  @Test
  public void testBytesConverter() {
    assertEquals(BackupEnvelope.TYPE_BYTES,
        ConverterTypeDetector.detectSchemaType(
            "org.apache.kafka.connect.converters.ByteArrayConverter",
            Collections.emptyMap(), "value.converter."));
  }

  @Test
  public void testNullConverterClass() {
    assertEquals(BackupEnvelope.TYPE_NONE,
        ConverterTypeDetector.detectSchemaType(
            null, Collections.emptyMap(), "value.converter."));
  }

  @Test
  public void testUnknownConverter() {
    assertEquals(BackupEnvelope.TYPE_UNKNOWN,
        ConverterTypeDetector.detectSchemaType(
            "com.example.CustomConverter",
            Collections.emptyMap(), "value.converter."));
  }

  @Test
  public void testRegisterCustomConverter() {
    ConverterTypeDetector.register("com.example.MyConverter", "MY_TYPE");
    assertEquals("MY_TYPE",
        ConverterTypeDetector.detectSchemaType(
            "com.example.MyConverter",
            Collections.emptyMap(), "value.converter."));
  }
}
