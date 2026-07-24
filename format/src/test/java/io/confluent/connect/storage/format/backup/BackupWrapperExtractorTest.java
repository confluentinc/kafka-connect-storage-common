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
import io.confluent.connect.storage.format.backup.BackupWrapperExtractor.Unwrapped;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class BackupWrapperExtractorTest {

  @Test
  public void testUnwrapSrWrapper() {
    Schema dataSchema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA).build();
    Schema wrapperSchema = BackupWrapper.buildSchema(dataSchema);

    Struct data = new Struct(dataSchema).put("name", "Alice");
    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        42, 1, "AVRO", "test-value", "{\"type\":\"record\"}",
        "{\"tree\":{}}", "[{\"ref\":1}]");
    Struct wrapper = BackupWrapper.buildWrapper(wrapperSchema, data, fields);

    Unwrapped result = BackupWrapperExtractor.unwrap(
        wrapper, wrapperSchema, false, "AVRO");

    assertNotNull(result.getData());
    assertEquals(42, (int) result.getSchemaId());
    assertEquals(1, (int) result.getSchemaVersion());
    assertEquals("AVRO", result.getSchemaType());
    assertEquals("test-value", result.getSubject());
    assertEquals("{\"type\":\"record\"}", result.getRawSchema());
    assertEquals("{\"tree\":{}}", result.getReferenceTreeJson());
    assertEquals("[{\"ref\":1}]", result.getDirectRefsJson());
  }

  @Test
  public void testUnwrapTombstone() {
    Unwrapped result = BackupWrapperExtractor.unwrap(
        null, null, false, "AVRO");

    assertNull(result.getData());
    assertNull(result.getSchema());
    assertNull(result.getSchemaId());
    assertEquals("NONE", result.getSchemaType());
  }

  @Test
  public void testUnwrapSchemalessMap() {
    HashMap<String, Object> map = new HashMap<>();
    map.put("key", "value");
    map.put("num", 42);

    Unwrapped result = BackupWrapperExtractor.unwrap(
        map, null, false, "JSON_SCHEMALESS");

    assertNotNull(result.getData());
    String json = (String) result.getData();
    assertTrue(json.contains("\"key\""));
    assertTrue(json.contains("\"value\""));
    assertEquals("JSON_SCHEMALESS", result.getSchemaType());
    assertNull(result.getSchemaId());
  }

  @Test
  public void testUnwrapSchemalessString() {
    Unwrapped result = BackupWrapperExtractor.unwrap(
        "plain text", null, false, "STRING");

    assertEquals("plain text", result.getData());
    assertEquals("JSON_SCHEMALESS", result.getSchemaType());
  }

  @Test
  public void testUnwrapNonSrWithSchema() {
    Unwrapped result = BackupWrapperExtractor.unwrap(
        "hello", Schema.STRING_SCHEMA, true, "STRING");

    assertEquals("hello", result.getData());
    assertEquals(Schema.STRING_SCHEMA, result.getSchema());
    assertEquals("STRING", result.getSchemaType());
    assertNull(result.getSchemaId());
    assertNull(result.getRawSchema());
  }

  @Test
  public void testUnwrapWrapperWithNullOptionalFields() {
    Schema dataSchema = Schema.STRING_SCHEMA;
    Schema wrapperSchema = BackupWrapper.buildSchema(dataSchema);

    BackupWrapper.WrapperFields fields = new BackupWrapper.WrapperFields(
        1, null, "STRING", "test-key", null, null, null);
    Struct wrapper = BackupWrapper.buildWrapper(wrapperSchema, "hello", fields);

    Unwrapped result = BackupWrapperExtractor.unwrap(
        wrapper, wrapperSchema, true, "STRING");

    assertEquals(1, (int) result.getSchemaId());
    assertNull(result.getSchemaVersion());
    assertNull(result.getRawSchema());
    assertNull(result.getReferenceTreeJson());
    assertNull(result.getDirectRefsJson());
  }

  @Test
  public void testUnwrapSchemalessNestedMap() {
    Map<String, Object> inner = new LinkedHashMap<>();
    inner.put("city", "NYC");
    inner.put("zip", 10001);
    Map<String, Object> outer = new LinkedHashMap<>();
    outer.put("name", "Alice");
    outer.put("address", inner);

    Unwrapped result = BackupWrapperExtractor.unwrap(
        outer, null, false, "JSON_SCHEMALESS");

    String json = (String) result.getData();
    assertTrue(json.contains("\"address\""));
    assertTrue(json.contains("\"city\":\"NYC\""));
    assertTrue(json.contains("\"zip\":10001"));
    assertEquals("JSON_SCHEMALESS", result.getSchemaType());
  }

  @Test
  public void testUnwrapSchemalessArray() {
    List<Object> list = Arrays.asList(1, "two", true, null);

    Unwrapped result = BackupWrapperExtractor.unwrap(
        list, null, false, "JSON_SCHEMALESS");

    String json = (String) result.getData();
    assertTrue(json.startsWith("["));
    assertTrue(json.contains("\"two\""));
    assertTrue(json.contains("true"));
    assertTrue(json.contains("null"));
  }

  @Test
  public void testUnwrapSchemalessEmptyMap() {
    Unwrapped result = BackupWrapperExtractor.unwrap(
        new HashMap<>(), null, false, "JSON_SCHEMALESS");

    assertEquals("{}", result.getData());
  }

  @Test
  public void testUnwrapSchemalessEmptyList() {
    Unwrapped result = BackupWrapperExtractor.unwrap(
        new ArrayList<>(), null, false, "JSON_SCHEMALESS");

    assertEquals("[]", result.getData());
  }

  @Test
  public void testUnwrapSchemalessNullValues() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("present", "val");
    map.put("absent", null);

    Unwrapped result = BackupWrapperExtractor.unwrap(
        map, null, false, "JSON_SCHEMALESS");

    String json = (String) result.getData();
    assertTrue(json.contains("\"absent\":null"));
    assertTrue(json.contains("\"present\":\"val\""));
  }

  @Test
  public void testUnwrapEmbeddedSchemaStruct() {
    Schema s = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .build();
    Struct data = new Struct(s)
        .put("name", "Alice").put("age", 30);

    Unwrapped result = BackupWrapperExtractor.unwrap(
        data, s, false, "JSON_EMBEDDED_SCHEMA");

    assertEquals(data, result.getData());
    assertEquals(s, result.getSchema());
    assertEquals("JSON_EMBEDDED_SCHEMA", result.getSchemaType());
    assertNull(result.getSchemaId());
  }

  @Test
  public void testUnwrapEmbeddedSchemaNestedStruct() {
    Schema inner = SchemaBuilder.struct()
        .field("city", Schema.STRING_SCHEMA).build();
    Schema outer = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("address", inner).build();
    Struct addr = new Struct(inner).put("city", "NYC");
    Struct data = new Struct(outer)
        .put("name", "Alice").put("address", addr);

    Unwrapped result = BackupWrapperExtractor.unwrap(
        data, outer, false, "JSON_EMBEDDED_SCHEMA");

    Struct restored = (Struct) result.getData();
    Struct restoredAddr = restored.getStruct("address");
    assertEquals("NYC", restoredAddr.getString("city"));
  }

  @Test
  public void testUnwrapEmbeddedSchemaWithArray() {
    Schema itemSchema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA).build();
    Schema s = SchemaBuilder.struct()
        .field("items", SchemaBuilder.array(itemSchema)).build();
    Struct item1 = new Struct(itemSchema).put("id", 1);
    Struct item2 = new Struct(itemSchema).put("id", 2);
    Struct data = new Struct(s)
        .put("items", Arrays.asList(item1, item2));

    Unwrapped result = BackupWrapperExtractor.unwrap(
        data, s, false, "JSON_EMBEDDED_SCHEMA");

    Struct restored = (Struct) result.getData();
    List<?> items = restored.getArray("items");
    assertEquals(2, items.size());
  }

  @Test
  public void testUnwrapPrimitiveString() {
    Unwrapped result = BackupWrapperExtractor.unwrap(
        "hello", Schema.STRING_SCHEMA, true, "STRING");

    assertEquals("hello", result.getData());
    assertEquals(Schema.STRING_SCHEMA, result.getSchema());
    assertEquals("STRING", result.getSchemaType());
  }

  @Test
  public void testUnwrapPrimitiveInt32() {
    Unwrapped result = BackupWrapperExtractor.unwrap(
        42, Schema.INT32_SCHEMA, true, "INT32");

    assertEquals(42, result.getData());
    assertEquals(Schema.INT32_SCHEMA, result.getSchema());
    assertEquals("INT32", result.getSchemaType());
  }

  @Test
  public void testUnwrapPrimitiveBytes() {
    byte[] bytes = new byte[]{1, 2, 3};

    Unwrapped result = BackupWrapperExtractor.unwrap(
        bytes, Schema.BYTES_SCHEMA, false, "BYTES");

    assertEquals(bytes, result.getData());
    assertEquals(Schema.BYTES_SCHEMA, result.getSchema());
    assertEquals("BYTES", result.getSchemaType());
  }

  private static void assertTrue(boolean condition) {
    org.junit.Assert.assertTrue(condition);
  }
}
