/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.connect.storage.partitioner;

import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.PartitionException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class FieldPartitionerTest extends StorageSinkTestBase {

  private <T> FieldPartitioner<T> getFieldPartitioner(String... fields) {
    Map<String, Object> config = new HashMap<>();
    config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);
    config.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, Arrays.asList(fields));
    config.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, PartitionerConfig.PARTITIONER_CLASS_DEFAULT);

    FieldPartitioner<T> partitioner = new FieldPartitioner<>();
    partitioner.configure(config);
    return partitioner;
  }

  private <T> String getEncodedPatitionerPath(FieldPartitioner<T> partitioner) {
    SinkRecord sinkRecord = createSinkRecord(TIMESTAMP);
    return partitioner.encodePartition(sinkRecord);
  }

  private <T> String getEncodedPatitionerPath(FieldPartitioner<T> partitioner, Schema schema, Struct struct) {
    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, 
        schema, struct, 0L);
    return partitioner.encodePartition(sinkRecord);
  }

  @Test
  public void testBooleanPartition() {
    String fieldName = "boolean";
    FieldPartitioner<Boolean> partitioner = getFieldPartitioner(fieldName);
    
    // Test true value
    String path = getEncodedPatitionerPath(partitioner);
    Map<String, Object> m = new LinkedHashMap<>();
    m.put(fieldName, true);
    assertThat(path, is(generateEncodedPartitionFromMap(m)));
    
    // Test false value
    Schema schema = SchemaBuilder.struct().name("record")
        .field(fieldName, Schema.BOOLEAN_SCHEMA)
        .build();
    Struct struct = new Struct(schema).put(fieldName, false);
    path = getEncodedPatitionerPath(partitioner, schema, struct);
    
    m = new LinkedHashMap<>();
    m.put(fieldName, false);
    assertThat(path, is(generateEncodedPartitionFromMap(m)));
  }

  @Test
  public void testNumberPartition() {
    // Test INT32
    String fieldName = "int";
    FieldPartitioner<Integer> intPartitioner = getFieldPartitioner(fieldName);
    String path = getEncodedPatitionerPath(intPartitioner);
    Map<String, Object> m = new LinkedHashMap<>();
    m.put(fieldName, 12);
    assertThat(path, is(generateEncodedPartitionFromMap(m)));

    // Test INT64
    fieldName = "long";
    FieldPartitioner<Long> longPartitioner = getFieldPartitioner(fieldName);
    path = getEncodedPatitionerPath(longPartitioner);
    m = new LinkedHashMap<>();
    m.put(fieldName, 12L);
    assertThat(path, is(generateEncodedPartitionFromMap(m)));
    
    // Test INT8
    fieldName = "byte";
    FieldPartitioner<Byte> bytePartitioner = getFieldPartitioner(fieldName);
    Schema schema = SchemaBuilder.struct().name("record")
        .field(fieldName, Schema.INT8_SCHEMA)
        .build();
    Struct struct = new Struct(schema).put(fieldName, (byte) 42);
    path = getEncodedPatitionerPath(bytePartitioner, schema, struct);
    m = new LinkedHashMap<>();
    m.put(fieldName, (byte) 42);
    assertThat(path, is(generateEncodedPartitionFromMap(m)));
    
    // Test INT16
    fieldName = "short";
    FieldPartitioner<Short> shortPartitioner = getFieldPartitioner(fieldName);
    schema = SchemaBuilder.struct().name("record")
        .field(fieldName, Schema.INT16_SCHEMA)
        .build();
    struct = new Struct(schema).put(fieldName, (short) 1234);
    path = getEncodedPatitionerPath(shortPartitioner, schema, struct);
    m = new LinkedHashMap<>();
    m.put(fieldName, (short) 1234);
    assertThat(path, is(generateEncodedPartitionFromMap(m)));
  }

  @Test
  public void testUnsupportedTypePartition() throws PartitionException {
    // Test FLOAT32
    String fieldName = "float";
    FieldPartitioner<Float> floatPartitioner = getFieldPartitioner(fieldName);
    Exception e = assertThrows(PartitionException.class, () -> {
      getEncodedPatitionerPath(floatPartitioner);
    });
    assertEquals("Error encoding partition.", e.getMessage());
    
    // Test FLOAT64
    fieldName = "double";
    FieldPartitioner<Double> doublePartitioner = getFieldPartitioner(fieldName);
    e = assertThrows(PartitionException.class, () -> {
      getEncodedPatitionerPath(doublePartitioner);
    });
    assertEquals("Error encoding partition.", e.getMessage());
    
    // Test ARRAY
    fieldName = "array";
    FieldPartitioner<String> arrayPartitioner = getFieldPartitioner(fieldName);
    Schema schema = SchemaBuilder.struct().name("record")
        .field(fieldName, SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .build();
    Struct struct = new Struct(schema).put(fieldName, Arrays.asList("item1", "item2"));
    e = assertThrows(PartitionException.class, () -> {
      getEncodedPatitionerPath(arrayPartitioner, schema, struct);
    });
    assertEquals("Error encoding partition.", e.getMessage());
  }

  @Test
  public void testStringPartition() {
    String fieldName = "string";
    FieldPartitioner<String> partitioner = getFieldPartitioner(fieldName);
    
    // Test basic string
    String path = getEncodedPatitionerPath(partitioner);
    Map<String, Object> m = new LinkedHashMap<>();
    m.put(fieldName, "def");
    assertThat(path, is(generateEncodedPartitionFromMap(m)));
    
    // Test special characters
    Schema schema = SchemaBuilder.struct().name("record")
        .field(fieldName, Schema.STRING_SCHEMA)
        .build();
    String specialString = "test/with\\special:chars=and+spaces";
    Struct struct = new Struct(schema).put(fieldName, specialString);
    path = getEncodedPatitionerPath(partitioner, schema, struct);
    m = new LinkedHashMap<>();
    m.put(fieldName, specialString);
    assertThat(path, is(generateEncodedPartitionFromMap(m)));
    
    // Test empty string
    struct = new Struct(schema).put(fieldName, "");
    path = getEncodedPatitionerPath(partitioner, schema, struct);
    m = new LinkedHashMap<>();
    m.put(fieldName, "");
    assertThat(path, is(generateEncodedPartitionFromMap(m)));
  }

  @Test
  public void testNotStructPartition() throws PartitionException {
    String fieldName = "foo";
    FieldPartitioner<String> partitioner = getFieldPartitioner(fieldName);
    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null,
          Schema.STRING_SCHEMA, fieldName, 0L);

    // Schema is not a Struct
    Exception e = assertThrows(PartitionException.class, () -> {
      partitioner.encodePartition(sinkRecord);
    });
    assertEquals("Error encoding partition.", e.getMessage());
  }

  @Test
  public void testMultiPartition() {
    FieldPartitioner<String> partitioner = getFieldPartitioner("string", "int");
    String path = getEncodedPatitionerPath(partitioner);

    Map<String, Object> m = new LinkedHashMap<>();
    m.put("string", "def");
    m.put("int", 12);
    assertThat(path, is(generateEncodedPartitionFromMap(m)));

    partitioner = getFieldPartitioner("int", "string");
    path = getEncodedPatitionerPath(partitioner);

    m = new LinkedHashMap<>();
    m.put("int", 12);
    m.put("string", "def");
    assertThat(path, is(generateEncodedPartitionFromMap(m)));
  }

  @Test
  public void testNullPartition() {
    String fieldName = "string";
    FieldPartitioner<String> partitioner = getFieldPartitioner(fieldName);
    
    Schema schema = SchemaBuilder.struct().name("record")
        .field(fieldName, SchemaBuilder.string().optional().build())
        .build();
    
    Struct struct = new Struct(schema);
    struct.put(fieldName, null);
    
    String path = getEncodedPatitionerPath(partitioner, schema, struct);
    
    Map<String, Object> m = new LinkedHashMap<>();
    m.put(fieldName, null);
    assertThat(path, is("string=null"));
  }

  @Test
  public void testMultiPartitionWithNulls() {
    FieldPartitioner<String> partitioner = getFieldPartitioner("string", "int", "boolean");
    
    Schema schema = SchemaBuilder.struct().name("record")
        .field("string", SchemaBuilder.string().optional().build())
        .field("int", SchemaBuilder.int32().optional().build())
        .field("boolean", SchemaBuilder.bool().optional().build())
        .build();
    
    Struct struct = new Struct(schema);
    struct.put("string", "def");
    struct.put("int", null);
    struct.put("boolean", null);
    
    String path = getEncodedPatitionerPath(partitioner, schema, struct);
    
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("string", "def");
    m.put("int", null);
    m.put("boolean", null);
    assertThat(path, is(generateEncodedPartitionFromMap(m)));
  }

  @Test
  public void testCustomDelimiterPartition() {
    Map<String, Object> config = new HashMap<>();
    config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, "|");
    config.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, Arrays.asList("string", "int"));

    FieldPartitioner<String> partitioner = new FieldPartitioner<>();
    partitioner.configure(config);
    
    String path = getEncodedPatitionerPath(partitioner);
    
    // For custom delimiter, we need to manually construct the expected result
    assertThat(path, is("string=def|int=12"));
  }
}
