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

  @Test
  public void testBoolPartition() {
    String fieldName = "boolean";
    FieldPartitioner<Boolean> partitioner = getFieldPartitioner(fieldName);
    String path = getEncodedPatitionerPath(partitioner);

    Map<String, Object> m = new LinkedHashMap<>();
    m.put(fieldName, true);
    assertThat(path, is(generateEncodedPartitionFromMap(m)));
  }

  @Test
  public void testNumberPartition() {
    String fieldName = "int";
    FieldPartitioner<Integer> intPartitioner = getFieldPartitioner(fieldName);
    String path = getEncodedPatitionerPath(intPartitioner);

    Map<String, Object> m = new LinkedHashMap<>();
    m.put(fieldName, 12);
    assertThat(path, is(generateEncodedPartitionFromMap(m)));

    fieldName = "long";
    FieldPartitioner<Long> longPartitioner = getFieldPartitioner(fieldName);
    path = getEncodedPatitionerPath(longPartitioner);

    m = new LinkedHashMap<>();
    m.put(fieldName, 12L);
    assertThat(path, is(generateEncodedPartitionFromMap(m)));
  }

  @Test
  public void testFloatPartition() throws PartitionException {
    String fieldName = "float";
    FieldPartitioner<Float> partitioner = getFieldPartitioner(fieldName);
    Exception e = assertThrows(PartitionException.class, () -> {
      getEncodedPatitionerPath(partitioner);
    });
    assertEquals("Error encoding partition.", e.getMessage());
  }

  @Test
  public void testDoublePartition() throws PartitionException {
    String fieldName = "double";
    FieldPartitioner<Double> partitioner = getFieldPartitioner(fieldName);
    Exception e = assertThrows(PartitionException.class, () -> {
      getEncodedPatitionerPath(partitioner);
    });
    assertEquals("Error encoding partition.", e.getMessage());
  }

  @Test
  public void testStringPartition() {
    String fieldName = "string";
    FieldPartitioner<String> partitioner = getFieldPartitioner(fieldName);
    String path = getEncodedPatitionerPath(partitioner);

    Map<String, Object> m = new LinkedHashMap<>();
    m.put(fieldName, "def");
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
  public void testMapPartition() throws PartitionException {
    String fieldName = "nested.string";

    Map<String, Object> expectedNestedMap = createMapWithTimestampField(TIMESTAMP);
    Map<String, Object> map = new HashMap<>();
    map.put("nested", expectedNestedMap);
    FieldPartitioner<String> partitioner;
    SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null,
            Schema.STRING_SCHEMA, map, 0L);
    String path;


    Map<String, Object> m = new LinkedHashMap<>();
    m.put("string", "def");
    partitioner = getFieldPartitioner(fieldName);
    path = partitioner.encodePartition(sinkRecord);
    assertThat(path, is(generateEncodedPartitionFromMap(m)));

    fieldName = "nested.int";
    m = new LinkedHashMap<>();
    m.put("int", "12");
    partitioner = getFieldPartitioner(fieldName);
    path = partitioner.encodePartition(sinkRecord);
    assertThat(path, is(generateEncodedPartitionFromMap(m)));

    fieldName = "nested.long";
    String secondFieldName = "nested.int";
    m = new LinkedHashMap<>();
    m.put("long", "12");
    m.put("int", "12");
    partitioner = getFieldPartitioner(fieldName, secondFieldName);
    path = partitioner.encodePartition(sinkRecord);
    assertThat(path, is(generateEncodedPartitionFromMap(m)));
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

}
