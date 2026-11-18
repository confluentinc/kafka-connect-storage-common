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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import io.confluent.connect.storage.common.GenericRecommender;
import io.confluent.connect.storage.common.StorageCommonConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

public class SchemaPartitionerTest {

  private static final String TOPIC = "test-topic";
  private static final String DELIM = "/";

  private static SchemaPartitioner<String> partitioner(PartitionerConfig.AffixType affixType) {
    Map<String, Object> config = new HashMap<>();
    config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, DELIM);
    config.put(PartitionerConfig.SCHEMA_PARTITION_AFFIX_TYPE_CONFIG, affixType.name());
    DefaultPartitioner<String> delegate = new DefaultPartitioner<>();
    SchemaPartitioner<String> partitioner = new SchemaPartitioner<>(delegate);
    partitioner.configure(config);
    return partitioner;
  }

  private static SinkRecord record(Schema schema) {
    return new SinkRecord(TOPIC, 12, null, null, schema, null, 0L);
  }

  private static final Schema NAMED_SCHEMA = SchemaBuilder.struct().name("com.example.User").build();

  @Test
  public void testPrefix() {
    assertEquals(
        "schema_name=com.example.User/partition=12",
        partitioner(PartitionerConfig.AffixType.PREFIX).encodePartition(record(NAMED_SCHEMA)));
  }

  @Test
  public void testSuffix() {
    assertEquals(
        "partition=12/schema_name=com.example.User",
        partitioner(PartitionerConfig.AffixType.SUFFIX).encodePartition(record(NAMED_SCHEMA)));
  }

  @Test
  public void testNoneLeavesPartitionUnchanged() {
    assertEquals(
        "partition=12",
        partitioner(PartitionerConfig.AffixType.NONE).encodePartition(record(NAMED_SCHEMA)));
  }

  @Test
  public void testEncodePartitionWithTimestampOverload() {
    assertEquals(
        "partition=12/schema_name=com.example.User",
        partitioner(PartitionerConfig.AffixType.SUFFIX)
            .encodePartition(record(NAMED_SCHEMA), 1000L));
  }

  @Test
  public void testNullSchema() {
    assertEquals(
        "partition=12/schema_name=null",
        partitioner(PartitionerConfig.AffixType.SUFFIX).encodePartition(record(null)));
  }

  @Test
  public void testDelegation() {
    DefaultPartitioner<String> delegate = new DefaultPartitioner<>();
    SchemaPartitioner<String> partitioner = new SchemaPartitioner<>(delegate);
    assertSame(delegate, partitioner.getDelegatePartitioner());
    partitioner.configure(new HashMap<String, Object>() {{
      put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, DELIM);
      put(PartitionerConfig.SCHEMA_PARTITION_AFFIX_TYPE_CONFIG, "PREFIX");
    }});
    assertEquals(
        TOPIC + DELIM + "x",
        partitioner.generatePartitionedPath(TOPIC, "x"));
  }

  @Test
  public void testConfigDefaultsAndRecommender() {
    ConfigDef configDef = PartitionerConfig.newConfigDef(new GenericRecommender());
    ConfigDef.ConfigKey key = configDef.configKeys()
        .get(PartitionerConfig.SCHEMA_PARTITION_AFFIX_TYPE_CONFIG);
    assertEquals("NONE", key.defaultValue);
    List<Object> values = key.recommender.validValues(key.name, new HashMap<String, Object>());
    assertEquals(3, values.size());
    assertEquals(true, values.contains("PREFIX"));
    assertEquals(true, values.contains("SUFFIX"));
    assertEquals(true, values.contains("NONE"));
  }
}
