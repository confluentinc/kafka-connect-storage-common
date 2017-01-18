/*
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.storage.partitioner;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.confluent.connect.storage.common.SchemaGenerator;
import io.confluent.connect.storage.common.StorageCommonConfig;

/**
 *
 * @param <T> The type representing the field schemas.
 */
public class DefaultPartitioner<T> implements Partitioner<T> {

  // CHECKSTYLE:OFF
  private static final String partitionField = "partition";
  private List<T> partitionFields =  new ArrayList<>();
  // CHECKSTYLE:ON
  private String delim;

  @Override
  public void configure(Map<String, Object> config) {
    partitionFields = newSchemaGenerator(config).newPartitionFields(partitionField);
    delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    return partitionField + "=" + String.valueOf(sinkRecord.kafkaPartition());
  }

  @Override
  public String generatePartitionedPath(String topic, String encodedPartition) {
    return topic + delim + encodedPartition;
  }

  @Override
  public List<T> partitionFields() {
    return partitionFields;
  }

  public SchemaGenerator<T> newSchemaGenerator(Map<String, Object> config) {
    String generatorName = (String) config.get(PartitionerConfig.SCHEMA_GENERATOR_CLASS_CONFIG);
    try {
      @SuppressWarnings("unchecked")
      Class<? extends SchemaGenerator<T>> generatorClass =
          (Class<? extends SchemaGenerator<T>>) Class.forName(generatorName);
      return generatorClass.newInstance();
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new ConfigException("Schema generator class not found: " + generatorName);
    }
  }
}
