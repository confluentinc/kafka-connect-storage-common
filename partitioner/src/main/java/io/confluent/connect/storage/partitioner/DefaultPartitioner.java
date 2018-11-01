/*
 * Copyright 2017 Confluent Inc.
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

import java.util.List;
import java.util.Map;

import io.confluent.connect.storage.common.SchemaGenerator;
import io.confluent.connect.storage.common.StorageCommonConfig;

/**
 *
 * @param <T> The type representing the field schemas.
 */
public class DefaultPartitioner<T> implements Partitioner<T> {
  private static final String PARTITION_FIELD = "partition";

  private static final String SCHEMA_GENERATOR_CLASS =
      "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator";

  protected Map<String, Object> config;
  protected List<T> partitionFields = null;
  protected String delim;

  @Override
  public void configure(Map<String, Object> config) {
    this.config = config;
    delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    return PARTITION_FIELD + "=" + String.valueOf(sinkRecord.kafkaPartition());
  }

  @Override
  public String generatePartitionedPath(String topic, String encodedPartition) {
    return topic + delim + encodedPartition;
  }

  @Override
  public List<T> partitionFields() {
    if (partitionFields == null) {
      partitionFields = newSchemaGenerator(config).newPartitionFields(PARTITION_FIELD);
    }
    return partitionFields;
  }

  @SuppressWarnings("unchecked")
  public SchemaGenerator<T> newSchemaGenerator(Map<String, Object> config) {
    Class<? extends SchemaGenerator<T>> generatorClass = null;
    try {
      generatorClass = getSchemaGeneratorClass();
      return generatorClass.newInstance();
    } catch (
        ClassNotFoundException
            | ClassCastException
            | IllegalAccessException
            | InstantiationException e) {
      ConfigException ce = new ConfigException("Invalid generator class: " + generatorClass);
      ce.initCause(e);
      throw ce;
    }
  }

  @SuppressWarnings("unchecked")
  protected Class<? extends SchemaGenerator<T>> getSchemaGeneratorClass()
      throws ClassNotFoundException {
    return (Class<? extends SchemaGenerator<T>>) Class.forName(SCHEMA_GENERATOR_CLASS);
  }

}
