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

import io.confluent.connect.storage.common.SchemaGenerator;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.Map;

import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.PartitionException;

public class TimeFieldPartitioner<T> extends DefaultPartitioner<T> {
  private static final Logger log = LoggerFactory.getLogger(TimeFieldPartitioner.class);

  // Duration of a partition in milliseconds.
  private long partitionDurationMs;
  private DateTimeFormatter formatter;

  private String fieldName;

  @Override
  public void configure(Map<String, Object> config) {
    String pathFormat = PartitioningCommon.getPathFormat(config);

    this.partitionDurationMs = (long) config.get(PartitionerConfig.PARTITION_DURATION_MS_CONFIG);
    this.delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    this.fieldName = (String) config.get(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
    this.formatter = PartitioningCommon.loadDateTimeFormatterFromConfiguration(config, pathFormat);

    SchemaGenerator<T> schemaGenerator = newSchemaGenerator(config);
    try {
      this.partitionFields = schemaGenerator.newPartitionFields(pathFormat);
    } catch (IllegalArgumentException e) {
      throw new ConfigException(PartitionerConfig.PATH_FORMAT_CONFIG, pathFormat, e.getMessage());
    }
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    Object value = sinkRecord.value();

    if (!(value instanceof Struct)) {
      log.error("Value is not Struct type.");
      throw new PartitionException("Error encoding partition.");
    }

    Struct struct = (Struct) value;
    Object partitionKey = struct.get(fieldName);

    if (!(partitionKey instanceof Date)) {
      log.error("Type {} is not supported as a partition key.", partitionKey.getClass().getName());
      throw new PartitionException("Error encoding partition.");
    }

    Date partitionDateKey = (Date) partitionKey;
    Date roundedDateKey = PartitioningCommon.roundInstantToMs(partitionDurationMs,
            partitionDateKey, formatter.getZone());
    DateTime roundedDateKeyJoda = new DateTime(roundedDateKey.getTime());
    return roundedDateKeyJoda.toString(formatter);
  }

  @Override
  @SuppressWarnings("unchecked")
  public SchemaGenerator<T> newSchemaGenerator(Map<String, Object> config) {
    Class<? extends SchemaGenerator<T>> generatorClass = null;
    try {
      generatorClass =
              (Class<? extends SchemaGenerator<T>>) config.get(PartitionerConfig.SCHEMA_GENERATOR_CLASS_CONFIG);
      return generatorClass.getConstructor(Map.class).newInstance(config);
    } catch (ClassCastException | IllegalAccessException | InstantiationException | InvocationTargetException
            | NoSuchMethodException e) {
      throw new ConfigException("Invalid generator class: " + generatorClass);
    }
  }
}
