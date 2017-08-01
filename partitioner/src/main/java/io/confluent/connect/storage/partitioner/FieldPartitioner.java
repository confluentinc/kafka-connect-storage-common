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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;

import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.PartitionException;

public class FieldPartitioner<T> extends DefaultPartitioner<T> {
  private static final Logger log = LoggerFactory.getLogger(FieldPartitioner.class);
  private List<String> fieldNames;

  @Override
  public void configure(Map<String, Object> config) {
    fieldNames = (List<String>) config.get(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
    delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);

    StringBuilder format = new StringBuilder();
    for (String field : fieldNames) {
      format.append(field + delim);
    }

    if (format.length() > 0) {
      format.delete(format.length() - delim.length(), format.length());
    }

    partitionFields = newSchemaGenerator(config).newPartitionFields(format.toString());
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    Object value = sinkRecord.value();
    Schema valueSchema = sinkRecord.valueSchema();
    StringBuilder partition = new StringBuilder();

    if (value instanceof Struct) {
      Struct struct = (Struct) value;
      for (String fieldName : fieldNames) {
        Object partitionKey = struct.get(fieldName);
        Schema.Type type = valueSchema.field(fieldName).schema().type();
        switch (type) {
          case INT8:
          case INT16:
          case INT32:
          case INT64:
            Number record = (Number) partitionKey;
            partition.append(fieldName + "=" + record.toString() + delim);
            break;
          case STRING:
            partition.append(fieldName + "=" + (String) partitionKey + delim);
            break;
          case BOOLEAN:
            boolean booleanRecord = (boolean) partitionKey;
            partition.append(fieldName + "=" + Boolean.toString(booleanRecord) + delim);
            break;
          default:
            log.error("Type {} is not supported as a partition key.", type.getName());
            throw new PartitionException("Error encoding partition.");
        }
      }

      if (partition.length() > 0) {
        partition.delete(partition.length() - delim.length(), partition.length());
      }
      return partition.toString();
    } else {
      log.error("Value is not Struct type.");
      throw new PartitionException("Error encoding partition.");
    }
  }
}