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

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
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


  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, Object> config) {
    fieldNames = (List<String>) config.get(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
    delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    Object value = sinkRecord.value();
    if (value instanceof Struct) {
      final Schema valueSchema = sinkRecord.valueSchema();
      final Struct struct = (Struct) value;

      StringBuilder builder = new StringBuilder();
      for (String fieldName : fieldNames) {
        if (builder.length() > 0) {
          builder.append(this.delim);
        }

        Object partitionKey = struct.get(fieldName);

        if (partitionKey == null) {
          builder.append(fieldName + "=null");
          continue;
        }

        String encodedValue = encodePartitionValue(fieldName, partitionKey, valueSchema);
        builder.append(encodedValue);
      }
      return builder.toString();
    } else {
      log.error("Value is not Struct type.");
      throw new PartitionException("Error encoding partition.");
    }
  }

  private String encodePartitionValue(String fieldName, Object partitionKey, Schema valueSchema) {
    Type type = valueSchema.field(fieldName).schema().type();
    switch (type) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
        Number record = (Number) partitionKey;
        return fieldName + "=" + record.toString();
      case STRING:
        return fieldName + "=" + (String) partitionKey;
      case BOOLEAN:
        boolean booleanRecord = (boolean) partitionKey;
        return fieldName + "=" + Boolean.toString(booleanRecord);
      default:
        log.error("Type {} is not supported as a partition key.", type.getName());
        throw new PartitionException("Error encoding partition.");
    }
  }

  @Override
  public List<T> partitionFields() {
    if (partitionFields == null) {
      partitionFields = newSchemaGenerator(config).newPartitionFields(
          Utils.join(fieldNames, ",")
      );
    }
    return partitionFields;
  }
}
