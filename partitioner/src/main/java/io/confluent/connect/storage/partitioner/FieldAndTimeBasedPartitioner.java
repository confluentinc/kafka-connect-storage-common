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

import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.util.DataUtils;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class FieldAndTimeBasedPartitioner<T> extends TimeBasedPartitioner<T> {
  private static final Logger log = LoggerFactory.getLogger(FieldAndTimeBasedPartitioner.class);
  private List<String> fieldNames;


  @Override
  public void configure(Map<String, Object> config) {
    super.configure(config);
    fieldNames = (List<String>) config.get(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
    delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    String timePartition = super.encodePartition(sinkRecord);

    Object value = sinkRecord.value();
    log.info(value.getClass().toString());

    if (value instanceof Struct) {
      final Schema valueSchema = sinkRecord.valueSchema();
      final Struct struct = (Struct) value;

      StringBuilder builder = new StringBuilder();
      for (String fieldName : fieldNames) {
        if (builder.length() > 0) {
          builder.append(this.delim);
        }

        Object partitionKey = struct.get(fieldName);
        Type type = valueSchema.field(fieldName).schema().type();
        switch (type) {
          case INT8:
          case INT16:
          case INT32:
          case INT64:
            Number record = (Number) partitionKey;
            builder.append(fieldName + "=" + record.toString());
            break;
          case STRING:
            builder.append(fieldName + "=" + (String) partitionKey);
            break;
          case BOOLEAN:
            boolean booleanRecord = (boolean) partitionKey;
            builder.append(fieldName + "=" + Boolean.toString(booleanRecord));
            break;
          default:
            log.error("Type {} is not supported as a partition key.", type.getName());
            throw new PartitionException("Error encoding partition.");
        }
      }
      return builder.append(delim).append(timePartition).toString();

    } else if (value instanceof Map) {
      StringBuilder builder = new StringBuilder();
      for (String fieldName : fieldNames) {
        if (builder.length() > 0) {
          builder.append(this.delim);
        }
        Map<?, ?> map = (Map<?, ?>) value;

        Object fieldValue = DataUtils.getNestedFieldValue(map, fieldName);

        if (fieldValue instanceof Field.Int8
                || fieldValue instanceof Field.Int16
                || fieldValue instanceof Field.Int32
                || fieldValue instanceof Field.Int64) {
          Number record = (Number) fieldValue;
          builder.append(record.toString());
        } else if (fieldValue instanceof String) {
          builder.append((String) fieldValue);
        } else if (fieldValue instanceof Date) {
          boolean booleanRecord = (boolean) fieldValue;
          builder.append(Boolean.toString(booleanRecord));
        } else {
          log.error(
                  "Unsupported type '{}' for user-defined timestamp field.",
                  fieldValue.getClass()
          );
          throw new PartitionException(
                  "Error extracting timestamp from record field: " + fieldName
          );
        }
      }
      return builder.append(delim).append(timePartition).toString();
    }
    log.error("Value is not Struct type.");
    throw new PartitionException("Error encoding partition.");
  }
}
