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

import io.confluent.connect.storage.util.DataUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
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
    Map<String, String> partitionValueMap = new LinkedHashMap<>();
    String partitionValue;

    for (String fieldName : fieldNames) {
      log.debug("Extracting partition field '{}'.", fieldName);
      if (value instanceof Struct) {
        final Schema valueSchema = sinkRecord.valueSchema();
        log.trace("Extracting partition field '{}' from struct '{}'.", fieldName, valueSchema);
        partitionValue = getStructField(valueSchema, (Struct) value, fieldName);
      } else if (value instanceof Map) {
        Map<?, ?> map = (Map<?, ?>) value;
        log.trace("Extracting partition field '{}' from map '{}'.", fieldName, map);
        partitionValue = getMapField(map, fieldName);
      } else {
        log.error("Value is not of Struct or Map type.");
        throw new PartitionException("Error encoding partition.");
      }
      partitionValueMap.put(fieldName, partitionValue);
    }
    return Utils.mkString(partitionValueMap, "", "", "=", delim);
  }

  private String getStructField(Schema valueSchema, Struct struct, String fieldName) {
    Object partitionKey = DataUtils.getNestedFieldValue(struct, fieldName);
    Schema fieldSchema = DataUtils.getNestedField(valueSchema, fieldName).schema();
    Type type = fieldSchema.type();
    switch (type) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
        Number record = (Number) partitionKey;
        return String.valueOf(record);
      case STRING:
        return (String) partitionKey;
      case BOOLEAN:
        boolean booleanRecord = (boolean) partitionKey;
        return Boolean.toString(booleanRecord);
      default:
        log.error("Type {} is not supported as a partition key.", type.getName());
        throw new PartitionException("Error encoding partition.");
    }
  }

  private String getMapField(Map<?, ?> map, String fieldName) {
    Object partitionKey = DataUtils.getNestedFieldValue(map, fieldName);
    if (partitionKey instanceof Number) {
      Number record = (Number) partitionKey;
      return String.valueOf(record);
    } else if (partitionKey == null || partitionKey instanceof String) {
      return (String) partitionKey;
    } else if (partitionKey instanceof Boolean) {
      boolean booleanRecord = (boolean) partitionKey;
      return Boolean.toString(booleanRecord);
    } else {
      log.error("Type {} is not supported as a partition key.", partitionKey.getClass());
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
