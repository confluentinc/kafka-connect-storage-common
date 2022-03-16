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
// import org.apache.kafka.common.config.ConfigException;
// import org.apache.kafka.common.utils.Time;
// import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
// import org.apache.kafka.connect.data.Timestamp;
// import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
// import org.joda.time.DateTime;
// import org.joda.time.DateTimeZone;
// import org.joda.time.format.DateTimeFormat;
// import org.joda.time.format.DateTimeFormatter;
// import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import java.util.Date;
import java.util.List;
// import java.util.Locale;
import java.util.Map;
// import java.util.regex.Pattern;

// import io.confluent.connect.storage.common.SchemaGenerator;
// import io.confluent.connect.storage.common.StorageCommonConfig;
// import io.confluent.connect.storage.common.util.StringUtils;
import io.confluent.connect.storage.errors.PartitionException;
// import io.confluent.connect.storage.util.DataUtils;

public class FieldAndTimeBasedPartitioner<T> extends TimeBasedPartitioner<T> {
  private static final Logger log = LoggerFactory.getLogger(FieldAndTimeBasedPartitioner.class);

  private List<String> fieldNames;

  @Override
  public void configure(Map<String, Object> config) {
    fieldNames = (List<String>) config.get(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
    super.configure(config);
  }

  // This is pulled from FieldPartitioner, which is private; alternate is to build a 
  private String encodeFieldPartition(SinkRecord sinkRecord) {
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
      return builder.toString();
    } else {
      log.error("Value is not Struct type.");
      throw new PartitionException("Error encoding partition.");
    }
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord, long nowInMillis) {
    String timeStampPartition = super.encodePartition(sinkRecord, nowInMillis);
    String fieldPartition = encodeFieldPartition(sinkRecord);
    return fieldPartition + this.delim + timeStampPartition;
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    String timeStampPartition = super.encodePartition(sinkRecord);
    String fieldPartition = encodeFieldPartition(sinkRecord);
    return fieldPartition + this.delim + timeStampPartition;
  }


  @Override
  public List<T> partitionFields() {
    if (partitionFields == null) {
      String fieldString = Utils.join(fieldNames, ",");
      partitionFields = newSchemaGenerator(config).newPartitionFields(
        fieldString + super.getPathFormat()
      );
    }
    return partitionFields;
  }
}