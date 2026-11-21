/*
 * Copyright 2026 Confluent Inc.
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

import io.confluent.connect.storage.common.StorageCommonConfig;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A partitioner that wraps another partitioner and adds the record value schema name, as a
 * {@code schema_name=<name>} path segment, either before (PREFIX) or after (SUFFIX) the
 * delegate's encoded partition.
 *
 * @param <T> the type of partition fields
 */
public class SchemaPartitioner<T> extends DelegatingPartitioner<T> {

  private static final String SCHEMA_NAME_PREFIX = "schema_name=";

  private PartitionerConfig.AffixType schemaAffixType;
  private String delim;

  public SchemaPartitioner(Partitioner<T> delegatePartitioner) {
    super(delegatePartitioner);
  }

  @Override
  public void configure(Map<String, Object> config) {
    Object delimValue = config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    if (delimValue == null) {
      throw new ConfigException(
          StorageCommonConfig.DIRECTORY_DELIM_CONFIG, null, "Directory delimiter must be set.");
    }
    this.delim = delimValue.toString();

    Object affix = config.get(PartitionerConfig.SCHEMA_PARTITION_AFFIX_TYPE_CONFIG);
    if (affix == null) {
      this.schemaAffixType = PartitionerConfig.AffixType.NONE;
    } else {
      try {
        this.schemaAffixType = PartitionerConfig.AffixType.valueOf(affix.toString());
      } catch (IllegalArgumentException e) {
        throw new ConfigException(
            PartitionerConfig.SCHEMA_PARTITION_AFFIX_TYPE_CONFIG,
            affix,
            "Valid values are: " + String.join(", ", PartitionerConfig.AffixType.names()));
      }
    }
    delegatePartitioner.configure(config);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    String encodedPartition = delegatePartitioner.encodePartition(sinkRecord);
    return generateSchemaBasedPath(encodedPartition, schemaName(sinkRecord));
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord, long nowInMillis) {
    String encodedPartition = delegatePartitioner.encodePartition(sinkRecord, nowInMillis);
    return generateSchemaBasedPath(encodedPartition, schemaName(sinkRecord));
  }

  private static String schemaName(SinkRecord sinkRecord) {
    Schema valueSchema = sinkRecord.valueSchema();
    return valueSchema != null ? valueSchema.name() : null;
  }

  private String generateSchemaBasedPath(String encodedPartition, String schemaName) {
    if (schemaAffixType == PartitionerConfig.AffixType.PREFIX) {
      return SCHEMA_NAME_PREFIX + schemaName + delim + encodedPartition;
    } else if (schemaAffixType == PartitionerConfig.AffixType.SUFFIX) {
      return encodedPartition + delim + SCHEMA_NAME_PREFIX + schemaName;
    }
    return encodedPartition;
  }

  @Override
  public String generatePartitionedPath(String topic, String encodedPartition) {
    return delegatePartitioner.generatePartitionedPath(topic, encodedPartition);
  }

  @Override
  public List<T> partitionFields() {
    return delegatePartitioner.partitionFields();
  }
}
