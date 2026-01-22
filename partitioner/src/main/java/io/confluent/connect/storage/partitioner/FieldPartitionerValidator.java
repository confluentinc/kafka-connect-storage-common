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

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;

import java.util.List;
import java.util.Map;

public class FieldPartitionerValidator {

  private final Map<String, String> connectorConfigs;
  private final Config config;

  public FieldPartitionerValidator(Map<String, String> connectorConfigs, Config config) {
    this.connectorConfigs = connectorConfigs;
    this.config = config;
  }

  public Config validate() {
    if (!isFieldPartitioner()) {
      return config;
    }

    String fieldName = connectorConfigs.get(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
    if (fieldName != null && !fieldName.trim().isEmpty()) {
      return config;
    }

    ConfigValue configValue = findConfigValue(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
    if (configValue != null) {
      configValue.addErrorMessage(
          "Partition field name cannot be null or empty when using FieldPartitioner."
      );
    }
    return config;
  }

  private boolean isFieldPartitioner() {
    String partitioner = connectorConfigs.get(PartitionerConfig.PARTITIONER_CLASS_CONFIG);
    if (partitioner == null) {
      return false;
    }

    try {
      Class<?> partitionerClass = Class.forName(
          partitioner, false, FieldPartitioner.class.getClassLoader()
      );
      return FieldPartitioner.class.isAssignableFrom(partitionerClass);
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  private ConfigValue findConfigValue(String name) {
    List<ConfigValue> configValues = config.configValues();
    if (configValues == null) {
      return null;
    }
    for (ConfigValue configValue : configValues) {
      if (name.equals(configValue.name())) {
        return configValue;
      }
    }
    return null;
  }
}
