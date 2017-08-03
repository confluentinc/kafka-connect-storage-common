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

package io.confluent.connect.storage.hive.schema;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import io.confluent.connect.storage.common.SchemaGenerator;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.hive.HiveConfig;

public class TimeBasedSchemaGenerator implements SchemaGenerator<FieldSchema> {
  private final Map<String, Object> config;

  public TimeBasedSchemaGenerator() {
    config = new HashMap<>();
    config.put(HiveConfig.HIVE_INTEGRATION_CONFIG, HiveConfig.HIVE_INTEGRATION_DEFAULT);
    config.put(
        StorageCommonConfig.DIRECTORY_DELIM_CONFIG,
        StorageCommonConfig.DIRECTORY_DELIM_DEFAULT
    );
  }

  public TimeBasedSchemaGenerator(Map<String, Object> config) {
    this.config = config == null ? Collections.<String, Object>emptyMap() : config;
  }

  @Override
  public List<FieldSchema> newPartitionFields(String format) {
    Boolean hiveIntegration = (Boolean) config.get(HiveConfig.HIVE_INTEGRATION_CONFIG);
    hiveIntegration = hiveIntegration == null
                      ? HiveConfig.HIVE_INTEGRATION_DEFAULT
                      : hiveIntegration;
    String delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    delim = delim == null ? StorageCommonConfig.DIRECTORY_DELIM_DEFAULT : delim;
    if (hiveIntegration && !verifyDateTimeFormat(format, delim)) {
      throw new IllegalArgumentException(
          "Path format doesn't meet the requirements for Hive integration, "
          + "which require prefixing each DateTime component with its name."
      );
    }

    List<FieldSchema> fields = new ArrayList<>();

    for (String field : format.split(delim)) {
      String[] parts = field.split("=");
      FieldSchema fieldSchema =
          new FieldSchema(parts[0].replace("'", ""), TypeInfoFactory.stringTypeInfo.toString(), "");
      fields.add(fieldSchema);
    }

    return fields;
  }

  private boolean verifyDateTimeFormat(String pathFormat, String delim) {
    // Path format does not require a trailing delimeter at the end of the path anymore.
    // But since we don't know what's the final component here, a delimiter is artificially added
    // to the pathFormat if needed.
    String extendedPathFormat = pathFormat.endsWith(delim) ? pathFormat : pathFormat + delim;
    String patternString =
        "'year'=Y{1,5}" + delim
            + "('month'=M{1,5}" + delim
            + ")?('day'=d{1,3}" + delim
            + ")?('hour'=H{1,3}" + delim
            + ")?('minute'=m{1,3}" + delim + ")?";
    return Pattern.compile(patternString).matcher(extendedPathFormat).matches();
  }
}
