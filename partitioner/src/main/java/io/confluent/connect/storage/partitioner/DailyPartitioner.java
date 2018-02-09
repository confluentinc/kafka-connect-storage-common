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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.confluent.connect.storage.common.StorageCommonConfig;

public class DailyPartitioner<T> extends TimeBasedPartitioner<T> {
  @Override
  public void configure(Map<String, Object> config) {
    long partitionDurationMs = TimeUnit.DAYS.toMillis(1);

    String delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    String pathFormat = "'year'=YYYY" + delim + "'month'=MM" + delim + "'day'=dd";

    config.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, partitionDurationMs);
    config.put(PartitionerConfig.PATH_FORMAT_CONFIG, pathFormat);

    super.configure(config);
  }
}
