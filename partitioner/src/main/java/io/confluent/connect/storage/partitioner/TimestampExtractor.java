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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;

import java.util.Map;

import io.confluent.connect.storage.common.GenericRecommender;

public interface TimestampExtractor {
  default ConfigDef.Recommender getPartitionerRecommender() {
    return new GenericRecommender();
  }

  default ConfigDef.Recommender getStorageRecommender() {
    return new GenericRecommender();
  }

  void configure(Map<String, String> props);

  Long extract(ConnectRecord<?> record);
}
