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

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Map;

/**
 * Partition incoming records, and generates directories and file names in which to store the
 * incoming records.
 *
 * @param <T> The type representing the field schemas.
 */
public interface Partitioner<T> {
  void configure(Map<String, Object> config);

  String encodePartition(SinkRecord sinkRecord);

  default String encodePartition(SinkRecord sinkRecord, long nowInMillis) {
    return encodePartition(sinkRecord);
  }

  String generatePartitionedPath(String topic, String encodedPartition);

  List<T> partitionFields();
}
