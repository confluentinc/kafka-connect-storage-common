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

import io.confluent.connect.storage.common.GenericRecommender;
import io.confluent.connect.storage.common.StorageCommonConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ChainedPartitioner<T> extends DefaultPartitioner<T> {
  private static final Logger log = LoggerFactory.getLogger(ChainedPartitioner.class);

  private final List<Partitioner<T>> partitionerList = new ArrayList<>();

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, Object> config) {
    super.configure(config);

    List<String> aliasList = (List<String>) config.get(PartitionerConfig.PARTITIONER_CHAIN_CONFIG);
    log.debug("Partitioners alias : {}", aliasList);

    for (String alias : aliasList) {
      try {
        Partitioner<T> partitioner = PartitionerFactory.newPartitioner(alias, config);
        partitionerList.add(partitioner);
        log.info("Partitioner is registered. alias: {}, {}", alias, partitioner.getClass());
      } catch (Exception e) {
        log.error("Fail to registering partitioner. alias: {}, config: {}", alias, config);
        throw new ConfigException(e.getMessage());
      }
    }
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    return getPartitionString(sinkRecord, null);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord, long nowInMillis) {
    return getPartitionString(sinkRecord, nowInMillis);
  }

  private String getPartitionString(SinkRecord sinkRecord, Long nowInMillis) {
    StringBuilder builder = new StringBuilder();

    for (Partitioner<T> partitioner : partitionerList) {
      if (builder.length() > 0) {
        builder.append(delim);
      }

      if (nowInMillis == null) {
        builder.append(partitioner.encodePartition(sinkRecord));
      } else {
        builder.append(partitioner.encodePartition(sinkRecord, nowInMillis));
      }
    }
    return builder.toString();
  }

  @Override
  public List<T> partitionFields() {
    return partitionerList.stream()
      .map(Partitioner::partitionFields)
      .reduce(new ArrayList<>(), (p1, p2) -> {
        p1.addAll(p2);
        return p1;
      });
  }

  private static class PartitionerFactory {
    private static final ConfigDef STORAGE_CONFIG_DEF
        = StorageCommonConfig.newConfigDef(new GenericRecommender());
    private static final ConfigDef PARTITION_CONFIG_DEF
        = PartitionerConfig.newConfigDef(new GenericRecommender());

    @SuppressWarnings("unchecked")
    public static <T> Partitioner<T> newPartitioner(
        String alias,
        Map<String, Object> config
    ) throws ClassNotFoundException {
      String prefix = PartitionerConfig.PARTITIONER_CHAIN_CONFIG + "." + alias + ".";
      String partitionerClassName = (String) config.get(prefix + "class");
      Partitioner<T> partitioner
          = Utils.newInstance(partitionerClassName, Partitioner.class);

      Map<String, Object> partitionerConfig = new HashMap<>();
      partitionerConfig.putAll(getStorageConfig(config));
      partitionerConfig.putAll(getPartitionConfig(prefix, config));
      partitioner.configure(partitionerConfig);
      return partitioner;
    }

    private static Map<String, Object> getStorageConfig(Map<String, Object> config) {
      Map<String, Object> storageConfig = new HashMap<>();
      Set<String> storageConfigKeySet = STORAGE_CONFIG_DEF.names();
      for (String key : storageConfigKeySet) {
        if (config.containsKey(key)) {
          storageConfig.put(key, config.get(key));
        }
      }
      return storageConfig;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getPartitionConfig(
        String prefix,
        Map<String, Object> config
    ) {
      Map<String, String> props = new HashMap<>();
      for (String key : config.keySet()) {
        if (key.startsWith(prefix)) {
          String originalKey = key.replace(prefix, "");
          props.put(originalKey, config.get(key).toString());
        }
      }
      PartitionerConfig partitionerConfig = new PartitionerConfig(PARTITION_CONFIG_DEF, props);
      return (Map<String, Object>) partitionerConfig.values();
    }
  }
}
