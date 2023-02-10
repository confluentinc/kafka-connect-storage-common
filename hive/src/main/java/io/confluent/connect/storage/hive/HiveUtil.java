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

package io.confluent.connect.storage.hive;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;

import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.Partitioner;

/**
 * Utility class for integration with Hive.
 */
public abstract class HiveUtil {

  protected String url;
  protected final HiveMetaStore hiveMetaStore;
  protected final String delim;

  public HiveUtil(AbstractConfig connectorConfig, HiveMetaStore hiveMetaStore) {
    this.url = connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG);
    this.hiveMetaStore = hiveMetaStore;
    this.delim = connectorConfig.getString(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
  }

  public abstract void createTable(String database, String tableName, Schema schema,
                                   Partitioner<FieldSchema> partitioner);

  public abstract void alterSchema(String database, String tableName, Schema schema);

  public Table newTable(String database, String table) {
    return new Table(database, hiveMetaStore.tableNameConverter(table));
  }

  public String hiveDirectoryName(String url, String topicsDir, String topic) {
    return url + delim + topicsDir + delim + topic + delim;
  }
}
