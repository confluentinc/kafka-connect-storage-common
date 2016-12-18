/*
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file exceptin compliance with the License.
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

import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Arrays;
import java.util.Map;

public class HiveConfig extends AbstractConfig {

  // This config is deprecated and will be removed in future releases. Use store.url instead.
  public static final String HDFS_URL_CONFIG = "hdfs.url";
  public static final String HDFS_URL_DOC =
      "The HDFS connection URL. This configuration has the format of hdfs:://hostname:port and "
      + "specifies the HDFS to export data to. This property is deprecated and will be removed in future releases. "
      + "Use ``store.url`` instead.";
  public static final String HDFS_URL_DEFAULT = null;
  public static final String HDFS_URL_DISPLAY = "HDFS URL";

  public static final String STORE_URL_CONFIG = "store.url";
  public static final String STORE_URL_DOC = "Store's connection URL, if applicable.";
  public static final String STORE_URL_DEFAULT = null;
  public static final String STORE_URL_DISPLAY = "Store URL";

  public static final String TOPICS_DIR_CONFIG = "topics.dir";
  public static final String TOPICS_DIR_DOC = "Top level directory to store the data ingested from Kafka.";
  public static final String TOPICS_DIR_DEFAULT = "topics";
  public static final String TOPICS_DIR_DISPLAY = "Topics directory";

  // Schema group
  public static final String SCHEMA_COMPATIBILITY_CONFIG = PartitionerConfig.SCHEMA_COMPATIBILITY_CONFIG;
  public static final String SCHEMA_COMPATIBILITY_DOC = PartitionerConfig.SCHEMA_COMPATIBILITY_DOC;
  public static final String SCHEMA_COMPATIBILITY_DEFAULT = PartitionerConfig.SCHEMA_COMPATIBILITY_DEFAULT;
  public static final String SCHEMA_COMPATIBILITY_DISPLAY = PartitionerConfig.SCHEMA_COMPATIBILITY_DISPLAY;

  public static final String SCHEMA_CACHE_SIZE_CONFIG = PartitionerConfig.SCHEMA_CACHE_SIZE_CONFIG;
  public static final String SCHEMA_CACHE_SIZE_DOC = PartitionerConfig.SCHEMA_CACHE_SIZE_DOC;
  public static final int SCHEMA_CACHE_SIZE_DEFAULT = PartitionerConfig.SCHEMA_CACHE_SIZE_DEFAULT;
  public static final String SCHEMA_CACHE_SIZE_DISPLAY = PartitionerConfig.SCHEMA_CACHE_SIZE_DISPLAY;

  // Hive group
  public static final String HIVE_INTEGRATION_CONFIG = PartitionerConfig.HIVE_INTEGRATION_CONFIG;
  public static final String HIVE_INTEGRATION_DOC = PartitionerConfig.HIVE_INTEGRATION_DOC;
  public static final boolean HIVE_INTEGRATION_DEFAULT = PartitionerConfig.HIVE_INTEGRATION_DEFAULT;
  public static final String HIVE_INTEGRATION_DISPLAY = PartitionerConfig.HIVE_INTEGRATION_DISPLAY;

  public static final String HIVE_METASTORE_URIS_CONFIG = PartitionerConfig.HIVE_METASTORE_URIS_CONFIG;
  public static final String HIVE_METASTORE_URIS_DOC = PartitionerConfig.HIVE_METASTORE_URIS_DOC;
  public static final String HIVE_METASTORE_URIS_DEFAULT = PartitionerConfig.HIVE_METASTORE_URIS_DEFAULT;
  public static final String HIVE_METASTORE_URIS_DISPLAY = PartitionerConfig.HIVE_METASTORE_URIS_DISPLAY;

  public static final String HIVE_CONF_DIR_CONFIG = PartitionerConfig.HIVE_CONF_DIR_CONFIG;
  public static final String HIVE_CONF_DIR_DOC = PartitionerConfig.HIVE_CONF_DIR_DOC;
  public static final String HIVE_CONF_DIR_DEFAULT = PartitionerConfig.HIVE_CONF_DIR_DEFAULT;
  public static final String HIVE_CONF_DIR_DISPLAY = PartitionerConfig.HIVE_CONF_DIR_DISPLAY;

  public static final String HIVE_HOME_CONFIG = PartitionerConfig.HIVE_HOME_CONFIG;
  public static final String HIVE_HOME_DOC = PartitionerConfig.HIVE_HOME_DOC;
  public static final String HIVE_HOME_DEFAULT = PartitionerConfig.HIVE_HOME_DEFAULT;
  public static final String HIVE_HOME_DISPLAY = PartitionerConfig.HIVE_HOME_DISPLAY;

  public static final String HIVE_DATABASE_CONFIG = PartitionerConfig.HIVE_DATABASE_CONFIG;
  public static final String HIVE_DATABASE_DOC = PartitionerConfig.HIVE_DATABASE_DOC;
  public static final String HIVE_DATABASE_DEFAULT = PartitionerConfig.HIVE_DATABASE_DEFAULT;
  public static final String HIVE_DATABASE_DISPLAY = PartitionerConfig.HIVE_DATABASE_DISPLAY;

  public static final String STORE_GROUP = "Store";
  public static final String HIVE_GROUP = PartitionerConfig.HIVE_GROUP;
  public static final String SCHEMA_GROUP = PartitionerConfig.SCHEMA_GROUP;

  // CHECKSTYLE:OFF
  public static final ConfigDef.Recommender hiveIntegrationDependentsRecommender =
      new PartitionerConfig.BooleanParentRecommender(HIVE_INTEGRATION_CONFIG);
  public static final ConfigDef.Recommender schemaCompatibilityRecommender =
      new PartitionerConfig.SchemaCompatibilityRecommender();
  // CHECKSTYLE:ON

  private static ConfigDef config = new ConfigDef();

  static {
    // Define Store's basic configuration group
    config.define(STORE_URL_CONFIG, Type.STRING, STORE_URL_DEFAULT, Importance.HIGH, STORE_URL_DOC, STORE_GROUP, 1, Width.MEDIUM, STORE_URL_DISPLAY);

    // HDFS_URL_CONFIG property is retained for backwards compatibility with HDFS connector and will be removed in future versions.
    config.define(HDFS_URL_CONFIG, Type.STRING, HDFS_URL_DEFAULT, Importance.HIGH, HDFS_URL_DOC, STORE_GROUP, 2, Width.MEDIUM, HDFS_URL_DISPLAY);

    config.define(TOPICS_DIR_CONFIG, Type.STRING, TOPICS_DIR_DEFAULT, Importance.HIGH, TOPICS_DIR_DOC, STORE_GROUP, 3, Width.SHORT, TOPICS_DIR_DISPLAY);

    // Define Hive configuration group
    config.define(HIVE_INTEGRATION_CONFIG, Type.BOOLEAN, HIVE_INTEGRATION_DEFAULT, Importance.HIGH, HIVE_INTEGRATION_DOC, HIVE_GROUP, 1, Width.SHORT, HIVE_INTEGRATION_DISPLAY,
                  Arrays.asList(HIVE_METASTORE_URIS_CONFIG, HIVE_CONF_DIR_CONFIG, HIVE_HOME_CONFIG, HIVE_DATABASE_CONFIG, SCHEMA_COMPATIBILITY_CONFIG))
        .define(HIVE_METASTORE_URIS_CONFIG, Type.STRING, HIVE_METASTORE_URIS_DEFAULT, Importance.HIGH, HIVE_METASTORE_URIS_DOC, HIVE_GROUP, 2, Width.MEDIUM,
                HIVE_METASTORE_URIS_DISPLAY, hiveIntegrationDependentsRecommender)
        .define(HIVE_CONF_DIR_CONFIG, Type.STRING, HIVE_CONF_DIR_DEFAULT, Importance.HIGH, HIVE_CONF_DIR_DOC, HIVE_GROUP, 3, Width.MEDIUM, HIVE_CONF_DIR_DISPLAY, hiveIntegrationDependentsRecommender)
        .define(HIVE_HOME_CONFIG, Type.STRING, HIVE_HOME_DEFAULT, Importance.HIGH, HIVE_HOME_DOC, HIVE_GROUP, 4, Width.MEDIUM, HIVE_HOME_DISPLAY, hiveIntegrationDependentsRecommender)
        .define(HIVE_DATABASE_CONFIG, Type.STRING, HIVE_DATABASE_DEFAULT, Importance.HIGH, HIVE_DATABASE_DOC, HIVE_GROUP, 5, Width.SHORT, HIVE_DATABASE_DISPLAY, hiveIntegrationDependentsRecommender);

    // Define Schema configuration group
    config.define(SCHEMA_COMPATIBILITY_CONFIG, Type.STRING, SCHEMA_COMPATIBILITY_DEFAULT, Importance.HIGH, SCHEMA_COMPATIBILITY_DOC, SCHEMA_GROUP, 1, Width.SHORT,
                  SCHEMA_COMPATIBILITY_DISPLAY, schemaCompatibilityRecommender)
        .define(SCHEMA_CACHE_SIZE_CONFIG, Type.INT, SCHEMA_CACHE_SIZE_DEFAULT, Importance.LOW, SCHEMA_CACHE_SIZE_DOC, SCHEMA_GROUP, 2, Width.SHORT, SCHEMA_CACHE_SIZE_DISPLAY);
  }

  public static ConfigDef getConfig() {
    return config;
  }

  public HiveConfig(Map<String, String> props) {
    super(config, props);
  }

  public static void main(String[] args) {
    System.out.println(config.toRst());
  }
}
