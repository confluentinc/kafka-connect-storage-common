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

package io.confluent.connect.storage;

import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Arrays;
import java.util.Map;

public class StorageSinkConnectorConfig extends AbstractConfig {

  // This config is deprecated and will be removed in future releases. Use store.url instead.
  public static final String HDFS_URL_CONFIG = HiveConfig.HDFS_URL_CONFIG;
  public static final String HDFS_URL_DOC = HiveConfig.HDFS_URL_DOC;
  public static final String HDFS_URL_DISPLAY = HiveConfig.HDFS_URL_DISPLAY;

  public static final String STORE_URL_CONFIG = HiveConfig.STORE_URL_CONFIG;
  public static final String STORE_URL_DOC = HiveConfig.STORE_URL_DOC;
  public static final String STORE_URL_DEFAULT = HiveConfig.STORE_URL_DEFAULT;
  public static final String STORE_URL_DISPLAY = HiveConfig.STORE_URL_DISPLAY;

  public static final String TOPICS_DIR_CONFIG = HiveConfig.TOPICS_DIR_CONFIG;
  public static final String TOPICS_DIR_DOC = HiveConfig.TOPICS_DIR_DOC;
  public static final String TOPICS_DIR_DEFAULT = HiveConfig.TOPICS_DIR_DEFAULT;
  public static final String TOPICS_DIR_DISPLAY = HiveConfig.TOPICS_DIR_DISPLAY;

  public static final String LOGS_DIR_CONFIG = "logs.dir";
  public static final String LOGS_DIR_DOC =
      "Top level directory to store the write ahead logs.";
  public static final String LOGS_DIR_DEFAULT = "logs";
  public static final String LOGS_DIR_DISPLAY = "Logs directory";

  public static final String FORMAT_CLASS_CONFIG = "format.class";
  public static final String FORMAT_CLASS_DOC =
      "The format class to use when writing data to the store. ";
  public static final String FORMAT_CLASS_DEFAULT = "io.confluent.connect.hdfs.avro.AvroFormat";
  public static final String FORMAT_CLASS_DISPLAY = "Format class";

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

  // Connector group
  public static final String FLUSH_SIZE_CONFIG = "flush.size";
  public static final String FLUSH_SIZE_DOC =
      "Number of records written to store before invoking file commits.";
  public static final String FLUSH_SIZE_DISPLAY = "Flush Size";

  public static final String ROTATE_INTERVAL_MS_CONFIG = "rotate.interval.ms";
  public static final String ROTATE_INTERVAL_MS_DOC =
      "The time interval in milliseconds to invoke file commits. This configuration ensures that "
          + "file commits are invoked every configured interval. This configuration is useful when data "
          + "ingestion rate is low and the connector didn't write enough messages to commit files."
          + "The default value -1 means that this feature is disabled.";
  public static final long ROTATE_INTERVAL_MS_DEFAULT = -1L;
  public static final String ROTATE_INTERVAL_MS_DISPLAY = "Rotate Interval (ms)";

  public static final String ROTATE_SCHEDULE_INTERVAL_MS_CONFIG = "rotate.schedule.interval.ms";
  public static final String ROTATE_SCHEDULE_INTERVAL_MS_DOC =
      "The time interval in milliseconds to periodically invoke file commits. This configuration ensures that "
          + "file commits are invoked every configured interval. Time of commit will be adjusted to 00:00 of selected timezone. "
          + "Commit will be performed at scheduled time regardless previous commit time or number of messages. "
          + "This configuration is useful when you have to commit your data based on current server time, like at the beginning of every hour. "
          + "The default value -1 means that this feature is disabled.";
  public static final long ROTATE_SCHEDULE_INTERVAL_MS_DEFAULT = -1L;
  public static final String ROTATE_SCHEDULE_INTERVAL_MS_DISPLAY = "Rotate Schedule Interval (ms)";

  public static final String RETRY_BACKOFF_CONFIG = "retry.backoff.ms";
  public static final String RETRY_BACKOFF_DOC =
      "The retry backoff in milliseconds. This config is used to "
          + "notify Kafka connect to retry delivering a message batch or performing recovery in case "
          + "of transient exceptions.";
  public static final long RETRY_BACKOFF_DEFAULT = 5000L;
  public static final String RETRY_BACKOFF_DISPLAY = "Retry Backoff (ms)";

  public static final String SHUTDOWN_TIMEOUT_CONFIG = "shutdown.timeout.ms";
  public static final String SHUTDOWN_TIMEOUT_DOC =
      "Clean shutdown timeout. This makes sure that asynchronous Hive metastore updates are "
          + "completed during connector shutdown.";
  public static final long SHUTDOWN_TIMEOUT_DEFAULT = 3000L;
  public static final String SHUTDOWN_TIMEOUT_DISPLAY = "Shutdown Timeout (ms)";

  public static final String PARTITIONER_CLASS_CONFIG = PartitionerConfig.PARTITIONER_CLASS_CONFIG;
  public static final String PARTITIONER_CLASS_DOC = PartitionerConfig.PARTITIONER_CLASS_DOC;
  public static final String PARTITIONER_CLASS_DEFAULT = PartitionerConfig.PARTITIONER_CLASS_DEFAULT;
  public static final String PARTITIONER_CLASS_DISPLAY = PartitionerConfig.PARTITIONER_CLASS_DISPLAY;

  public static final String PARTITION_FIELD_NAME_CONFIG = PartitionerConfig.PARTITION_FIELD_NAME_CONFIG;
  public static final String PARTITION_FIELD_NAME_DOC = PartitionerConfig.PARTITION_FIELD_NAME_CONFIG;
  public static final String PARTITION_FIELD_NAME_DEFAULT = PartitionerConfig.PARTITION_FIELD_NAME_CONFIG;
  public static final String PARTITION_FIELD_NAME_DISPLAY = PartitionerConfig.PARTITION_FIELD_NAME_CONFIG;

  public static final String PARTITION_DURATION_MS_CONFIG = PartitionerConfig.PARTITION_DURATION_MS_CONFIG;
  public static final String PARTITION_DURATION_MS_DOC = PartitionerConfig.PARTITION_DURATION_MS_DOC;
  public static final long PARTITION_DURATION_MS_DEFAULT = PartitionerConfig.PARTITION_DURATION_MS_DEFAULT;
  public static final String PARTITION_DURATION_MS_DISPLAY = PartitionerConfig.PARTITION_DURATION_MS_DISPLAY;

  public static final String PATH_FORMAT_CONFIG = PartitionerConfig.PATH_FORMAT_CONFIG;
  public static final String PATH_FORMAT_DOC = PartitionerConfig.PATH_FORMAT_DOC;
  public static final String PATH_FORMAT_DEFAULT = PartitionerConfig.PATH_FORMAT_DEFAULT;
  public static final String PATH_FORMAT_DISPLAY = PartitionerConfig.PATH_FORMAT_DISPLAY;

  public static final String LOCALE_CONFIG = PartitionerConfig.LOCALE_CONFIG;
  public static final String LOCALE_DOC = PartitionerConfig.LOCALE_DOC;
  public static final String LOCALE_DEFAULT = PartitionerConfig.LOCALE_DEFAULT;
  public static final String LOCALE_DISPLAY = PartitionerConfig.LOCALE_DISPLAY;

  public static final String TIMEZONE_CONFIG = PartitionerConfig.TIMEZONE_CONFIG;
  public static final String TIMEZONE_DOC = PartitionerConfig.TIMEZONE_DOC;
  public static final String TIMEZONE_DEFAULT = PartitionerConfig.TIMEZONE_DEFAULT;
  public static final String TIMEZONE_DISPLAY = PartitionerConfig.TIMEZONE_DISPLAY;

  public static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG = "filename.offset.zero.pad.width";
  public static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_DOC =
      "Width to zero pad offsets in store's filenames if offsets are too short in order to "
          + "provide fixed width filenames that can be ordered by simple lexicographic sorting.";
  public static final int FILENAME_OFFSET_ZERO_PAD_WIDTH_DEFAULT = 10;
  public static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_DISPLAY = "Filename Offset Zero Pad Width";

  // Schema group
  public static final String SCHEMA_COMPATIBILITY_CONFIG = PartitionerConfig.SCHEMA_COMPATIBILITY_CONFIG;
  public static final String SCHEMA_COMPATIBILITY_DOC = PartitionerConfig.SCHEMA_COMPATIBILITY_DOC;
  public static final String SCHEMA_COMPATIBILITY_DEFAULT = PartitionerConfig.SCHEMA_COMPATIBILITY_DEFAULT;
  public static final String SCHEMA_COMPATIBILITY_DISPLAY = PartitionerConfig.SCHEMA_COMPATIBILITY_DISPLAY;

  public static final String SCHEMA_CACHE_SIZE_CONFIG = PartitionerConfig.SCHEMA_CACHE_SIZE_CONFIG;
  public static final String SCHEMA_CACHE_SIZE_DOC = PartitionerConfig.SCHEMA_CACHE_SIZE_DOC;
  public static final int SCHEMA_CACHE_SIZE_DEFAULT = PartitionerConfig.SCHEMA_CACHE_SIZE_DEFAULT;
  public static final String SCHEMA_CACHE_SIZE_DISPLAY = PartitionerConfig.SCHEMA_CACHE_SIZE_DISPLAY;

  // Internal group
  public static final String STORAGE_CLASS_CONFIG = "storage.class";
  public static final String STORAGE_CLASS_DOC =
      "The underlying storage layer. The default is HDFS.";
  public static final String STORAGE_CLASS_DEFAULT = "io.confluent.connect.hdfs.storage.HdfsStorage";
  public static final String STORAGE_CLASS_DISPLAY = "Storage Class";

  public static final String STORE_GROUP = "Store";
  public static final String HIVE_GROUP = "Hive";
  public static final String SCHEMA_GROUP = "Schema";
  public static final String CONNECTOR_GROUP = "Connector";
  public static final String INTERNAL_GROUP = "Internal";

  // CHECKSTYLE:OFF
  public static final ConfigDef.Recommender hiveIntegrationDependentsRecommender = PartitionerConfig.hiveIntegrationDependentsRecommender;
  public static final ConfigDef.Recommender schemaCompatibilityRecommender = PartitionerConfig.schemaCompatibilityRecommender;
  public static final ConfigDef.Recommender partitionerClassDependentsRecommender = PartitionerConfig.hiveIntegrationDependentsRecommender;
  // CHECKSTYLE:ON

  protected static ConfigDef config = new ConfigDef();

  static {
    // Define Store's basic configuration group
    config.define(STORE_URL_CONFIG, Type.STRING, STORE_URL_DEFAULT, Importance.HIGH, STORE_URL_DOC, STORE_GROUP, 1, Width.MEDIUM, STORE_URL_DISPLAY);

    // HDFS_URL_CONFIG property is retained for backwards compatibility with HDFS connector and will be removed in future versions.
    config.define(HDFS_URL_CONFIG, Type.STRING, Importance.HIGH, HDFS_URL_DOC, STORE_GROUP, 1, Width.MEDIUM, HDFS_URL_DISPLAY);

    config.define(TOPICS_DIR_CONFIG, Type.STRING, TOPICS_DIR_DEFAULT, Importance.HIGH, TOPICS_DIR_DOC, STORE_GROUP, 4, Width.SHORT, TOPICS_DIR_DISPLAY)
        .define(LOGS_DIR_CONFIG, Type.STRING, LOGS_DIR_DEFAULT, Importance.HIGH, LOGS_DIR_DOC, STORE_GROUP, 5, Width.SHORT, LOGS_DIR_DISPLAY)
        .define(FORMAT_CLASS_CONFIG, Type.STRING, FORMAT_CLASS_DEFAULT, Importance.HIGH, FORMAT_CLASS_DOC, STORE_GROUP, 6, Width.SHORT, FORMAT_CLASS_DISPLAY);

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

    // Define Connector configuration group
    config.define(FLUSH_SIZE_CONFIG, Type.INT, Importance.HIGH, FLUSH_SIZE_DOC, CONNECTOR_GROUP, 1, Width.SHORT, FLUSH_SIZE_DISPLAY)
        .define(ROTATE_INTERVAL_MS_CONFIG, Type.LONG, ROTATE_INTERVAL_MS_DEFAULT, Importance.HIGH, ROTATE_INTERVAL_MS_DOC, CONNECTOR_GROUP, 2, Width.SHORT, ROTATE_INTERVAL_MS_DISPLAY)
        .define(ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, Type.LONG, ROTATE_SCHEDULE_INTERVAL_MS_DEFAULT, Importance.MEDIUM, ROTATE_SCHEDULE_INTERVAL_MS_DOC, CONNECTOR_GROUP, 3, Width.SHORT, ROTATE_SCHEDULE_INTERVAL_MS_DISPLAY)
        .define(RETRY_BACKOFF_CONFIG, Type.LONG, RETRY_BACKOFF_DEFAULT, Importance.LOW, RETRY_BACKOFF_DOC, CONNECTOR_GROUP, 4, Width.SHORT, RETRY_BACKOFF_DISPLAY)
        .define(SHUTDOWN_TIMEOUT_CONFIG, Type.LONG, SHUTDOWN_TIMEOUT_DEFAULT, Importance.MEDIUM, SHUTDOWN_TIMEOUT_DOC, CONNECTOR_GROUP, 5, Width.SHORT, SHUTDOWN_TIMEOUT_DISPLAY)
        .define(PARTITIONER_CLASS_CONFIG, Type.STRING, PARTITIONER_CLASS_DEFAULT, Importance.HIGH, PARTITIONER_CLASS_DOC, CONNECTOR_GROUP, 6, Width.LONG, PARTITIONER_CLASS_DISPLAY,
                Arrays.asList(PARTITION_FIELD_NAME_CONFIG, PARTITION_DURATION_MS_CONFIG, PATH_FORMAT_CONFIG, LOCALE_CONFIG, TIMEZONE_CONFIG))
        .define(PARTITION_FIELD_NAME_CONFIG, Type.STRING, PARTITION_FIELD_NAME_DEFAULT, Importance.MEDIUM, PARTITION_FIELD_NAME_DOC, CONNECTOR_GROUP, 7, Width.MEDIUM,
                PARTITION_FIELD_NAME_DISPLAY, partitionerClassDependentsRecommender)
        .define(PARTITION_DURATION_MS_CONFIG, Type.LONG, PARTITION_DURATION_MS_DEFAULT, Importance.MEDIUM, PARTITION_DURATION_MS_DOC, CONNECTOR_GROUP, 8, Width.SHORT,
                PARTITION_DURATION_MS_DISPLAY, partitionerClassDependentsRecommender)
        .define(PATH_FORMAT_CONFIG, Type.STRING, PATH_FORMAT_DEFAULT, Importance.MEDIUM, PATH_FORMAT_DOC, CONNECTOR_GROUP, 9, Width.LONG, PATH_FORMAT_DISPLAY,
                partitionerClassDependentsRecommender)
        .define(LOCALE_CONFIG, Type.STRING, LOCALE_DEFAULT, Importance.MEDIUM, LOCALE_DOC, CONNECTOR_GROUP, 10, Width.MEDIUM, LOCALE_DISPLAY, partitionerClassDependentsRecommender)
        .define(TIMEZONE_CONFIG, Type.STRING, TIMEZONE_DEFAULT, Importance.MEDIUM, TIMEZONE_DOC, CONNECTOR_GROUP, 11, Width.MEDIUM, TIMEZONE_DISPLAY, partitionerClassDependentsRecommender)
        .define(FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG, Type.INT, FILENAME_OFFSET_ZERO_PAD_WIDTH_DEFAULT, ConfigDef.Range.atLeast(0), Importance.LOW, FILENAME_OFFSET_ZERO_PAD_WIDTH_DOC,
                CONNECTOR_GROUP, 12, Width.SHORT, FILENAME_OFFSET_ZERO_PAD_WIDTH_DISPLAY);

    // Define Internal configuration group
    config.define(STORAGE_CLASS_CONFIG, Type.STRING, STORAGE_CLASS_DEFAULT, Importance.LOW, STORAGE_CLASS_DOC, INTERNAL_GROUP, 1, Width.MEDIUM, STORAGE_CLASS_DISPLAY);
  }

  public static ConfigDef getConfig() {
    return config;
  }

  public StorageSinkConnectorConfig(Map<String, String> props) {
    super(config, props);
  }

  public static void main(String[] args) {
    System.out.println(config.toRst());
  }
}
