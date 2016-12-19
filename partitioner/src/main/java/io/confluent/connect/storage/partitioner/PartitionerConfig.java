/*
 * Copyright 2016 Confluent Inc.
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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PartitionerConfig extends AbstractConfig {

  // Hive group
  public static final String HIVE_INTEGRATION_CONFIG = "hive.integration";
  public static final String HIVE_INTEGRATION_DOC =
      "Configuration indicating whether to integrate with Hive when running the connector.";
  public static final boolean HIVE_INTEGRATION_DEFAULT = false;
  public static final String HIVE_INTEGRATION_DISPLAY = "Hive Integration";

  public static final String HIVE_METASTORE_URIS_CONFIG = "hive.metastore.uris";
  public static final String HIVE_METASTORE_URIS_DOC =
      "The Hive metastore URIs, can be IP address or fully-qualified domain name "
      + "and port of the metastore host.";
  public static final String HIVE_METASTORE_URIS_DEFAULT = "";
  public static final String HIVE_METASTORE_URIS_DISPLAY = "Hive Metastore URIs";

  public static final String HIVE_CONF_DIR_CONFIG = "hive.conf.dir";
  public static final String HIVE_CONF_DIR_DOC = "Hive configuration directory";
  public static final String HIVE_CONF_DIR_DEFAULT = "";
  public static final String HIVE_CONF_DIR_DISPLAY = "Hive configuration directory.";

  public static final String HIVE_HOME_CONFIG = "hive.home";
  public static final String HIVE_HOME_DOC = "Hive home directory.";
  public static final String HIVE_HOME_DEFAULT = "";
  public static final String HIVE_HOME_DISPLAY = "Hive home directory";

  public static final String HIVE_DATABASE_CONFIG = "hive.database";
  public static final String HIVE_DATABASE_DOC =
      "The database to use when the connector creates tables in Hive.";
  public static final String HIVE_DATABASE_DEFAULT = "default";
  public static final String HIVE_DATABASE_DISPLAY = "Hive database";

  public static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";
  public static final String PARTITIONER_CLASS_DOC =
      "The partitioner to use when writing data to the store. You can use ``DefaultPartitioner``, "
      + "which preserves the Kafka partitions; ``FieldPartitioner``, which partitions the data to "
      + "different directories according to the value of the partitioning field specified "
      + "in ``partition.field.name``; ``TimebasedPartitioner``, which partitions data "
      + "according to ingestion time.";
  public static final String PARTITIONER_CLASS_DEFAULT =
      "io.confluent.connect.hdfs.partitioner.DefaultPartitioner";
  public static final String PARTITIONER_CLASS_DISPLAY = "Partitioner Class";

  public static final String PARTITION_FIELD_NAME_CONFIG = "partition.field.name";
  public static final String PARTITION_FIELD_NAME_DOC =
      "The name of the partitioning field when FieldPartitioner is used.";
  public static final String PARTITION_FIELD_NAME_DEFAULT = "";
  public static final String PARTITION_FIELD_NAME_DISPLAY = "Partition Field Name";

  public static final String PARTITION_DURATION_MS_CONFIG = "partition.duration.ms";
  public static final String PARTITION_DURATION_MS_DOC =
      "The duration of a partition milliseconds used by ``TimeBasedPartitioner``. "
      + "The default value -1 means that we are not using ``TimebasedPartitioner``.";
  public static final long PARTITION_DURATION_MS_DEFAULT = -1L;
  public static final String PARTITION_DURATION_MS_DISPLAY = "Partition Duration (ms)";

  public static final String PATH_FORMAT_CONFIG = "path.format";
  public static final String PATH_FORMAT_DOC =
      "This configuration is used to set the format of the data directories when partitioning with "
      + "``TimeBasedPartitioner``. The format set in this configuration converts the Unix timestamp "
      + "to proper directories strings. For example, if you set "
      + "``path.format='year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/``, the data directories will have"
      + " the format ``/year=2015/month=12/day=07/hour=15``.";
  public static final String PATH_FORMAT_DEFAULT = "";
  public static final String PATH_FORMAT_DISPLAY = "Path Format";

  public static final String LOCALE_CONFIG = "locale";
  public static final String LOCALE_DOC =
      "The locale to use when partitioning with ``TimeBasedPartitioner``.";
  public static final String LOCALE_DEFAULT = "";
  public static final String LOCALE_DISPLAY = "Locale";

  public static final String TIMEZONE_CONFIG = "timezone";
  public static final String TIMEZONE_DOC =
      "The timezone to use when partitioning with ``TimeBasedPartitioner``.";
  public static final String TIMEZONE_DEFAULT = "";
  public static final String TIMEZONE_DISPLAY = "Timezone";

  // Schema group
  public static final String SCHEMA_COMPATIBILITY_CONFIG = "schema.compatibility";
  public static final String SCHEMA_COMPATIBILITY_DOC =
      "The schema compatibility rule to use when the connector is observing schema changes. The "
      + "supported configurations are NONE, BACKWARD, FORWARD and FULL.";
  public static final String SCHEMA_COMPATIBILITY_DEFAULT = "NONE";
  public static final String SCHEMA_COMPATIBILITY_DISPLAY = "Schema Compatibility";

  public static final String SCHEMA_CACHE_SIZE_CONFIG = "schema.cache.size";
  public static final String SCHEMA_CACHE_SIZE_DOC =
      "The size of the schema cache used in the Avro converter.";
  public static final int SCHEMA_CACHE_SIZE_DEFAULT = 1000;
  public static final String SCHEMA_CACHE_SIZE_DISPLAY = "Schema Cache Size";

  public static final String HIVE_GROUP = "Hive";
  public static final String SCHEMA_GROUP = "Schema";
  public static final String PARTITIONER_GROUP = "Partitioner";

  // CHECKSTYLE:OFF
  public static final ConfigDef.Recommender hiveIntegrationDependentsRecommender =
      new BooleanParentRecommender(HIVE_INTEGRATION_CONFIG);
  public static final ConfigDef.Recommender schemaCompatibilityRecommender = new SchemaCompatibilityRecommender();
  public static final ConfigDef.Recommender partitionerClassDependentsRecommender =
      new PartitionerClassDependentsRecommender();
  // CHECKSTYLE:ON

  private static ConfigDef config = new ConfigDef();

  static {

    // Define Schema configuration group
    config.define(SCHEMA_COMPATIBILITY_CONFIG, Type.STRING, SCHEMA_COMPATIBILITY_DEFAULT, Importance.HIGH, SCHEMA_COMPATIBILITY_DOC, SCHEMA_GROUP, 1, Width.SHORT,
                  SCHEMA_COMPATIBILITY_DISPLAY, schemaCompatibilityRecommender)
        .define(SCHEMA_CACHE_SIZE_CONFIG, Type.INT, SCHEMA_CACHE_SIZE_DEFAULT, Importance.LOW, SCHEMA_CACHE_SIZE_DOC, SCHEMA_GROUP, 2, Width.SHORT, SCHEMA_CACHE_SIZE_DISPLAY);

    // Define Hive configuration group
    config.define(HIVE_INTEGRATION_CONFIG, Type.BOOLEAN, HIVE_INTEGRATION_DEFAULT, Importance.HIGH, HIVE_INTEGRATION_DOC, HIVE_GROUP, 1, Width.SHORT, HIVE_INTEGRATION_DISPLAY,
        Arrays.asList(HIVE_METASTORE_URIS_CONFIG, HIVE_CONF_DIR_CONFIG, HIVE_HOME_CONFIG, HIVE_DATABASE_CONFIG, SCHEMA_COMPATIBILITY_CONFIG))
        .define(HIVE_METASTORE_URIS_CONFIG, Type.STRING, HIVE_METASTORE_URIS_DEFAULT, Importance.HIGH, HIVE_METASTORE_URIS_DOC, HIVE_GROUP, 2, Width.MEDIUM,
            HIVE_METASTORE_URIS_DISPLAY, hiveIntegrationDependentsRecommender)
        .define(HIVE_CONF_DIR_CONFIG, Type.STRING, HIVE_CONF_DIR_DEFAULT, Importance.HIGH, HIVE_CONF_DIR_DOC, HIVE_GROUP, 3, Width.MEDIUM, HIVE_CONF_DIR_DISPLAY, hiveIntegrationDependentsRecommender)
        .define(HIVE_HOME_CONFIG, Type.STRING, HIVE_HOME_DEFAULT, Importance.HIGH, HIVE_HOME_DOC, HIVE_GROUP, 4, Width.MEDIUM, HIVE_HOME_DISPLAY, hiveIntegrationDependentsRecommender)
        .define(HIVE_DATABASE_CONFIG, Type.STRING, HIVE_DATABASE_DEFAULT, Importance.HIGH, HIVE_DATABASE_DOC, HIVE_GROUP, 5, Width.SHORT, HIVE_DATABASE_DISPLAY, hiveIntegrationDependentsRecommender);

    // Define Connector configuration group
    config.define(PARTITIONER_CLASS_CONFIG, Type.STRING, PARTITIONER_CLASS_DEFAULT, Importance.HIGH, PARTITIONER_CLASS_DOC, PARTITIONER_GROUP, 6, Width.LONG, PARTITIONER_CLASS_DISPLAY,
            Arrays.asList(PARTITION_FIELD_NAME_CONFIG, PARTITION_DURATION_MS_CONFIG, PATH_FORMAT_CONFIG, LOCALE_CONFIG, TIMEZONE_CONFIG))
        .define(PARTITION_FIELD_NAME_CONFIG, Type.STRING, PARTITION_FIELD_NAME_DEFAULT, Importance.MEDIUM, PARTITION_FIELD_NAME_DOC, PARTITIONER_GROUP, 7, Width.MEDIUM,
        PARTITION_FIELD_NAME_DISPLAY, partitionerClassDependentsRecommender)
        .define(PARTITION_DURATION_MS_CONFIG, Type.LONG, PARTITION_DURATION_MS_DEFAULT, Importance.MEDIUM, PARTITION_DURATION_MS_DOC, PARTITIONER_GROUP, 8, Width.SHORT,
            PARTITION_DURATION_MS_DISPLAY, partitionerClassDependentsRecommender)
        .define(PATH_FORMAT_CONFIG, Type.STRING, PATH_FORMAT_DEFAULT, Importance.MEDIUM, PATH_FORMAT_DOC, PARTITIONER_GROUP, 9, Width.LONG, PATH_FORMAT_DISPLAY,
            partitionerClassDependentsRecommender)
        .define(LOCALE_CONFIG, Type.STRING, LOCALE_DEFAULT, Importance.MEDIUM, LOCALE_DOC, PARTITIONER_GROUP, 10, Width.MEDIUM, LOCALE_DISPLAY, partitionerClassDependentsRecommender)
        .define(TIMEZONE_CONFIG, Type.STRING, TIMEZONE_DEFAULT, Importance.MEDIUM, TIMEZONE_DOC, PARTITIONER_GROUP, 11, Width.MEDIUM, TIMEZONE_DISPLAY, partitionerClassDependentsRecommender);
  }

  public static class SchemaCompatibilityRecommender extends BooleanParentRecommender {

    public SchemaCompatibilityRecommender() {
      super(HIVE_INTEGRATION_CONFIG);
    }

    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
      boolean hiveIntegration = (Boolean) connectorConfigs.get(parentConfigName);
      if (hiveIntegration) {
        return Arrays.<Object>asList("BACKWARD", "FORWARD", "FULL");
      } else {
        return Arrays.<Object>asList("NONE", "BACKWARD", "FORWARD", "FULL");
      }
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
      return true;
    }
  }

  public static class BooleanParentRecommender implements ConfigDef.Recommender {
    
    protected String parentConfigName;
    
    public BooleanParentRecommender(String parentConfigName) {
      this.parentConfigName = parentConfigName;
    }
    
    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
      return new LinkedList<>();
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
      return (Boolean) connectorConfigs.get(parentConfigName);
    }
  }

  public static class PartitionerClassDependentsRecommender implements ConfigDef.Recommender {

    @Override
    public List<Object> validValues(String name, Map<String, Object> props) {
      return new LinkedList<>();
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
      String partitionerName = (String) connectorConfigs.get(PARTITIONER_CLASS_CONFIG);
      try {
        @SuppressWarnings("unchecked")
        Class<? extends Partitioner> partitioner = (Class<? extends Partitioner>) Class.forName(partitionerName);
        if (classNameEquals(partitionerName, DefaultPartitioner.class)) {
          return false;
        } else if (FieldPartitioner.class.isAssignableFrom(partitioner)) {
          // subclass of FieldPartitioner
          return name.equals(PARTITION_FIELD_NAME_CONFIG);
        } else if (TimeBasedPartitioner.class.isAssignableFrom(partitioner)) {
          // subclass of TimeBasedPartitioner
          if (classNameEquals(partitionerName, DailyPartitioner.class) || classNameEquals(partitionerName, HourlyPartitioner.class)) {
            return name.equals(LOCALE_CONFIG) || name.equals(TIMEZONE_CONFIG);
          } else {
            return name.equals(PARTITION_DURATION_MS_CONFIG) || name.equals(PATH_FORMAT_CONFIG) || name.equals(LOCALE_CONFIG) || name.equals(TIMEZONE_CONFIG);
          }
        } else {
          throw new ConfigException("Not a valid partitioner class: " + partitionerName);
        }
      } catch (ClassNotFoundException e) {
        throw new ConfigException("Partitioner class not found: " + partitionerName);
      }
    }
  }

  private static boolean classNameEquals(String className, Class<?> clazz) {
    return className.equals(clazz.getSimpleName()) || className.equals(clazz.getCanonicalName());
  }

  public static ConfigDef getConfig() {
    return config;
  }

  public PartitionerConfig(Map<String, String> props) {
    super(config, props);
  }

  public static void main(String[] args) {
    System.out.println(config.toRst());
  }
}
