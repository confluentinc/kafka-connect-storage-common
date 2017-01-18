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

  // Partitioner group
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
  public static final String SCHEMA_GENERATOR_CLASS_CONFIG = "schema.generator.class";
  public static final String SCHEMA_GENERATOR_CLASS_DOC =
      "The schema generator to use for integration with Hive. You can use ``DefaultSchemaGenerator``, "
      + "for both ``DefaultPartitioner`` and ``FieldPartitioner`` or ``TimeBasedSchemaGenerator`` for any "
      + "``TimeBasedPartitioner``.";
  public static final String SCHEMA_GENERATOR_CLASS_DEFAULT =
      "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator";
  public static final String SCHEMA_GENERATOR_CLASS_DISPLAY = "Schema Generator Class";

  // CHECKSTYLE:OFF
  public static final ConfigDef.Recommender partitionerClassDependentsRecommender =
      new PartitionerClassDependentsRecommender();
  // CHECKSTYLE:ON

  protected static final ConfigDef CONFIG_DEF = new ConfigDef();

  static {
    {
      // Define Hive configuration group
      final String group = "Partitioner";
      int orderInGroup = 0;

      // Define Connector configuration group
      CONFIG_DEF.define(PARTITIONER_CLASS_CONFIG,
          Type.STRING,
          PARTITIONER_CLASS_DEFAULT,
          Importance.HIGH,
          PARTITIONER_CLASS_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          PARTITIONER_CLASS_DISPLAY,
          Arrays.asList(PARTITION_FIELD_NAME_CONFIG, PARTITION_DURATION_MS_CONFIG, PATH_FORMAT_CONFIG, LOCALE_CONFIG, TIMEZONE_CONFIG, SCHEMA_GENERATOR_CLASS_CONFIG));

      CONFIG_DEF.define(PARTITION_FIELD_NAME_CONFIG,
          Type.STRING,
          PARTITION_FIELD_NAME_DEFAULT,
          Importance.MEDIUM,
          PARTITION_FIELD_NAME_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          PARTITION_FIELD_NAME_DISPLAY,
          partitionerClassDependentsRecommender);

      CONFIG_DEF.define(PARTITION_DURATION_MS_CONFIG,
          Type.LONG,
          PARTITION_DURATION_MS_DEFAULT,
          Importance.MEDIUM,
          PARTITION_DURATION_MS_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          PARTITION_DURATION_MS_DISPLAY,
          partitionerClassDependentsRecommender);

      CONFIG_DEF.define(PATH_FORMAT_CONFIG,
          Type.STRING,
          PATH_FORMAT_DEFAULT,
          Importance.MEDIUM,
          PATH_FORMAT_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          PATH_FORMAT_DISPLAY,
          partitionerClassDependentsRecommender);

      CONFIG_DEF.define(LOCALE_CONFIG,
          Type.STRING,
          LOCALE_DEFAULT,
          Importance.MEDIUM,
          LOCALE_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          LOCALE_DISPLAY,
          partitionerClassDependentsRecommender);

      CONFIG_DEF.define(TIMEZONE_CONFIG,
          Type.STRING,
          TIMEZONE_DEFAULT,
          Importance.MEDIUM,
          TIMEZONE_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          TIMEZONE_DISPLAY,
          partitionerClassDependentsRecommender);

      CONFIG_DEF.define(SCHEMA_GENERATOR_CLASS_CONFIG,
          Type.STRING,
          SCHEMA_GENERATOR_CLASS_DEFAULT,
          Importance.HIGH,
          SCHEMA_GENERATOR_CLASS_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          SCHEMA_GENERATOR_CLASS_DISPLAY);
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
        Class<? extends Partitioner<?>> partitioner = (Class<? extends Partitioner<?>>) Class.forName(partitionerName);
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
    return CONFIG_DEF;
  }

  public PartitionerConfig(Map<String, String> props) {
    super(CONFIG_DEF, props);
  }

  public static void main(String[] args) {
    System.out.println(CONFIG_DEF.toRst());
  }
}
