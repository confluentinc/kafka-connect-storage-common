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

package io.confluent.connect.storage;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.connect.storage.common.ComposableConfig;

public class StorageSinkConnectorConfig extends AbstractConfig implements ComposableConfig {

  // Connector group
  public static final String FORMAT_CLASS_CONFIG = "format.class";
  public static final String FORMAT_CLASS_DOC =
      "The format class to use when writing data to the store. ";
  public static final String FORMAT_CLASS_DISPLAY = "Format class";

  public static final String FLUSH_SIZE_CONFIG = "flush.size";
  public static final String FLUSH_SIZE_DOC =
      "Number of records written to store before invoking file commits.";
  public static final String FLUSH_SIZE_DISPLAY = "Flush Size";

  public static final String ROTATE_INTERVAL_MS_CONFIG = "rotate.interval.ms";
  public static final String
      ROTATE_INTERVAL_MS_DOC =
      "The time interval in milliseconds to invoke file commits. This configuration ensures that "
      + "file commits are invoked every configured interval. This configuration is useful when "
      + "data ingestion rate is low and the connector didn't write enough messages to commit "
      + "files. The default value -1 means that this feature is disabled.";
  public static final long ROTATE_INTERVAL_MS_DEFAULT = -1L;
  public static final String ROTATE_INTERVAL_MS_DISPLAY = "Rotate Interval (ms)";

  public static final String ROTATE_SCHEDULE_INTERVAL_MS_CONFIG = "rotate.schedule.interval.ms";
  public static final String ROTATE_SCHEDULE_INTERVAL_MS_DOC =
      "The time interval in milliseconds to periodically invoke file commits. This configuration "
      + "ensures that file commits are invoked every configured interval. Time of commit will be "
      + "adjusted to 00:00 of selected timezone. Commit will be performed at scheduled time "
      + "regardless previous commit time or number of messages. This configuration is useful when"
      + " you have to commit your data based on current server time, like at the beginning of "
      + "every hour. The default value -1 means that this feature is disabled.";
  public static final long ROTATE_SCHEDULE_INTERVAL_MS_DEFAULT = -1L;
  public static final String ROTATE_SCHEDULE_INTERVAL_MS_DISPLAY = "Rotate Schedule Interval (ms)";

  public static final String RETRY_BACKOFF_CONFIG = "retry.backoff.ms";
  public static final String
      RETRY_BACKOFF_DOC =
      "The retry backoff in milliseconds. This config is used to notify Kafka connect to retry "
      + "delivering a message batch or performing recovery in case of transient exceptions.";
  public static final long RETRY_BACKOFF_DEFAULT = 5000L;
  public static final String RETRY_BACKOFF_DISPLAY = "Retry Backoff (ms)";

  public static final String SHUTDOWN_TIMEOUT_CONFIG = "shutdown.timeout.ms";
  public static final String
      SHUTDOWN_TIMEOUT_DOC =
      "Clean shutdown timeout. This makes sure that asynchronous Hive metastore updates are "
      + "completed during connector shutdown.";
  public static final long SHUTDOWN_TIMEOUT_DEFAULT = 3000L;
  public static final String SHUTDOWN_TIMEOUT_DISPLAY = "Shutdown Timeout (ms)";

  public static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG =
      "filename.offset.zero.pad.width";
  public static final String
      FILENAME_OFFSET_ZERO_PAD_WIDTH_DOC =
      "Width to zero pad offsets in store's filenames if offsets are too short in order to "
      + "provide fixed width filenames that can be ordered by simple lexicographic sorting.";
  public static final int FILENAME_OFFSET_ZERO_PAD_WIDTH_DEFAULT = 10;
  public static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_DISPLAY =
      "Filename Offset Zero Pad Width";

  public static final String SCHEMA_CACHE_SIZE_CONFIG = "schema.cache.size";
  public static final String SCHEMA_CACHE_SIZE_DOC =
      "The size of the schema cache used in the Avro converter.";
  public static final int SCHEMA_CACHE_SIZE_DEFAULT = 1000;
  public static final String SCHEMA_CACHE_SIZE_DISPLAY = "Schema Cache Size";

  public static final String ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG = "enhanced.avro.schema.support";
  public static final boolean ENHANCED_AVRO_SCHEMA_SUPPORT_DEFAULT = false;
  public static final String ENHANCED_AVRO_SCHEMA_SUPPORT_DOC =
      "Enable enhanced avro schema support in AvroConverter: Enum symbol preservation and Package"
          + " Name awareness";
  public static final String ENHANCED_AVRO_SCHEMA_SUPPORT_DISPLAY = "Enhanced Avro Support";

  public static final String CONNECT_META_DATA_CONFIG = "connect.meta.data";
  public static final boolean CONNECT_META_DATA_DEFAULT = true;
  public static final String CONNECT_META_DATA_DOC =
      "Allow connect converter to add its meta data to the output schema";
  public static final String CONNECT_META_DATA_DISPLAY = "Connect Metadata";

  public static final String AVRO_CODEC_CONFIG = "avro.codec";
  public static final String AVRO_CODEC_DEFAULT = "null";
  public static final String AVRO_CODEC_DISPLAY = "Avro Compression Codec";
  public static final String AVRO_CODEC_DOC = "The Avro compression codec to be used for output  "
      + "files. Available values: null, deflate, snappy and bzip2 (CodecSource is org.apache"
      + ".avro.file.CodecFactory)";
  public static final String[] AVRO_SUPPORTED_CODECS = new String[]{"null", "deflate", "snappy",
      "bzip2"};

  /**
   * Create a new configuration definition.
   *
   * @param formatClassRecommender A recommender for format classes shipping out-of-the-box with
   *     a connector. The recommender should not prevent additional custom classes from being
   *     added during runtime.
   * @param  avroRecommender A recommender for avro compression codecs.
   * @return the newly created configuration definition.
   */
  public static ConfigDef newConfigDef(
      ConfigDef.Recommender formatClassRecommender,
      ConfigDef.Recommender avroRecommender
  ) {
    ConfigDef configDef = new ConfigDef();
    {
      // Define Store's basic configuration group
      final String group = "Connector";
      int orderInGroup = 0;

      configDef.define(
          FORMAT_CLASS_CONFIG,
          Type.CLASS,
          Importance.HIGH,
          FORMAT_CLASS_DOC,
          group,
          ++orderInGroup,
          Width.NONE,
          FORMAT_CLASS_DISPLAY,
          formatClassRecommender
      );

      configDef.define(
          FLUSH_SIZE_CONFIG,
          Type.INT,
          Importance.HIGH,
          FLUSH_SIZE_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          FLUSH_SIZE_DISPLAY
      );

      configDef.define(
          ROTATE_INTERVAL_MS_CONFIG,
          Type.LONG,
          ROTATE_INTERVAL_MS_DEFAULT,
          Importance.HIGH,
          ROTATE_INTERVAL_MS_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          ROTATE_INTERVAL_MS_DISPLAY
      );

      configDef.define(
          ROTATE_SCHEDULE_INTERVAL_MS_CONFIG,
          Type.LONG,
          ROTATE_SCHEDULE_INTERVAL_MS_DEFAULT,
          Importance.MEDIUM,
          ROTATE_SCHEDULE_INTERVAL_MS_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          ROTATE_SCHEDULE_INTERVAL_MS_DISPLAY
      );

      configDef.define(
          SCHEMA_CACHE_SIZE_CONFIG,
          Type.INT,
          SCHEMA_CACHE_SIZE_DEFAULT,
          Importance.LOW,
          SCHEMA_CACHE_SIZE_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          SCHEMA_CACHE_SIZE_DISPLAY
      );

      configDef.define(
          ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG,
          Type.BOOLEAN,
          ENHANCED_AVRO_SCHEMA_SUPPORT_DEFAULT,
          Importance.LOW,
          ENHANCED_AVRO_SCHEMA_SUPPORT_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          ENHANCED_AVRO_SCHEMA_SUPPORT_DISPLAY
      );

      configDef.define(
          CONNECT_META_DATA_CONFIG,
          Type.BOOLEAN,
          CONNECT_META_DATA_DEFAULT,
          Importance.LOW,
          CONNECT_META_DATA_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          CONNECT_META_DATA_DISPLAY
      );

      configDef.define(
          RETRY_BACKOFF_CONFIG,
          Type.LONG,
          RETRY_BACKOFF_DEFAULT,
          Importance.LOW,
          RETRY_BACKOFF_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          RETRY_BACKOFF_DISPLAY
      );

      configDef.define(
          SHUTDOWN_TIMEOUT_CONFIG,
          Type.LONG,
          SHUTDOWN_TIMEOUT_DEFAULT,
          Importance.MEDIUM,
          SHUTDOWN_TIMEOUT_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          SHUTDOWN_TIMEOUT_DISPLAY
      );

      configDef.define(
          FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG,
          Type.INT,
          FILENAME_OFFSET_ZERO_PAD_WIDTH_DEFAULT,
          ConfigDef.Range.atLeast(0),
          Importance.LOW,
          FILENAME_OFFSET_ZERO_PAD_WIDTH_DOC,
          group,
          ++orderInGroup,
          Width.LONG,
          FILENAME_OFFSET_ZERO_PAD_WIDTH_DISPLAY
      );

      configDef.define(
          AVRO_CODEC_CONFIG,
          Type.STRING,
          AVRO_CODEC_DEFAULT,
          ConfigDef.ValidString.in(AVRO_SUPPORTED_CODECS),
          Importance.LOW,
          AVRO_CODEC_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          AVRO_CODEC_DISPLAY,
          avroRecommender
      );

    }
    return configDef;
  }

  public String getAvroCodec() {
    return getString(AVRO_CODEC_CONFIG);
  }

  @Override
  public Object get(String key) {
    return super.get(key);
  }

  public StorageSinkConnectorConfig(ConfigDef configDef, Map<String, String> props) {
    super(configDef, props);
  }

  public AvroDataConfig avroDataConfig() {
    Map<String, Object> props = new HashMap<>();
    props.put(SCHEMA_CACHE_SIZE_CONFIG, get(SCHEMA_CACHE_SIZE_CONFIG));
    props.put(ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, get(ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG));
    props.put(CONNECT_META_DATA_CONFIG, get(CONNECT_META_DATA_CONFIG));
    return new AvroDataConfig(props);
  }
}
