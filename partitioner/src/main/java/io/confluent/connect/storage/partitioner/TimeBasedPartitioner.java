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

import io.confluent.connect.storage.util.DataUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import io.confluent.connect.storage.common.SchemaGenerator;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.common.util.StringUtils;
import io.confluent.connect.storage.errors.PartitionException;

public class TimeBasedPartitioner<T> extends DefaultPartitioner<T> {
  private static final Logger log = LoggerFactory.getLogger(TimeBasedPartitioner.class);

  private static final String SCHEMA_GENERATOR_CLASS =
      "io.confluent.connect.storage.hive.schema.TimeBasedSchemaGenerator";

  // Duration of a partition in milliseconds.
  private long partitionDurationMs;
  private String pathFormat;
  private DateTimeFormatter formatter;
  protected TimestampExtractor timestampExtractor;

  protected void init(
      long partitionDurationMs,
      String pathFormat,
      Locale locale,
      DateTimeZone timeZone,
      Map<String, Object> config
  ) {
    delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    this.partitionDurationMs = partitionDurationMs;
    this.pathFormat = pathFormat;
    try {
      this.formatter = getDateTimeFormatter(pathFormat, timeZone).withLocale(locale);
      timestampExtractor = newTimestampExtractor(
          (String) config.get(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG));
      timestampExtractor.configure(config);
    } catch (IllegalArgumentException e) {
      ConfigException ce = new ConfigException(
          PartitionerConfig.PATH_FORMAT_CONFIG,
          pathFormat,
          e.getMessage()
      );
      ce.initCause(e);
      throw ce;
    }
  }

  private static DateTimeFormatter getDateTimeFormatter(String str, DateTimeZone timeZone) {
    return DateTimeFormat.forPattern(str).withZone(timeZone);
  }

  public static long getPartition(long timeGranularityMs, long timestamp, DateTimeZone timeZone) {
    long adjustedTimestamp = timeZone.convertUTCToLocal(timestamp);
    long partitionedTime = (adjustedTimestamp / timeGranularityMs) * timeGranularityMs;
    return timeZone.convertLocalToUTC(partitionedTime, false);
  }

  public long getPartitionDurationMs() {
    return partitionDurationMs;
  }

  public String getPathFormat() {
    return pathFormat;
  }

  // public for testing
  public TimestampExtractor getTimestampExtractor() {
    return timestampExtractor;
  }

  @Override
  public void configure(Map<String, Object> config) {
    super.configure(config);
    long partitionDurationMsProp =
        (long) config.get(PartitionerConfig.PARTITION_DURATION_MS_CONFIG);
    if (partitionDurationMsProp < 0) {
      throw new ConfigException(
          PartitionerConfig.PARTITION_DURATION_MS_CONFIG,
          partitionDurationMsProp,
          "Partition duration needs to be a positive."
      );
    }

    String pathFormat = (String) config.get(PartitionerConfig.PATH_FORMAT_CONFIG);
    if (StringUtils.isBlank(pathFormat) || pathFormat.equals(delim)) {
      throw new ConfigException(
          PartitionerConfig.PATH_FORMAT_CONFIG,
          pathFormat,
          "Path format cannot be empty."
      );
    } else if (!StringUtils.isBlank(delim) && pathFormat.endsWith(delim)) {
      // Delimiter has been added by the user at the end of the path format string. Removing.
      pathFormat = pathFormat.substring(0, pathFormat.length() - delim.length());
    }

    String localeString = (String) config.get(PartitionerConfig.LOCALE_CONFIG);
    if (StringUtils.isBlank(localeString)) {
      throw new ConfigException(
          PartitionerConfig.LOCALE_CONFIG,
          localeString,
          "Locale cannot be empty."
      );
    }

    String timeZoneString = (String) config.get(PartitionerConfig.TIMEZONE_CONFIG);
    if (StringUtils.isBlank(timeZoneString)) {
      throw new ConfigException(
          PartitionerConfig.TIMEZONE_CONFIG,
          timeZoneString,
          "Timezone cannot be empty."
      );
    }

    Locale locale = new Locale(localeString);
    DateTimeZone timeZone = DateTimeZone.forID(timeZoneString);
    init(partitionDurationMsProp, pathFormat, locale, timeZone, config);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord, long nowInMillis) {
    Long timestamp = timestampExtractor.extract(sinkRecord, nowInMillis);
    return encodedPartitionForTimestamp(sinkRecord, timestamp);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    Long timestamp = timestampExtractor.extract(sinkRecord);
    return encodedPartitionForTimestamp(sinkRecord, timestamp);
  }

  private String encodedPartitionForTimestamp(SinkRecord sinkRecord, Long timestamp) {
    if (timestamp == null) {
      String msg = "Unable to determine timestamp using timestamp.extractor "
          + timestampExtractor.getClass().getName()
          + " for record: "
          + sinkRecord;
      log.error(msg);
      throw new ConnectException(msg);
    }
    DateTime bucket = new DateTime(
        getPartition(partitionDurationMs, timestamp, formatter.getZone())
    );
    return bucket.toString(formatter);
  }

  @Override
  public List<T> partitionFields() {
    if (partitionFields == null) {
      partitionFields = newSchemaGenerator(config).newPartitionFields(pathFormat);
    }
    return partitionFields;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Class<? extends SchemaGenerator<T>> getSchemaGeneratorClass()
      throws ClassNotFoundException {
    return (Class<? extends SchemaGenerator<T>>) Class.forName(SCHEMA_GENERATOR_CLASS);
  }

  public TimestampExtractor newTimestampExtractor(String extractorClassName) {
    try {
      switch (extractorClassName) {
        case "Wallclock":
        case "Record":
        case "RecordField":
          extractorClassName = "io.confluent.connect.storage.partitioner.TimeBasedPartitioner$"
              + extractorClassName
              + "TimestampExtractor";
          break;
        default:
      }
      Class<?> klass = Class.forName(extractorClassName);
      if (!TimestampExtractor.class.isAssignableFrom(klass)) {
        throw new ConnectException(
            "Class " + extractorClassName + " does not implement TimestampExtractor"
        );
      }
      return (TimestampExtractor) klass.newInstance();
    } catch (ClassNotFoundException
        | ClassCastException
        | IllegalAccessException
        | InstantiationException e) {
      ConfigException ce = new ConfigException(
          "Invalid timestamp extractor: " + extractorClassName
      );
      ce.initCause(e);
      throw ce;
    }
  }

  public static class WallclockTimestampExtractor implements TimestampExtractor {
    @Override
    public void configure(Map<String, Object> config) {}

    /**
     * Returns the current timestamp supplied by the caller, which is assumed to be the processing
     * time.
     *
     * @param record Record from which to extract time
     * @param nowInMillis Time in ms specified by caller, useful for getting consistent wallclocks
     * @return The wallclock specified by the input parameter in milliseconds
     */
    @Override
    public Long extract(ConnectRecord<?> record, long nowInMillis) {
      return nowInMillis;
    }

    /**
     * Returns the current time from {@link Time#SYSTEM}.
     *
     * @param record Record to extract time from
     * @return Wallclock time in milliseconds
     */
    @Override
    public Long extract(ConnectRecord<?> record) {
      return Time.SYSTEM.milliseconds();
    }
  }

  public static class RecordTimestampExtractor implements TimestampExtractor {
    @Override
    public void configure(Map<String, Object> config) {}

    @Override
    public Long extract(ConnectRecord<?> record) {
      return record.timestamp();
    }
  }

  public static class RecordFieldTimestampExtractor implements TimestampExtractor {
    private String fieldName;
    private DateTimeFormatter dateTime;

    @Override
    public void configure(Map<String, Object> config) {
      fieldName = (String) config.get(PartitionerConfig.TIMESTAMP_FIELD_NAME_CONFIG);
      dateTime = ISODateTimeFormat.dateTimeParser();
    }

    @Override
    public Long extract(ConnectRecord<?> record) {
      Object value = record.value();
      if (value instanceof Struct) {
        Struct struct = (Struct) value;
        Object timestampValue = DataUtils.getNestedFieldValue(struct, fieldName);
        Schema fieldSchema = DataUtils.getNestedField(record.valueSchema(), fieldName).schema();

        if (Timestamp.LOGICAL_NAME.equals(fieldSchema.name())) {
          return ((Date) timestampValue).getTime();
        }

        switch (fieldSchema.type()) {
          case INT32:
          case INT64:
            return ((Number) timestampValue).longValue();
          case STRING:
            return dateTime.parseMillis((String) timestampValue);
          default:
            log.error(
                "Unsupported type '{}' for user-defined timestamp field.",
                fieldSchema.type().getName()
            );
            throw new PartitionException(
                "Error extracting timestamp from record field: " + fieldName
            );
        }
      } else if (value instanceof Map) {
        Map<?, ?> map = (Map<?, ?>) value;
        Object timestampValue = DataUtils.getNestedFieldValue(map, fieldName);
        if (timestampValue instanceof Number) {
          return ((Number) timestampValue).longValue();
        } else if (timestampValue instanceof String) {
          return dateTime.parseMillis((String) timestampValue);
        } else if (timestampValue instanceof Date) {
          return ((Date) timestampValue).getTime();
        } else {
          log.error(
              "Unsupported type '{}' for user-defined timestamp field.",
              timestampValue.getClass()
          );
          throw new PartitionException(
              "Error extracting timestamp from record field: " + fieldName
          );
        }
      } else {
        log.error("Value is not of Struct or Map type.");
        throw new PartitionException("Error encoding partition.");
      }
    }
  }
}
