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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.lang.reflect.InvocationTargetException;
import java.util.Locale;
import java.util.Map;

import io.confluent.connect.storage.common.SchemaGenerator;
import io.confluent.connect.storage.common.StorageCommonConfig;

public class TimeBasedPartitioner<T> extends DefaultPartitioner<T> {
  // Duration of a partition in milliseconds.
  private long partitionDurationMs;
  private DateTimeFormatter formatter;

  protected void init(long partitionDurationMs, String pathFormat, Locale locale, DateTimeZone timeZone,
                      Map<String, Object> config) {
    delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    this.partitionDurationMs = partitionDurationMs;
    this.formatter = getDateTimeFormatter(pathFormat, timeZone).withLocale(locale);
    try {
      partitionFields = newSchemaGenerator(config).newPartitionFields(pathFormat);
    } catch (IllegalArgumentException e) {
      throw new ConfigException(PartitionerConfig.PATH_FORMAT_CONFIG, pathFormat, e.getMessage());
    }
  }

  private static DateTimeFormatter getDateTimeFormatter(String str, DateTimeZone timeZone) {
    return DateTimeFormat.forPattern(str).withZone(timeZone);
  }

  public static long getPartition(long timeGranularityMs, long timestamp, DateTimeZone timeZone) {
    long adjustedTimeStamp = timeZone.convertUTCToLocal(timestamp);
    long partitionedTime = (adjustedTimeStamp / timeGranularityMs) * timeGranularityMs;
    return timeZone.convertLocalToUTC(partitionedTime, false);
  }

  @Override
  public void configure(Map<String, Object> config) {
    long partitionDurationMs = (long) config.get(PartitionerConfig.PARTITION_DURATION_MS_CONFIG);
    if (partitionDurationMs < 0) {
      throw new ConfigException(PartitionerConfig.PARTITION_DURATION_MS_CONFIG,
                                partitionDurationMs, "Partition duration needs to be a positive.");
    }

    String pathFormat = (String) config.get(PartitionerConfig.PATH_FORMAT_CONFIG);
    if (pathFormat.equals("")) {
      throw new ConfigException(PartitionerConfig.PATH_FORMAT_CONFIG,
                                pathFormat, "Path format cannot be empty.");
    }

    String localeString = (String) config.get(PartitionerConfig.LOCALE_CONFIG);
    if (localeString.equals("")) {
      throw new ConfigException(PartitionerConfig.LOCALE_CONFIG,
                                localeString, "Locale cannot be empty.");
    }
    String timeZoneString = (String) config.get(PartitionerConfig.TIMEZONE_CONFIG);
    if (timeZoneString.equals("")) {
      throw new ConfigException(PartitionerConfig.TIMEZONE_CONFIG,
                                timeZoneString, "Timezone cannot be empty.");
    }


    Locale locale = new Locale(localeString);
    DateTimeZone timeZone = DateTimeZone.forID(timeZoneString);
    init(partitionDurationMs, pathFormat, locale, timeZone, config);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    long timestamp = System.currentTimeMillis();
    DateTime bucket = new DateTime(getPartition(partitionDurationMs, timestamp, formatter.getZone()));
    return bucket.toString(formatter);
  }

  @Override
  @SuppressWarnings("unchecked")
  public SchemaGenerator<T> newSchemaGenerator(Map<String, Object> config) {
    Class<? extends SchemaGenerator<T>> generatorClass = null;
    try {
      generatorClass =
          (Class<? extends SchemaGenerator<T>>) config.get(PartitionerConfig.SCHEMA_GENERATOR_CLASS_CONFIG);
      return generatorClass.getConstructor(Map.class).newInstance(config);
    } catch (ClassCastException | IllegalAccessException | InstantiationException | InvocationTargetException
        | NoSuchMethodException e) {
      throw new ConfigException("Invalid generator class: " + generatorClass);
    }
  }
}
