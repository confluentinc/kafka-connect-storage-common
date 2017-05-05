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

import io.confluent.connect.storage.common.SchemaGenerator;
import org.apache.kafka.common.config.ConfigException;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class PartitioningCommon {
  static String getPathFormat(Map<String, Object> config) {
    String pathFormat = (String) config.get(PartitionerConfig.PATH_FORMAT_CONFIG);
    if (pathFormat.equals("")) {
      throw new ConfigException(PartitionerConfig.PATH_FORMAT_CONFIG,
              pathFormat, "Path format cannot be empty.");
    }
    return pathFormat;
  }

  static DateTimeFormatter loadDateTimeFormatterFromConfiguration(Map<String, Object> config, String pathFormat) {
    String localeString = (String) config.get(PartitionerConfig.LOCALE_CONFIG);
    if (localeString.equals("")) {
      throw new ConfigException(PartitionerConfig.LOCALE_CONFIG,
              localeString, "Locale cannot be empty.");
    }
    Locale locale = new Locale(localeString);

    String timeZoneString = (String) config.get(PartitionerConfig.TIMEZONE_CONFIG);
    if (timeZoneString.equals("")) {
      throw new ConfigException(PartitionerConfig.TIMEZONE_CONFIG,
              timeZoneString, "Timezone cannot be empty.");
    }
    DateTimeZone timeZone = DateTimeZone.forID(timeZoneString);

    return DateTimeFormat.forPattern(pathFormat).withZone(timeZone).withLocale(locale);
  }

  static <T> List<T> loadDateTimePartitionFields(SchemaGenerator<T> schemaGenerator, String pathFormat) {
    try {
      return schemaGenerator.newPartitionFields(pathFormat);
    } catch (IllegalArgumentException e) {
      throw new ConfigException(PartitionerConfig.PATH_FORMAT_CONFIG, pathFormat, e.getMessage());
    }
  }

  static long roundInstantToMs(long timeGranularityMs, long timestamp, DateTimeZone timeZone) {
    if (timeGranularityMs < 1) {
      return timestamp;
    }

    long adjustedTimeStamp = timeZone.convertUTCToLocal(timestamp);
    long partitionedTime = (adjustedTimeStamp / timeGranularityMs) * timeGranularityMs;
    return timeZone.convertLocalToUTC(partitionedTime, false);
  }

  static Date roundInstantToMs(long timeGranularityMs, Date timestamp, DateTimeZone timeZone) {
    if (timeGranularityMs < 1) {
      return timestamp;
    }

    long adjustedTimeStamp = timeZone.convertUTCToLocal(timestamp.getTime());
    long partitionedTime = (adjustedTimeStamp / timeGranularityMs) * timeGranularityMs;
    return new Date(timeZone.convertLocalToUTC(partitionedTime, false));
  }
}
