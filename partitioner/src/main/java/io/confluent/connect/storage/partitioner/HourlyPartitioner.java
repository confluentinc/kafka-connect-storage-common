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

import org.apache.kafka.common.config.ConfigException;
import org.joda.time.DateTimeZone;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HourlyPartitioner<T> extends TimeBasedPartitioner<T> {

  private static long partitionDurationMs = TimeUnit.HOURS.toMillis(1);
  private static String pathFormat = "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/";

  @Override
  public void configure(Map<String, Object> config) {
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

  public String getPathFormat() {
    return pathFormat;
  }
}
