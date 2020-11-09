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

package io.confluent.connect.storage.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

public class DateTimeUtils {

  /**
   * Calculates next period of periodMs after currentTimeMs
   * starting from midnight in given timeZone.
   * If the next period is in next day then 12am of next day
   * will be returned
   *
   * @param currentTimeMs time to calculate at
   * @param periodMs period in ms
   * @param timeZone timezone to get midnight time
   * @return timestamp in ms
   */
  public static long getNextTimeAdjustedByDay(
      long currentTimeMs,
      long periodMs,
      DateTimeZone timeZone
  ) {
    DateTime currentDT = new DateTime(currentTimeMs).withZone(timeZone);
    DateTime startOfDayDT = currentDT.withTimeAtStartOfDay();
    DateTime startOfNextDayDT = startOfDayDT.plusDays(1);
    Duration currentDayDuration = new Duration(startOfDayDT, startOfNextDayDT);
    long todayInMs = currentDayDuration.getMillis();

    long startOfDay = startOfDayDT.getMillis();
    long nextPeriodOffset = ((currentTimeMs - startOfDay) / periodMs + 1) * periodMs;
    long offset = Math.min(nextPeriodOffset, todayInMs);
    return startOfDay + offset;
  }
}
