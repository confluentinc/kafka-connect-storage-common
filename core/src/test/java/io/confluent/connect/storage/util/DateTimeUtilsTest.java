package io.confluent.connect.storage.util;

import org.joda.time.DateTime;
import org.junit.Test;


import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class DateTimeUtilsTest {
    private final DateTime midnight = DateTime.now().withTimeAtStartOfDay();

    private DateTime calc(DateTime current, long periodMs) {
        return new DateTime(DateTimeUtils.getNextTimeAdjustedByDay(
                current.getMillis(),
                periodMs,
                current.getZone())
        );
    }

    private DateTime calcHourPeriod(DateTime current) {
        return calc(current, TimeUnit.HOURS.toMillis(1));
    }

    @Test
    public void testGetNextTimeAdjustedByDayWOTimeZone() {
        assertEquals(calcHourPeriod(midnight), midnight.plusHours(1));
        assertEquals(calcHourPeriod(midnight.minusSeconds(1)), midnight);
        assertEquals(calcHourPeriod(midnight.plusSeconds(1)), midnight.plusHours(1));
        assertEquals(calcHourPeriod(midnight.plusHours(1)), midnight.plusHours(2));
        assertEquals(calcHourPeriod(midnight.plusHours(1).minusSeconds(1)), midnight.plusHours(1));
    }

    @Test
    public void testGetNextTimeAdjustedByDayPeriodDoesNotFitIntoDay() {
        DateTime midnight = DateTime.now().withTimeAtStartOfDay();
        long sevenHoursMs = TimeUnit.HOURS.toMillis(7);
        assertEquals(calc(midnight, sevenHoursMs), midnight.plusHours(7));
        assertEquals(calc(midnight.plusSeconds(1), sevenHoursMs), midnight.plusHours(7));
        assertEquals(calc(midnight.plusSeconds(1), sevenHoursMs), midnight.plusHours(7));
        assertEquals(calc(midnight.minusSeconds(1), sevenHoursMs), midnight);
        assertEquals(calc(midnight.minusHours(7).minusSeconds(1), sevenHoursMs), midnight.minusDays(1).plusHours(21));
    }
}