package io.confluent.connect.storage.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;


import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class DateTimeUtilsTest {
    private static final DateTimeZone DATE_TIME_ZONE = DateTimeZone.forID("America/Los_Angeles");
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

    @Test
    public void testDaylightSavingTime() {
        DateTime time = new DateTime(2015, 11, 1, 2, 1, DATE_TIME_ZONE);
        String pathFormat = "'year='YYYY/'month='MMMM/'day='dd/'hour='H/";
        DateTimeFormatter formatter = DateTimeFormat.forPattern(pathFormat).withZone(DATE_TIME_ZONE);
        long utc1 = DATE_TIME_ZONE.convertLocalToUTC(time.getMillis() - TimeUnit.MINUTES.toMillis(60), false);
        long utc2 = DATE_TIME_ZONE.convertLocalToUTC(time.getMillis() - TimeUnit.MINUTES.toMillis(120), false);
        DateTime time1 = new DateTime(DATE_TIME_ZONE.convertUTCToLocal(utc1));
        DateTime time2 = new DateTime(DATE_TIME_ZONE.convertUTCToLocal(utc2));
        assertEquals(time1.toString(formatter), time2.toString(formatter));
    }
}