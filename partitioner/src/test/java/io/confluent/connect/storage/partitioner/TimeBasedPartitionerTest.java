/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.connect.storage.partitioner;

import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.common.StorageCommonConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TimeBasedPartitionerTest extends StorageSinkTestBase {
  private static final String timeZoneString = "America/Los_Angeles";
  private static final DateTimeZone DATE_TIME_ZONE = DateTimeZone.forID(timeZoneString);
  private BiHourlyPartitioner partitioner = new BiHourlyPartitioner();

  @Test
  public void testGeneratePartitionedPath() throws Exception {
    Map<String, Object> config = createConfig(null);
    partitioner.configure(config);

    long timestamp = new DateTime(2015, 1, 1, 3, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
    SinkRecord sinkRecord = createSinkRecord(timestamp);
    String encodedPartition = partitioner.encodePartition(sinkRecord);
    final String topic = "topic";
    String path = partitioner.generatePartitionedPath(topic, encodedPartition);
    assertEquals(topic+"/year=2015/month=1/day=1/hour=3/", path);
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

  @Test
  public void testRecordFieldTimeExtractor() throws Exception {
    TimeBasedPartitioner<String> partitioner = new TimeBasedPartitioner<>();
    Map<String, Object> config = createConfig("timestamp");
    partitioner.configure(config);

    long timestamp = new DateTime(2015, 4, 2, 1, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
    SinkRecord sinkRecord = createSinkRecord(timestamp);

    String encodedPartition = partitioner.encodePartition(sinkRecord);

    assertEquals("year=2015/month=4/day=2/hour=1/", encodedPartition);
  }

  @Test
  public void testNestedRecordFieldTimeExtractor() throws Exception {
    TimeBasedPartitioner<String> partitioner = new TimeBasedPartitioner<>();
    Map<String, Object> config = createConfig("nested.timestamp");
    partitioner.configure(config);

    long timestamp = new DateTime(2015, 4, 2, 1, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
    SinkRecord sinkRecord = createSinkRecordWithNestedTimeField(timestamp);

    String encodedPartition = partitioner.encodePartition(sinkRecord);

    assertEquals("year=2015/month=4/day=2/hour=1/", encodedPartition);
  }

  @Test
  public void testRecordTimeExtractor() throws Exception {
    TimeBasedPartitioner<String> partitioner = new TimeBasedPartitioner<>();
    Map<String, Object> config = createConfig(null);
    partitioner.configure(config);

    long timestamp = new DateTime(2015, 4, 2, 1, 0, 0, 0, DateTimeZone.forID(timeZoneString)).getMillis();
    SinkRecord sinkRecord = createSinkRecord(timestamp);

    String encodedPartition = partitioner.encodePartition(sinkRecord);

    assertEquals("year=2015/month=4/day=2/hour=1/", encodedPartition);
  }

  private static class BiHourlyPartitioner extends TimeBasedPartitioner<String> {
    private static long partitionDurationMs = TimeUnit.HOURS.toMillis(2);

    @Override
    public String getPathFormat() {
        return "'year'=YYYY/'month'=MMMM/'day'=dd/'hour'=H/";
    }

    @Override
    public void configure(Map<String, Object> config) {
        init(partitionDurationMs, getPathFormat(), Locale.ENGLISH, DATE_TIME_ZONE, config);
        super.configure(config);
    }
  }

  private Map<String, Object> createConfig(String timeFieldName) {
    Map<String, Object> config = new HashMap<>();

    config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);
    config.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Record" +
            (timeFieldName == null ? "" : "Field"));
    config.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.HOURS.toMillis(1));
    config.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'year'=YYYY/'month'=M/'day'=d/'hour'=H/");
    config.put(PartitionerConfig.LOCALE_CONFIG, Locale.US.toString());
    config.put(PartitionerConfig.TIMEZONE_CONFIG, DATE_TIME_ZONE.toString());
    if (timeFieldName != null) {
        config.put(PartitionerConfig.TIMESTAMP_FIELD_NAME_CONFIG, timeFieldName);
    }
    return config;
  }

  private SinkRecord createSinkRecord(long timestamp) {
    Schema schema = createSchemaWithTimestampField();
    Struct record = createRecordWithTimestampField(schema, timestamp);
    return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, schema, record, 0L,
            timestamp, TimestampType.CREATE_TIME);
  }

  private SinkRecord createSinkRecordWithNestedTimeField(long timestamp) {
    Struct record = createRecordWithNestedTimeField(timestamp);
    return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, record.schema(), record, 0L,
            timestamp, TimestampType.CREATE_TIME);
}
}