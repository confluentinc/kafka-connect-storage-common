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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableInstant;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.util.DateTimeUtils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TimeBasedPartitionerTest extends StorageSinkTestBase {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static final String TIME_ZONE = "America/Los_Angeles";
  private static final DateTimeZone DATE_TIME_ZONE = DateTimeZone.forID(TIME_ZONE);
  private static final String PATH_FORMAT = "'year'=YYYY/'month'=M/'day'=d/'hour'=H/";

  private static final int YEAR = 2015;
  private static final int MONTH = DateTimeConstants.APRIL;
  private static final int DAY = 2;
  private static final int HOUR = 1;
  public static final DateTime DATE_TIME =
        new DateTime(YEAR, MONTH, DAY, HOUR, 0, DATE_TIME_ZONE);

  @Test
  public void testGetDurationMs() {
    BiHourlyPartitioner partitioner = (BiHourlyPartitioner) configurePartitioner(
          new BiHourlyPartitioner(), null, null);

    assertThat(partitioner.getPartitionDurationMs(), is(BiHourlyPartitioner.partitionDurationMs));
  }

  @Test
  public void testNonPostitiveDuration() {
    thrown.expect(ConfigException.class);
    thrown.expectMessage(startsWith("Invalid value -1 for configuration " +
          PartitionerConfig.PARTITION_DURATION_MS_CONFIG));

    Map<String, Object> config = new HashMap<>();
    config.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, -1L);
    configurePartitioner(new TimeBasedPartitioner<>(), null, config);
  }

  @Test
  public void testGetPathFormat() {
    BiHourlyPartitioner partitioner = (BiHourlyPartitioner) configurePartitioner(
          new BiHourlyPartitioner(), null, null);

    assertThat(partitioner.getPathFormat(), is(PATH_FORMAT));
  }

  @Test
  public void testGeneratePartitionedPath() throws Exception {
    BiHourlyPartitioner partitioner = (BiHourlyPartitioner) configurePartitioner(
          new BiHourlyPartitioner(), null, null);

    SinkRecord sinkRecord = getSinkRecord();
    String encodedPartition = partitioner.encodePartition(sinkRecord);
    final String topic = "topic";
    String path = partitioner.generatePartitionedPath(topic, encodedPartition);
    assertEquals(topic+"/year=2015/month=4/day=2/hour=0/", path);
  }

  @Test
  public void testInvalidPathFormat() {
    final String configKey = PartitionerConfig.PATH_FORMAT_CONFIG;
    // Single quotes should be around the year, month literals, not the format strings
    final String pathFormat = "year='YYYY'/month='MM'";

    thrown.expect(ConfigException.class);
    thrown.expectMessage(startsWith(String.format("Invalid value %s for configuration %s",
          pathFormat, configKey)));

    Map<String, Object> config = new HashMap<>();
    config.put(configKey, pathFormat);
    configurePartitioner(new TimeBasedPartitioner<>(),
          null, config);
  }

  @Test
  public void testNullPathFormat() {
    TimeBasedPartitioner<String> partitioner = new TimeBasedPartitioner<>();
    Map<String, Object> config = createConfig(null);

    final String configKey = PartitionerConfig.PATH_FORMAT_CONFIG;
    final String msg = "Path format cannot be empty.";
    String path = null;

    thrown.expect(ConfigException.class);
    thrown.expectMessage(is(String.format("Invalid value %s for configuration %s: " +
            msg, path, configKey)));

    config.put(configKey, path);
    partitioner.configure(config);
  }

  @Test
  public void testEmptyPathFormat() {
    TimeBasedPartitioner<String> partitioner = new TimeBasedPartitioner<>();
    Map<String, Object> config = createConfig(null);

    final String configKey = PartitionerConfig.PATH_FORMAT_CONFIG;
    final String msg = "Path format cannot be empty.";
    String path = "";

    thrown.expect(ConfigException.class);
    thrown.expectMessage(is(String.format("Invalid value %s for configuration %s: " +
            msg, path, configKey)));

    config.put(configKey, path);
    partitioner.configure(config);
  }

  @Test
  public void testRootPathFormat() {
    final String configKey = PartitionerConfig.PATH_FORMAT_CONFIG;
    final String msg = "Path format cannot be empty.";
    String path = StorageCommonConfig.DIRECTORY_DELIM_DEFAULT;
    Map<String, Object> config = createConfig(null);
    config.put(configKey, path);

    thrown.expect(ConfigException.class);
    thrown.expectMessage(is(String.format("Invalid value %s for configuration %s: " +
            msg, path, configKey)));

    configurePartitioner(new TimeBasedPartitioner<>(), null, config);
  }

  @Test
  public void testPathFormatEndDelim() {
    Map<String, Object> config = createConfig(null);
    config.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'year='YYYY/");
    TimeBasedPartitioner<String> partitioner = configurePartitioner(
          new TimeBasedPartitioner<>(), null, config);
    assertEquals("'year='YYYY", partitioner.getPathFormat());
  }

  @Test
  public void testNullLocale() {
    TimeBasedPartitioner<String> partitioner = new TimeBasedPartitioner<>();
    Map<String, Object> config = createConfig(null);

    final String configKey = PartitionerConfig.LOCALE_CONFIG;
    final String msg = "Locale cannot be empty.";
    String localeString = null;

    thrown.expect(ConfigException.class);
    thrown.expectMessage(is(String.format("Invalid value %s for configuration %s: " +
            msg, localeString, configKey)));

    config.put(configKey, localeString);
    partitioner.configure(config);
  }

  @Test
  public void testEmptyLocale() {
    TimeBasedPartitioner<String> partitioner = new TimeBasedPartitioner<>();
    Map<String, Object> config = createConfig(null);

    final String configKey = PartitionerConfig.LOCALE_CONFIG;
    final String msg = "Locale cannot be empty.";
    String localeString = "";

    thrown.expect(ConfigException.class);
    thrown.expectMessage(is(String.format("Invalid value %s for configuration %s: " +
            msg, localeString, configKey)));

    config.put(configKey, localeString);
    partitioner.configure(config);
  }

  @Test
  public void testNullTimezone() {
    TimeBasedPartitioner<String> partitioner = new TimeBasedPartitioner<>();
    Map<String, Object> config = createConfig(null);

    final String configKey = PartitionerConfig.TIMEZONE_CONFIG;
    final String msg = "Timezone cannot be empty.";
    String timeZoneString = null;

    thrown.expect(ConfigException.class);
    thrown.expectMessage(is(String.format("Invalid value %s for configuration %s: " +
            msg, timeZoneString, configKey)));

    config.put(configKey, timeZoneString);
    partitioner.configure(config);
  }

  @Test
  public void testEmptyTimezone() {
    TimeBasedPartitioner<String> partitioner = new TimeBasedPartitioner<>();
    Map<String, Object> config = createConfig(null);

    final String configKey = PartitionerConfig.TIMEZONE_CONFIG;
    final String msg = "Timezone cannot be empty.";
    String timeZoneString = "";

    thrown.expect(ConfigException.class);
    thrown.expectMessage(is(String.format("Invalid value %s for configuration %s: " +
            msg, timeZoneString, configKey)));

    config.put(configKey, timeZoneString);
    partitioner.configure(config);
  }

  @Test
  public void testInvalidTimestampExtractor() {
    String extractorClassName = "Future";
    extractorClassName = TimeBasedPartitioner.class.getName() + "$"
          + extractorClassName
          + "TimestampExtractor";

    thrown.expect(ConfigException.class);
    thrown.expectMessage(is("Invalid timestamp extractor: " + extractorClassName));

    Map<String, Object> config = new HashMap<>();
    config.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, extractorClassName);
    configurePartitioner(new TimeBasedPartitioner<String>(), null, config);
  }

  @Test
  public void testUnassignableTimestampExtractor() {
    String extractorClassName = DateTimeUtils.class.getName();

    thrown.expect(ConnectException.class);
    thrown.expectMessage(is(String.format(
          "Class %s does not implement TimestampExtractor", extractorClassName)));

    Map<String, Object> config = new HashMap<>();
    config.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, extractorClassName);
    configurePartitioner(new TimeBasedPartitioner<String>(), null, config);
  }

  @Test
  public void testRemovePathFormatEndDelim() {
    Map<String, Object> config = new HashMap<>();
    config.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'year='YYYY/");
    TimeBasedPartitioner<String> partitioner = configurePartitioner(
          new TimeBasedPartitioner<>(), null, config);

    assertThat(partitioner.getPathFormat(), is("'year='YYYY"));
  }

  @Test
  public void testRecordTimeExtractorNullSinkRecordTime() {
    thrown.expect(ConnectException.class);
    thrown.expectMessage(startsWith("Unable to determine timestamp using timestamp.extractor"));

    SinkRecord r = createValuedSinkRecord(Schema.STRING_SCHEMA, "foo", null);

    // Setting time field as null to use RecordTimestampExtractor
    getEncodedPartition(null, r);
  }

  @Test
  public void testRecordFieldNotStructOrMap() {
    thrown.expect(PartitionException.class);
    thrown.expectMessage(is("Error encoding partition."));

    Schema valueSchema = Schema.STRING_SCHEMA;
    SinkRecord r = createValuedSinkRecord(valueSchema, "foo", 0L);

    // Field name must be non-null, but value doesn't matter here, as valueSchema
    // isn't Struct or Map
    getEncodedPartition("bar", r);
  }

  @Test
  public void testNullSinkRecordTime() {
    thrown.expect(ConnectException.class);
    thrown.expectMessage(startsWith("Unable to determine timestamp using timestamp.extractor"));

    SinkRecord r = new SinkRecord(TOPIC, PARTITION, null, null,
          Schema.STRING_SCHEMA, "foo", 0);
    getEncodedPartition(null, r);
  }

  @Test
  public void testIntTimeExtract() {
    TimeBasedPartitioner<String> partitioner = configurePartitioner(
          new TimeBasedPartitioner<>(), "int", null);

    String path = getPartitionedPath(partitioner);

    long timeGranularityMs = (long) partitioner.config.get(PartitionerConfig.PARTITION_DURATION_MS_CONFIG);
    long partitionMs = TimeBasedPartitioner.getPartition(timeGranularityMs, 12L, DATE_TIME_ZONE);
    DateTime dt = new DateTime(partitionMs, DATE_TIME_ZONE);

    validatePathFromDateTime(path, dt, TOPIC);
  }

  @Test
  public void testLongTimeExtract() {
    TimeBasedPartitioner<String> partitioner = configurePartitioner(
          new TimeBasedPartitioner<>(), "long", null);

    String path = getPartitionedPath(partitioner);

    long timeGranularityMs = (long) partitioner.config.get(PartitionerConfig.PARTITION_DURATION_MS_CONFIG);
    long partitionMs = TimeBasedPartitioner.getPartition(timeGranularityMs, 12L, DATE_TIME_ZONE);
    DateTime dt = new DateTime(partitionMs, DATE_TIME_ZONE);

    validatePathFromDateTime(path, dt, TOPIC);
  }

  @Test
  public void testFloatTimeExtract() {
    String fieldName = "float";
    thrown.expect(PartitionException.class);
    thrown.expectMessage(is("Error extracting timestamp from record field: " + fieldName));

    getEncodedPartition(fieldName);
  }

  @Test
  public void testDoubleTimeExtract() {
    String fieldName = "double";
    thrown.expect(PartitionException.class);
    thrown.expectMessage(is("Error extracting timestamp from record field: " + fieldName));

    getEncodedPartition(fieldName);
  }

  @Test
  public void testStructRecordFieldArrayExtraction() {
    final String fieldName = "array";

    thrown.expect(PartitionException.class);
    thrown.expectMessage(is("Error extracting timestamp from record field: " + fieldName));

    final SchemaBuilder fieldSchema = SchemaBuilder.array(Schema.INT64_SCHEMA);
    final long millis = DATE_TIME.getMillis();
    final List<Long> value = Collections.singletonList(millis);

    Schema valueSchema = SchemaBuilder.struct().field(fieldName, fieldSchema);
    Struct struct = new Struct(valueSchema).put(fieldName, value);
    SinkRecord sinkRecord = createValuedSinkRecord(valueSchema, struct, millis);

    getEncodedPartition(fieldName, sinkRecord);
  }

  @Test
  public void testMapRecordFieldArrayExtraction() {
    final String fieldName = "array";

    thrown.expect(PartitionException.class);
    thrown.expectMessage(is("Error extracting timestamp from record field: " + fieldName));

    final SchemaBuilder fieldSchema = SchemaBuilder.array(Schema.INT64_SCHEMA);
    final long millis = DATE_TIME.getMillis();
    final List<Long> value = Collections.singletonList(millis);

    Map<String, Object> m = new HashMap<>();
    m.put(fieldName, value);
    Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, fieldSchema);
    SinkRecord sinkRecord = createValuedSinkRecord(mapSchema, m, millis);

    getEncodedPartition(fieldName, sinkRecord);
  }

  @Test
  public void testInvalidStringTimeStructExtract() {
    String fieldName = "string";

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(startsWith("Invalid format"));

    SinkRecord sinkRecord = createSinkRecord(DATE_TIME.getMillis());
    Struct struct = (Struct) sinkRecord.value();
    assertThat(String.valueOf(struct.get(fieldName)), is("def"));

    getEncodedPartition(fieldName, sinkRecord);
  }

  @Test
  public void testInvalidStringTimeMapExtract() {
    String fieldName = "string";

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(startsWith("Invalid format"));

    Schema keySchema = Schema.STRING_SCHEMA;
    Schema valueSchema = createNewSchema();
    Schema mapSchema = SchemaBuilder.map(keySchema, valueSchema);
    Map<String, Object> m = new HashMap<>();
    m.put("header", createRecord(valueSchema));
    SinkRecord sinkRecord = createValuedSinkRecord(mapSchema, m, DATE_TIME.getMillis());
    Struct nestedValue = (Struct) ((Map) sinkRecord.value()).get("header");
    assertThat(nestedValue.get(fieldName), is("abc"));

    getEncodedPartition(String.format("header.%s", fieldName), sinkRecord);
  }

  @Test
  public void testStringTimeExtract() {
    DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
    String timeStr = fmt.print(DATE_TIME);

    String timeFieldName = "timestamp";
    Schema schema = SchemaBuilder.struct().name("record")
          .field(timeFieldName, Schema.STRING_SCHEMA);
    Struct s = new Struct(schema).put(timeFieldName, timeStr);
    SinkRecord record = new SinkRecord(TOPIC, PARTITION, null, null, schema, s,
          0, DATE_TIME.getMillis(), TimestampType.LOG_APPEND_TIME);

    String encodedPartition = getEncodedPartition(timeFieldName, record);
    validateEncodedPartition(encodedPartition);
  }

  @Test
  public void testNumericRecordFieldTimeMap() {
    String timeField = "timestamp";
    TimeBasedPartitioner<String> partitioner = configurePartitioner(
          new TimeBasedPartitioner<>(), timeField, null);

    assertThat(partitioner.getTimestampExtractor(), instanceOf(
          TimeBasedPartitioner.RecordFieldTimestampExtractor.class));

    Number ts = Integer.MAX_VALUE;
    testMapNumericTimestampPartitionEncoding(
          partitioner, timeField, ts, Schema.INT32_SCHEMA, new DateTime(ts.intValue()));

    ts = DATE_TIME.getMillis();
    testMapNumericTimestampPartitionEncoding(
          partitioner, timeField, ts, Schema.INT64_SCHEMA, DATE_TIME);
  }

  @Test
  public void testRecordFieldTimeDateExtractor() {
    String timeField = "timestamp";
    TimeBasedPartitioner<String> partitioner = configurePartitioner(
          new TimeBasedPartitioner<>(), timeField, null);

    assertThat(partitioner.getTimestampExtractor(),
          instanceOf(TimeBasedPartitioner.RecordFieldTimestampExtractor.class));

    DateTime moment = new DateTime(2015, 4, 2, 1, 0, 0, 0, DateTimeZone.forID(TIME_ZONE));
    String expectedPartition = "year=2015/month=4/day=2/hour=1";

    long rawTimestamp = moment.getMillis();
    SinkRecord sinkRecord = createSinkRecord(rawTimestamp);
    String encodedPartition = partitioner.encodePartition(sinkRecord);
    assertEquals(expectedPartition, encodedPartition);

    String timestamp = ISODateTimeFormat.dateTimeNoMillis().print(moment);
    sinkRecord = createSinkRecord(Schema.STRING_SCHEMA, timestamp);
    encodedPartition = partitioner.encodePartition(sinkRecord);
    assertEquals(expectedPartition, encodedPartition);

    timestamp = ISODateTimeFormat.dateTime().print(moment);
    sinkRecord = createSinkRecord(Schema.STRING_SCHEMA, timestamp);
    encodedPartition = partitioner.encodePartition(sinkRecord);
    assertEquals(expectedPartition, encodedPartition);

    sinkRecord = createSinkRecord(Timestamp.SCHEMA, moment.toDate());
    encodedPartition = partitioner.encodePartition(sinkRecord);
    assertEquals(expectedPartition, encodedPartition);

    int shortTimestamp = (int) new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeZone.forID(TIME_ZONE)).getMillis();
    sinkRecord = createSinkRecord(Schema.INT32_SCHEMA, shortTimestamp);
    encodedPartition = partitioner.encodePartition(sinkRecord);
    assertEquals("year=1970/month=1/day=1/hour=0", encodedPartition);

    // Struct - Date extraction
    sinkRecord = createSinkRecord(rawTimestamp);
    String structEncodedPartition = partitioner.encodePartition(sinkRecord);
    validateEncodedPartition(structEncodedPartition);

    // Map - Date Extraction
    String mapEncodedPartition = testMapNumericTimestampPartitionEncoding(
          partitioner, timeField, DATE_TIME.toDate(), Timestamp.SCHEMA, DATE_TIME);

    assertThat(structEncodedPartition, is(mapEncodedPartition));
  }

  @Test
  public void testNestedRecordFieldTimeExtractor() throws Exception {
    TimeBasedPartitioner<String> partitioner = configurePartitioner(
          new TimeBasedPartitioner<>(), "nested.timestamp", null);

    assertThat(partitioner.getTimestampExtractor(),
          instanceOf(TimeBasedPartitioner.RecordFieldTimestampExtractor.class));

    long timestamp = DATE_TIME.getMillis();
    SinkRecord sinkRecord = createSinkRecordWithNestedTimestampField(timestamp);

    String encodedPartition = partitioner.encodePartition(sinkRecord);

    validateEncodedPartition(encodedPartition);
  }

  @Test
  public void testRecordFieldTimeStringExtractor() {
    DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
    long millis = DATE_TIME.getMillis();
    String timeStr = fmt.print(millis);

    String timeFieldName = "timestamp";
    TimeBasedPartitioner<String> partitioner = configurePartitioner(
          new TimeBasedPartitioner<>(), timeFieldName, null);
    assertThat(partitioner.getTimestampExtractor(), instanceOf(
          TimeBasedPartitioner.RecordFieldTimestampExtractor.class));

    // Struct with time as formatted string
    Schema schema = SchemaBuilder.struct().name("record")
          .field(timeFieldName, Schema.STRING_SCHEMA);
    Struct s = new Struct(schema).put(timeFieldName, timeStr);
    SinkRecord sinkRecord = createValuedSinkRecord(schema, s, millis);
    String encodedPartition = partitioner.encodePartition(sinkRecord);
    validateEncodedPartition(encodedPartition);

    // Create nested time field using map{string->struct}
    Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, schema);
    Map<String, Struct> header = new HashMap<>();
    header.put("header", s);
    sinkRecord = createValuedSinkRecord(mapSchema, header, millis);

    encodedPartition = getEncodedPartition(String.format("header.%s", timeFieldName), sinkRecord);
    validateEncodedPartition(encodedPartition);
  }

  @Test
  public void testRecordTimeExtractor() throws Exception {
    TimeBasedPartitioner<String> partitioner = configurePartitioner(
          new TimeBasedPartitioner<>(), null, null);

    assertThat(partitioner.getTimestampExtractor(),
          instanceOf(TimeBasedPartitioner.RecordTimestampExtractor.class));

    SinkRecord sinkRecord = getSinkRecord();
    assertThat(sinkRecord.timestamp(), is(DATE_TIME.getMillis()));
    String encodedPartition = partitioner.encodePartition(sinkRecord);
    validateEncodedPartition(encodedPartition);

    encodedPartition = partitioner.encodePartition(sinkRecord, 123L);
    validateEncodedPartition(encodedPartition);
  }

  @Test
  public void testWallclockTimeExtractor() {
    long now = 15778800000L;
    TimeBasedPartitioner<String> partitioner = configurePartitioner(
        new TimeBasedPartitioner<>(), null, Collections.singletonMap(
            PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Wallclock")
    );

    assertThat(partitioner.getTimestampExtractor(),
        instanceOf(TimeBasedPartitioner.WallclockTimestampExtractor.class));

    SinkRecord sinkRecord = getSinkRecord();

    String encodedPartition = partitioner.encodePartition(sinkRecord, now);
    validatePathFromDateTime(encodedPartition, new DateTime(now, DATE_TIME_ZONE));
  }

  /**
   * Get a SinkRecord with a value of a configurable <code>Map&lt;String, value&gt;</code>
   * keyed by {@code timeField} having a value of a {@code timestamp} Object that represents
   * a numeric timestamp.
   *
   * @param partitioner An instance of {@link TimeBasedPartitioner}
   * @param timeField The name of the timestamp field in the {@link SinkRecord#value} map
   * @param timestamp A {@link Number} or {@link Date} instance that can represent a numeric
   *                  timestamp
   * @param valueSchema The schema that will be wrapped as <code>Map&lt;String, valueSchema&gt;</code>
   *                    for {@link SinkRecord#valueSchema}
   * @param dateToValidate A {@link DateTime} object to validate the encoded partition by using
   * {@link TimeBasedPartitionerTest#validatePathFromDateTime(String, ReadableInstant)}
   * @param <T> The {@code partitioner} field type
   * @return The value of {@link TimeBasedPartitioner#encodePartition(SinkRecord)} from the record
   * in the <code>Map&lt;String, value&gt;</code>
   */
  private <T> String testMapNumericTimestampPartitionEncoding(
        TimeBasedPartitioner<T> partitioner,
        String timeField,
        Object timestamp,
        Schema valueSchema,
        DateTime dateToValidate) {
    Schema keySchema = Schema.STRING_SCHEMA;
    Schema mapSchema = SchemaBuilder.map(keySchema, valueSchema);
    Map<String, Object> map = new HashMap<>();
    map.put(timeField, timestamp);

    Long timestampValue = null;
    if (timestamp instanceof Number) {
      timestampValue = ((Number) timestamp).longValue();
    } else if (timestamp instanceof Date) {
      timestampValue = ((Date) timestamp).getTime();
    }
    assertThat("Number or Date timestamp received",
          timestampValue, is(notNullValue()));
    SinkRecord sinkRecord = createValuedSinkRecord(mapSchema, map, timestampValue);

    String encodedPartition = partitioner.encodePartition(sinkRecord);
    validatePathFromDateTime(encodedPartition, dateToValidate);
    return encodedPartition;
  }

  private static class BiHourlyPartitioner extends TimeBasedPartitioner<String> {
    private static long partitionDurationMs = TimeUnit.HOURS.toMillis(2);

    @Override
    public String getPathFormat() {
        return PATH_FORMAT;
    }

    @Override
    public void configure(Map<String, Object> config) {
      super.configure(config);
      init(partitionDurationMs, getPathFormat(), Locale.ENGLISH, DATE_TIME_ZONE, config);
    }
  }

  private Map<String, Object> createConfig(String timeFieldName) {
    Map<String, Object> config = new HashMap<>();

    config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);
    config.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Record" +
          (timeFieldName == null ? "" : "Field"));
    config.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.HOURS.toMillis(1));
    config.put(PartitionerConfig.PATH_FORMAT_CONFIG, PATH_FORMAT);
    config.put(PartitionerConfig.LOCALE_CONFIG, Locale.US.toString());
    config.put(PartitionerConfig.TIMEZONE_CONFIG, DATE_TIME_ZONE.toString());
    if (timeFieldName != null) {
      config.put(PartitionerConfig.TIMESTAMP_FIELD_NAME_CONFIG, timeFieldName);
    }
    return config;
  }

  /**
   * Configure a {@link TimeBasedPartitioner} with an optional timestamp fieldname and additional overrides
   * on top of {@link TimeBasedPartitionerTest#createConfig(String)}
   *
   * @param partitioner An instance of a {@link TimeBasedPartitioner}
   * @param timeField The field name to use for a timestamp, or null. Nested fields should be
   *                  dot-separated. <br/>
   *                  Null values will set the extractor to be {@link io.confluent.connect.storage.partitioner.TimeBasedPartitioner.RecordTimestampExtractor}.<br/>
   *                  Non-Null values will set the extractor to be {@link io.confluent.connect.storage.partitioner.TimeBasedPartitioner.RecordFieldTimestampExtractor}.
   * @param configOverride Optional overrides to apply to {@link TimeBasedPartitioner#configure(Map)}
   * @param <T> Partition field type
   * @return The {@code partitioner} instance configured with the default and override properties
   */
  private <T> TimeBasedPartitioner<T> configurePartitioner(
        TimeBasedPartitioner<T> partitioner,
        String timeField,
        Map<String, Object> configOverride) {
    if (partitioner == null) {
      partitioner = new TimeBasedPartitioner<>();
    }
    Map<String, Object> config = createConfig(timeField);
    if (configOverride != null) {
      for (Map.Entry<String, Object> e : configOverride.entrySet()) {
        config.put(e.getKey(), e.getValue());
      }
    }
    partitioner.configure(config);
    return partitioner;
  }

  private SinkRecord getSinkRecord() {
    long timestamp = new DateTime(2015, 4, 2, 1, 0,
          0, 0, DateTimeZone.forID(TIME_ZONE)).getMillis();
    return createSinkRecord(timestamp);
  }

  private SinkRecord createSinkRecord(Schema timestampSchema, Object timestamp) {
    Schema schema = createSchemaWithTimestampField(timestampSchema);
    Struct record = createRecordWithTimestampField(schema, timestamp);
    return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, schema, record, 0L,
        timestamp instanceof Long ? (Long) timestamp : Time.SYSTEM.milliseconds(), TimestampType.CREATE_TIME);
  }

  private SinkRecord createSchemalessSinkRecord(Object timestamp) {
    Map<String, Object> record = new HashMap<>();
    record.put("timestamp", timestamp);
    return new SinkRecord(TOPIC, PARTITION, null, null, null, record, 0L,
        timestamp instanceof Long ? (Long) timestamp : Time.SYSTEM.milliseconds(), TimestampType.CREATE_TIME);
  }

  protected SinkRecord createSinkRecordWithNestedTimestampField(long timestamp) {
    Struct record = createRecordWithNestedTimestampField(timestamp);
    return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, record.schema(), record, 0L,
          timestamp, TimestampType.CREATE_TIME);
  }

  private String getPartitionedPath(TimeBasedPartitioner<String> partitioner) {
    SinkRecord sinkRecord = getSinkRecord();
    String encodedPartition = partitioner.encodePartition(sinkRecord);
    return partitioner.generatePartitionedPath(TOPIC, encodedPartition);
  }

  /**
   * @param valueSchema The {@link Schema} value of the record
   * @param value Object that matches schema {@code valueSchema}
   * @param timestamp Timestamp for the record
   * @return {@link SinkRecord} with a null key
   */
  private SinkRecord createValuedSinkRecord(Schema valueSchema, Object value, Long timestamp) {
    return new SinkRecord(TOPIC, PARTITION,
          null, null, valueSchema, value,
          0, timestamp,
          timestamp == null ? TimestampType.NO_TIMESTAMP_TYPE : TimestampType.LOG_APPEND_TIME);
  }

  /**
   * Gets the encoded partition of a {@code SinkRecord} using either {@code Record} or
   * {@code RecordTime} extractors.
   *
   * @param timeFieldName The field name to use for a timestamp, or null. Nested fields should be
   *                      dot-separated. <br/>
   *                      Null values will set the extractor to be RecordTimestampExtractor.<br/>
   *                      Non-Null values will set the extractor to be RecordFieldTimestampExtractor.
   *
   * @param r A SinkRecord to partition on
   * @return The result of {@link TimeBasedPartitioner#encodePartition(SinkRecord)} for
   * record {@code r} using configurations in {@link TimeBasedPartitionerTest#createConfig(String)}
   */
  private String getEncodedPartition(String timeFieldName, SinkRecord r) {
    TimeBasedPartitioner<String> partitioner = configurePartitioner(
          new TimeBasedPartitioner<>(), timeFieldName, null);
    return partitioner.encodePartition(r);
  }

  private String getEncodedPartition(String timeFieldName) {
    return getEncodedPartition(timeFieldName, getSinkRecord());
  }

  /**
   * Assert path is equal to formatted {@link TimeBasedPartitionerTest#PATH_FORMAT}
   * using {@link TimeBasedPartitionerTest#DATE_TIME}
   *
   * @param encodedPartition Encoded Partition path, without topic name
   */
  private void validateEncodedPartition(String encodedPartition) {
    validatePathFromDateTime(encodedPartition, DATE_TIME, null);
  }

  /**
   * Assert path is equal to {@link TimeBasedPartitionerTest#PATH_FORMAT}
   * using datetime object @{code i}
   *
   * @param path Encoded Partition path, without topic name
   * @param i DateTime Instant
   */
  private void validatePathFromDateTime(String path, ReadableInstant i) {
    validatePathFromDateTime(path, i, null);
  }

  private void validatePathFromDateTime(String path, ReadableInstant i, String topic) {
    int yearLength = 4;
    int monthLength = 1;
    int dayLength = 1;
    int hourLength = 1;
    String expectedPath = new DateTimeFormatterBuilder()
          .appendLiteral((topic == null ? "" : TOPIC + "/" ) + "year=")
          .appendYear(yearLength, yearLength)
          .appendLiteral("/month=")
          .appendMonthOfYear(monthLength)
          .appendLiteral("/day=")
          .appendDayOfMonth(dayLength)
          .appendLiteral("/hour=")
          .appendHourOfDay(hourLength)
          .toFormatter()
          .withLocale(Locale.US)
          .withZone(DATE_TIME_ZONE)
          .print(i);
    assertThat(path, is(expectedPath));
  }
}
