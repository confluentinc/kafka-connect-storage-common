/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.storage.backup;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BackupModeValidatorTest {

  private static final String AVRO_CONVERTER = "io.confluent.connect.avro.AvroConverter";
  private static final String PROTOBUF_CONVERTER = "io.confluent.connect.protobuf.ProtobufConverter";
  private static final String JSON_SCHEMA_CONVERTER = "io.confluent.connect.json.JsonSchemaConverter";
  private static final String STRING_CONVERTER = "org.apache.kafka.connect.storage.StringConverter";
  private static final String BYTE_ARRAY_CONVERTER = "org.apache.kafka.connect.converters.ByteArrayConverter";
  private static final String SIMPLE_HEADER_CONVERTER = "org.apache.kafka.connect.storage.SimpleHeaderConverter";

  private static final String AVRO_FORMAT = "AvroFormat";
  private static final String JSON_FORMAT = "JsonFormat";
  private static final String PARQUET_FORMAT = "ParquetFormat";
  private static final String BYTE_ARRAY_FORMAT = "ByteArrayFormat";

  private static final String KEY_CONVERTER = "key.converter";
  private static final String VALUE_CONVERTER = "value.converter";
  private static final String KEY_SCHEMA_BACKUP_ENABLED = "key.converter.schema.backup.enabled";
  private static final String VALUE_SCHEMA_BACKUP_ENABLED = "value.converter.schema.backup.enabled";
  private static final String STORE_KAFKA_KEYS = "store.kafka.keys";
  private static final String STORE_KAFKA_HEADERS = "store.kafka.headers";
  private static final String TRANSFORMS = "transforms";
  private static final String HEADER_CONVERTER = "header.converter";
  private static final String PARQUET_CODEC = "parquet.codec";
  private static final String PARTITIONER_CLASS = "partitioner.class";
  private static final String TIMESTAMP_EXTRACTOR = "timestamp.extractor";
  private static final String DEFAULT_PARTITIONER =
      "io.confluent.connect.storage.partitioner.DefaultPartitioner";
  private static final String TIME_BASED_PARTITIONER =
      "io.confluent.connect.storage.partitioner.TimeBasedPartitioner";
  private static final String FIELD_PARTITIONER =
      "io.confluent.connect.storage.partitioner.FieldPartitioner";
  private static final String RECORD_FIELD_EXTRACTOR =
      "io.confluent.connect.storage.partitioner.TimeBasedPartitioner$RecordFieldTimestampExtractor";
  private static final String WALLCLOCK_EXTRACTOR =
      "io.confluent.connect.storage.partitioner.TimeBasedPartitioner$WallclockTimestampExtractor";
  private static final String SCHEMA_COMPATIBILITY = "schema.compatibility";

  private static final String TRUE = "true";
  private static final String FALSE = "false";

  private static final String ERR_STORE_KAFKA_KEYS_TRUE = "store.kafka.keys=true";
  private static final String ERR_STORE_KAFKA_HEADERS_TRUE = "store.kafka.headers=true";
  private static final String ERR_SR_BACKED_VALUE = "value.converter uses SR-backed converter";
  private static final String ERR_SR_BACKED_KEY = "key.converter uses SR-backed converter";
  private static final String ERR_TRANSFORMS = "Single Message Transforms";
  private static final String ERR_JSON_SCHEMA_ENABLE = "format.json.schema.enable=true is required";
  private static final String ERR_KEY_CONVERTER_UNSET = "key.converter must be set explicitly";
  private static final String ERR_VALUE_CONVERTER_UNSET = "value.converter must be set explicitly";

  private Map<String, String> baseSinkConfigs() {
    Map<String, String> configs = new HashMap<>();
    configs.put(KEY_CONVERTER, STRING_CONVERTER);
    configs.put(VALUE_CONVERTER, AVRO_CONVERTER);
    configs.put(VALUE_SCHEMA_BACKUP_ENABLED, TRUE);
    configs.put("value.converter.enhanced.avro.schema.support", TRUE);
    return configs;
  }

  private static boolean containsError(List<String> errors, String snippet) {
    return errors.stream().anyMatch(e -> e.contains(snippet));
  }

  @Test
  public void testValidSinkConfigProducesZeroErrors() {
    List<String> errors = BackupModeValidator.validateSinkConfigs(baseSinkConfigs(), AVRO_FORMAT, true);
    assertEquals(0, errors.size());
  }

  @Test
  public void testUnsetKeyConverterFails() {
    Map<String, String> configs = baseSinkConfigs();
    configs.remove(KEY_CONVERTER);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, ERR_KEY_CONVERTER_UNSET));
  }

  @Test
  public void testUnsetValueConverterFails() {
    Map<String, String> configs = baseSinkConfigs();
    configs.remove(VALUE_CONVERTER);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, ERR_VALUE_CONVERTER_UNSET));
  }

  @Test
  public void testUnsetBothConvertersFailsBoth() {
    Map<String, String> configs = baseSinkConfigs();
    configs.remove(KEY_CONVERTER);
    configs.remove(VALUE_CONVERTER);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, ERR_KEY_CONVERTER_UNSET));
    assertTrue(containsError(errors, ERR_VALUE_CONVERTER_UNSET));
  }

  @Test
  public void testByteArrayFormatSinkIsRejected() {
    List<String> errors = BackupModeValidator.validateSinkConfigs(baseSinkConfigs(), BYTE_ARRAY_FORMAT, true);
    assertTrue(containsError(errors, BYTE_ARRAY_FORMAT));
  }

  @Test
  public void testAvroFormatSinkPassesFormatCheck() {
    List<String> errors = BackupModeValidator.validateSinkConfigs(baseSinkConfigs(), AVRO_FORMAT, true);
    assertFalse(containsError(errors, BYTE_ARRAY_FORMAT));
  }

  @Test
  public void testJsonFormatWithoutSchemaEnableIsRejected() {
    List<String> errors = BackupModeValidator.validateSinkConfigs(baseSinkConfigs(), JSON_FORMAT, false);
    assertTrue(containsError(errors, ERR_JSON_SCHEMA_ENABLE));
  }

  @Test
  public void testJsonFormatWithSchemaEnableIsAccepted() {
    List<String> errors = BackupModeValidator.validateSinkConfigs(baseSinkConfigs(), JSON_FORMAT, true);
    assertFalse(containsError(errors, ERR_JSON_SCHEMA_ENABLE));
  }

  @Test
  public void testNonJsonFormatIgnoresSchemaEnableFlag() {
    List<String> errors = BackupModeValidator.validateSinkConfigs(baseSinkConfigs(), AVRO_FORMAT, false);
    assertFalse(containsError(errors, ERR_JSON_SCHEMA_ENABLE));
  }

  @Test
  public void testAvroValueConverterWithoutSchemaBackupEnabledIsRejected() {
    Map<String, String> configs = baseSinkConfigs();
    configs.remove(VALUE_SCHEMA_BACKUP_ENABLED);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, ERR_SR_BACKED_VALUE));
  }

  @Test
  public void testProtobufValueConverterWithoutSchemaBackupEnabledIsRejected() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(VALUE_CONVERTER, PROTOBUF_CONVERTER);
    configs.remove(VALUE_SCHEMA_BACKUP_ENABLED);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, ERR_SR_BACKED_VALUE));
  }

  @Test
  public void testStringValueConverterSkipsSchemaBackupCheck() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(VALUE_CONVERTER, STRING_CONVERTER);
    configs.remove(VALUE_SCHEMA_BACKUP_ENABLED);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertFalse(containsError(errors, "schema.backup.enabled"));
  }

  @Test
  public void testAvroKeyConverterWithoutSchemaBackupEnabledIsRejected() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(KEY_CONVERTER, AVRO_CONVERTER);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, ERR_SR_BACKED_KEY));
  }

  @Test
  public void testValueHasFlagKeyDoesNotFailsOnlyKey() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(KEY_CONVERTER, AVRO_CONVERTER);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, ERR_SR_BACKED_KEY));
    assertFalse(containsError(errors, ERR_SR_BACKED_VALUE));
  }

  @Test
  public void testBothSrBackedBothHaveFlagPasses() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(KEY_CONVERTER, AVRO_CONVERTER);
    configs.put(KEY_SCHEMA_BACKUP_ENABLED, TRUE);
    configs.put("key.converter.enhanced.avro.schema.support", TRUE);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertFalse(containsError(errors, "schema.backup.enabled"));
  }

  @Test
  public void testSinkAvroValueConverterWithoutEnhancedFails() {
    Map<String, String> configs = baseSinkConfigs();
    configs.remove("value.converter.enhanced.avro.schema.support");

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, "value.converter.enhanced.avro.schema.support"));
  }

  @Test
  public void testSinkAvroKeyConverterWithoutEnhancedFails() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(KEY_CONVERTER, AVRO_CONVERTER);
    configs.put(KEY_SCHEMA_BACKUP_ENABLED, TRUE);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, "key.converter.enhanced.avro.schema.support"));
  }

  @Test
  public void testSinkNonAvroConverterSkipsEnhancedCheck() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(VALUE_CONVERTER, STRING_CONVERTER);
    configs.remove("value.converter.enhanced.avro.schema.support");
    configs.remove(VALUE_SCHEMA_BACKUP_ENABLED);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertFalse(containsError(errors, "enhanced.avro.schema.support"));
  }

  @Test
  public void testTransformsSetIsRejected() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(TRANSFORMS, "myTransform");

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, ERR_TRANSFORMS));
  }

  @Test
  public void testTransformsWhitespaceIsAccepted() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(TRANSFORMS, "   ");

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertFalse(containsError(errors, ERR_TRANSFORMS));
  }

  @Test
  public void testTransformsAbsentIsAccepted() {
    List<String> errors = BackupModeValidator.validateSinkConfigs(baseSinkConfigs(), AVRO_FORMAT, true);
    assertFalse(containsError(errors, ERR_TRANSFORMS));
  }

  @Test
  public void testStoreKafkaKeysTrueIsRejected() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(STORE_KAFKA_KEYS, TRUE);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, ERR_STORE_KAFKA_KEYS_TRUE));
  }

  @Test
  public void testStoreKafkaHeadersTrueIsRejected() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(STORE_KAFKA_HEADERS, TRUE);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, ERR_STORE_KAFKA_HEADERS_TRUE));
  }

  @Test
  public void testStoreKafkaKeysFalseIsAccepted() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(STORE_KAFKA_KEYS, FALSE);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertFalse(containsError(errors, STORE_KAFKA_KEYS));
  }

  @Test
  public void testStoreKafkaHeadersFalseIsAccepted() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(STORE_KAFKA_HEADERS, FALSE);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertFalse(containsError(errors, STORE_KAFKA_HEADERS));
  }

  @Test
  public void testStoreKafkaKeysAbsentIsAccepted() {
    List<String> errors = BackupModeValidator.validateSinkConfigs(baseSinkConfigs(), AVRO_FORMAT, true);
    assertFalse(containsError(errors, STORE_KAFKA_KEYS));
  }

  @Test
  public void testStoreKafkaKeysAndHeadersBothTrueBothRejected() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(STORE_KAFKA_KEYS, TRUE);
    configs.put(STORE_KAFKA_HEADERS, TRUE);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, ERR_STORE_KAFKA_KEYS_TRUE));
    assertTrue(containsError(errors, ERR_STORE_KAFKA_HEADERS_TRUE));
  }

  @Test
  public void testStoreKafkaKeysErrorMentionsEnvelopeCaptures() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(STORE_KAFKA_KEYS, TRUE);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(errors.stream().anyMatch(e ->
        e.contains(ERR_STORE_KAFKA_KEYS_TRUE) && e.contains("Envelope")));
  }

  @Test
  public void testAvroConverterWithoutEnhancedSchemaSupportExercisesWarnPath() {
    List<String> errors = BackupModeValidator.validateSinkConfigs(baseSinkConfigs(), AVRO_FORMAT, true);
    assertEquals(0, errors.size());
  }

  @Test
  public void testAvroConverterWithEnhancedSchemaSupportExercisesWarnPath() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put("value.converter.enhanced.avro.schema.support", TRUE);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertEquals(0, errors.size());
  }

  @Test
  public void testProtobufConverterOptionalForNullablesWarnPath() {
    Map<String, String> configs = protobufSinkConfigs();

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertFalse(containsError(errors, "optional.for.nullables"));
  }

  @Test
  public void testJsonSchemaConverterWithAvroFormatExercisesWarnPath() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(VALUE_CONVERTER, JSON_SCHEMA_CONVERTER);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertEquals(0, errors.size());
  }

  @Test
  public void testJsonSchemaConverterWithNonAvroFormatSkipsRecommendation() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(VALUE_CONVERTER, JSON_SCHEMA_CONVERTER);
    configs.put(PARQUET_CODEC, "none");

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, PARQUET_FORMAT, true);
    assertEquals(0, errors.size());
  }

  @Test
  public void testStringValueConverterExercisesWarnPath() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(VALUE_CONVERTER, STRING_CONVERTER);
    configs.remove(VALUE_SCHEMA_BACKUP_ENABLED);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertEquals(0, errors.size());
  }

  @Test
  public void testParquetFormatWithSnappyCodecFails() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(PARQUET_CODEC, "snappy");

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, PARQUET_FORMAT, true);
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("parquet.codec=snappy"));
  }

  @Test
  public void testParquetFormatWithGzipCodecFails() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(PARQUET_CODEC, "gzip");

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, PARQUET_FORMAT, true);
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("parquet.codec=gzip"));
  }

  @Test
  public void testParquetFormatWithUnsetCodecFails() {
    Map<String, String> configs = baseSinkConfigs();
    configs.remove(PARQUET_CODEC);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, PARQUET_FORMAT, true);
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains("snappy (default)"));
  }

  @Test
  public void testParquetFormatWithNoneCodecPasses() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(PARQUET_CODEC, "none");

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, PARQUET_FORMAT, true);
    assertEquals(0, errors.size());
  }

  @Test
  public void testParquetFormatWithNoneCodecCaseInsensitivePasses() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(PARQUET_CODEC, "NONE");

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, PARQUET_FORMAT, true);
    assertEquals(0, errors.size());
  }

  @Test
  public void testNonParquetFormatIgnoresParquetCodec() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(PARQUET_CODEC, "snappy");

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertEquals(0, errors.size());
  }

  @Test
  public void testByteArrayHeaderConverterPassesHeaderCheck() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(HEADER_CONVERTER, BYTE_ARRAY_CONVERTER);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertEquals(0, errors.size());
  }

  @Test
  public void testAbsentHeaderConverterExercisesInfoLog() {
    List<String> errors = BackupModeValidator.validateSinkConfigs(baseSinkConfigs(), AVRO_FORMAT, true);
    assertEquals(0, errors.size());
  }

  @Test
  public void testCustomHeaderConverterExercisesInfoLog() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(HEADER_CONVERTER, SIMPLE_HEADER_CONVERTER);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertEquals(0, errors.size());
  }

  @Test
  public void testSchemaCompatibilityNoneExercisesWarnPath() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(SCHEMA_COMPATIBILITY, "NONE");

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertEquals(0, errors.size());
  }

  @Test
  public void testSchemaCompatibilityBackwardExercisesWarnPath() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(SCHEMA_COMPATIBILITY, "BACKWARD");

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertEquals(0, errors.size());
  }

  @Test
  public void testSchemaCompatibilityAbsentExercisesWarnPath() {
    List<String> errors = BackupModeValidator.validateSinkConfigs(baseSinkConfigs(), AVRO_FORMAT, true);
    assertEquals(0, errors.size());
  }

  @Test
  public void testValidSourceConfigProducesZeroErrors() {
    Map<String, String> configs = new HashMap<>();
    configs.put(VALUE_CONVERTER, AVRO_CONVERTER);
    configs.put("value.converter.enhanced.avro.schema.support", TRUE);

    List<String> errors = BackupModeValidator.validateSourceConfigs(configs, AVRO_FORMAT);
    assertEquals(0, errors.size());
  }

  @Test
  public void testByteArrayFormatSourceIsRejected() {
    Map<String, String> configs = new HashMap<>();
    configs.put(VALUE_CONVERTER, AVRO_CONVERTER);
    configs.put("value.converter.enhanced.avro.schema.support", TRUE);

    List<String> errors = BackupModeValidator.validateSourceConfigs(configs, BYTE_ARRAY_FORMAT);
    assertTrue(containsError(errors, BYTE_ARRAY_FORMAT));
  }

  @Test
  public void testSourceAvroValueConverterWithoutEnhancedFails() {
    Map<String, String> configs = new HashMap<>();
    configs.put(VALUE_CONVERTER, AVRO_CONVERTER);

    List<String> errors = BackupModeValidator.validateSourceConfigs(configs, AVRO_FORMAT);
    assertTrue(containsError(errors, "value.converter.enhanced.avro.schema.support"));
  }

  @Test
  public void testSourceAvroKeyConverterWithoutEnhancedFails() {
    Map<String, String> configs = new HashMap<>();
    configs.put(KEY_CONVERTER, AVRO_CONVERTER);
    configs.put(VALUE_CONVERTER, STRING_CONVERTER);

    List<String> errors = BackupModeValidator.validateSourceConfigs(configs, AVRO_FORMAT);
    assertTrue(containsError(errors, "key.converter.enhanced.avro.schema.support"));
  }

  @Test
  public void testSourceNonAvroConverterSkipsEnhancedCheck() {
    Map<String, String> configs = new HashMap<>();
    configs.put(VALUE_CONVERTER, STRING_CONVERTER);

    List<String> errors = BackupModeValidator.validateSourceConfigs(configs, AVRO_FORMAT);
    assertFalse(containsError(errors, "enhanced.avro.schema.support"));
  }

  @Test
  public void testSourceProtobufValueConverterWithoutEnhancedFails() {
    Map<String, String> configs = new HashMap<>();
    configs.put(VALUE_CONVERTER, PROTOBUF_CONVERTER);

    List<String> errors = BackupModeValidator.validateSourceConfigs(configs, AVRO_FORMAT);
    assertTrue(containsError(errors, "value.converter.enhanced.protobuf.schema.support"));
  }

  @Test
  public void testLogSinkStartupSummaryDoesNotThrow() {
    BackupModeValidator.logSinkStartupSummary(baseSinkConfigs(), AVRO_FORMAT, "STRING", "AVRO");
  }

  @Test
  public void testLogSinkStartupSummaryWithNullConvertersDoesNotThrow() {
    BackupModeValidator.logSinkStartupSummary(new HashMap<>(), AVRO_FORMAT, "NONE", "NONE");
  }

  @Test
  public void testDefaultPartitionerPasses() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(PARTITIONER_CLASS, DEFAULT_PARTITIONER);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertFalse(containsError(errors, "partitioner.class"));
  }

  @Test
  public void testTimeBasedPartitionerPasses() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(PARTITIONER_CLASS, TIME_BASED_PARTITIONER);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertFalse(containsError(errors, "partitioner.class"));
  }

  @Test
  public void testFieldPartitionerIsRejected() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(PARTITIONER_CLASS, FIELD_PARTITIONER);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, "FieldPartitioner"));
  }

  @Test
  public void testCustomPartitionerIsRejected() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(PARTITIONER_CLASS, "com.example.CustomPartitioner");

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, "com.example.CustomPartitioner"));
  }

  @Test
  public void testUnsetPartitionerPasses() {
    List<String> errors = BackupModeValidator.validateSinkConfigs(
        baseSinkConfigs(), AVRO_FORMAT, true);
    assertFalse(containsError(errors, "partitioner.class"));
  }

  @Test
  public void testWallclockTimestampExtractorPasses() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(PARTITIONER_CLASS, TIME_BASED_PARTITIONER);
    configs.put(TIMESTAMP_EXTRACTOR, WALLCLOCK_EXTRACTOR);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertFalse(containsError(errors, "timestamp.extractor"));
  }

  @Test
  public void testRecordFieldTimestampExtractorIsRejected() {
    Map<String, String> configs = baseSinkConfigs();
    configs.put(PARTITIONER_CLASS, TIME_BASED_PARTITIONER);
    configs.put(TIMESTAMP_EXTRACTOR, RECORD_FIELD_EXTRACTOR);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, "RecordFieldTimestampExtractor"));
  }

  // ── Protobuf converter validators ────────────────────────────────

  private Map<String, String> protobufSinkConfigs() {
    Map<String, String> configs = new HashMap<>();
    configs.put(KEY_CONVERTER, STRING_CONVERTER);
    configs.put(VALUE_CONVERTER, PROTOBUF_CONVERTER);
    configs.put(VALUE_SCHEMA_BACKUP_ENABLED, TRUE);
    configs.put("value.converter.enhanced.protobuf.schema.support", TRUE);
    configs.put("value.converter.wrapper.for.raw.primitives", FALSE);
    return configs;
  }

  @Test
  public void testProtobufSinkAllFlagsSetPasses() {
    List<String> errors = BackupModeValidator.validateSinkConfigs(
        protobufSinkConfigs(), AVRO_FORMAT, true);
    assertFalse(containsError(errors, "protobuf"));
    assertFalse(containsError(errors, "wrapper.for"));
  }

  @Test
  public void testProtobufSinkWithoutEnhancedFails() {
    Map<String, String> configs = protobufSinkConfigs();
    configs.remove("value.converter.enhanced.protobuf.schema.support");

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, "value.converter.enhanced.protobuf.schema.support"));
  }

  @Test
  public void testProtobufSinkWrapperForRawPrimitivesUnsetFails() {
    Map<String, String> configs = protobufSinkConfigs();
    configs.remove("value.converter.wrapper.for.raw.primitives");

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, "value.converter.wrapper.for.raw.primitives"));
  }

  @Test
  public void testProtobufSinkWrapperForRawPrimitivesTrueFails() {
    Map<String, String> configs = protobufSinkConfigs();
    configs.put("value.converter.wrapper.for.raw.primitives", TRUE);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, "value.converter.wrapper.for.raw.primitives"));
  }

  @Test
  public void testProtobufSinkWrapperForNullablesTrueFails() {
    Map<String, String> configs = protobufSinkConfigs();
    configs.put("value.converter.wrapper.for.nullables", TRUE);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, "value.converter.wrapper.for.nullables"));
  }

  @Test
  public void testProtobufKeyConverterValidatedToo() {
    Map<String, String> configs = protobufSinkConfigs();
    configs.put(KEY_CONVERTER, PROTOBUF_CONVERTER);
    configs.put(KEY_SCHEMA_BACKUP_ENABLED, TRUE);

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertTrue(containsError(errors, "key.converter.enhanced.protobuf.schema.support"));
    assertTrue(containsError(errors, "key.converter.wrapper.for.raw.primitives"));
  }

  @Test
  public void testProtobufSourceWithoutEnhancedFails() {
    Map<String, String> configs = new HashMap<>();
    configs.put(VALUE_CONVERTER, PROTOBUF_CONVERTER);

    List<String> errors = BackupModeValidator.validateSourceConfigs(configs, AVRO_FORMAT);
    assertTrue(containsError(errors, "value.converter.enhanced.protobuf.schema.support"));
  }

  @Test
  public void testProtobufSourceWrapperForRawPrimitivesNotValidated() {
    Map<String, String> configs = new HashMap<>();
    configs.put(VALUE_CONVERTER, PROTOBUF_CONVERTER);
    configs.put("value.converter.enhanced.protobuf.schema.support", TRUE);

    List<String> errors = BackupModeValidator.validateSourceConfigs(configs, AVRO_FORMAT);
    assertFalse(containsError(errors, "wrapper.for.raw.primitives"));
  }

  @Test
  public void testProtobufSourceWrapperForNullablesTrueFails() {
    Map<String, String> configs = new HashMap<>();
    configs.put(VALUE_CONVERTER, PROTOBUF_CONVERTER);
    configs.put("value.converter.enhanced.protobuf.schema.support", TRUE);
    configs.put("value.converter.wrapper.for.nullables", TRUE);

    List<String> errors = BackupModeValidator.validateSourceConfigs(configs, AVRO_FORMAT);
    assertTrue(containsError(errors, "value.converter.wrapper.for.nullables"));
  }

  @Test
  public void testNonProtobufConverterSkipsAllProtobufChecks() {
    Map<String, String> configs = baseSinkConfigs();

    List<String> errors = BackupModeValidator.validateSinkConfigs(configs, AVRO_FORMAT, true);
    assertFalse(containsError(errors, "protobuf"));
    assertFalse(containsError(errors, "wrapper.for"));
  }
}
