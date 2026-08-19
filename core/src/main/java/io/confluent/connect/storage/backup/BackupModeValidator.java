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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

/**
 * Shared config validation for backup and restore modes across all
 * object storage connectors (S3, GCS, Azure). CSP-specific validators
 * delegate to this class for backup/restore-related checks.
 *
 * <p>Validations are organized in three tiers:
 * <ul>
 *   <li>Tier 1 (FAIL): Returns errors that prevent connector start</li>
 *   <li>Tier 2 (WARN): Logs warnings for suboptimal configs</li>
 *   <li>Tier 3 (INFO): Logs startup summary for troubleshooting</li>
 * </ul>
 */
public final class BackupModeValidator {

  private static final Logger log = LoggerFactory.getLogger(BackupModeValidator.class);

  private static final String AVRO_CONVERTER =
      "io.confluent.connect.avro.AvroConverter";
  private static final String PROTOBUF_CONVERTER =
      "io.confluent.connect.protobuf.ProtobufConverter";
  private static final String JSON_SCHEMA_CONVERTER =
      "io.confluent.connect.json.JsonSchemaConverter";

  private static final String PARTITIONER_PKG =
      "io.confluent.connect.storage.partitioner.";
  private static final Set<String> SUPPORTED_PARTITIONERS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
          PARTITIONER_PKG + "DefaultPartitioner",
          PARTITIONER_PKG + "TimeBasedPartitioner",
          PARTITIONER_PKG + "DailyPartitioner",
          PARTITIONER_PKG + "HourlyPartitioner")));
  // TimeBasedPartitioner.newTimestampExtractor() expands the short name
  // "RecordField" to the FQCN at runtime, so both must be rejected.
  private static final Set<String> UNSUPPORTED_TIMESTAMP_EXTRACTORS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
          "RecordField",
          PARTITIONER_PKG + "TimeBasedPartitioner$RecordFieldTimestampExtractor")));

  private static final String FORMAT_SIMPLE_NAME_JSON = "JsonFormat";
  private static final String FORMAT_SIMPLE_NAME_BYTE_ARRAY = "ByteArrayFormat";
  private static final String FORMAT_SIMPLE_NAME_AVRO = "AvroFormat";
  private static final String FORMAT_SIMPLE_NAME_PARQUET = "ParquetFormat";

  private BackupModeValidator() {
  }

  /**
   * Validates sink (backup) connector configs. Returns a list of error
   * messages for Tier 1 failures. Logs Tier 2 warnings.
   *
   * @param configs the full connector config map
   * @param formatClassName the resolved format class simple name
   * @param jsonSchemaEmbedded whether format.json.schema.enable is true
   * @return list of error messages (empty if all valid)
   */
  public static List<String> validateSinkConfigs(
      Map<String, String> configs,
      String formatClassName,
      boolean jsonSchemaEmbedded) {
    List<String> errors = new ArrayList<>();

    validateByteArrayFormat(formatClassName, errors);
    validateJsonFormatSchemaEnable(formatClassName, jsonSchemaEmbedded, errors);
    validateParquetCompression(configs, formatClassName, errors);
    validateConverterExplicitlySet(configs, BackupEnvelope.KEY_CONVERTER_CONFIG, errors);
    validateConverterExplicitlySet(configs, BackupEnvelope.VALUE_CONVERTER_CONFIG, errors);
    validateSinkConverter(configs, BackupEnvelope.KEY_CONVERTER_CONFIG, errors);
    validateSinkConverter(configs, BackupEnvelope.VALUE_CONVERTER_CONFIG, errors);
    validateTransformsRejected(configs, errors);
    validateStoreKafkaKeysHeadersRejected(configs, errors);
    validatePartitionerSupported(configs, errors);

    warnSinkSuboptimalConfigs(configs, formatClassName);

    return errors;
  }

  /**
   * Validates source (restore) connector configs. Returns a list of error
   * messages for Tier 1 failures. Logs Tier 2 warnings.
   *
   * @param configs the full connector config map
   * @param formatClassName the resolved format class simple name
   * @return list of error messages (empty if all valid)
   */
  public static List<String> validateSourceConfigs(
      Map<String, String> configs,
      String formatClassName) {
    List<String> errors = new ArrayList<>();

    validateByteArrayFormat(formatClassName, errors);
    validateSourceConverter(configs, BackupEnvelope.KEY_CONVERTER_CONFIG, errors);
    validateSourceConverter(configs, BackupEnvelope.VALUE_CONVERTER_CONFIG, errors);
    warnSourceSuboptimalConfigs(configs);

    return errors;
  }

  private static void validateSinkConverter(
      Map<String, String> configs, String prefix, List<String> errors) {
    String converterClass = configs.get(prefix);
    if (converterClass == null) {
      return;
    }
    validateSchemaBackupEnabled(configs, prefix, errors);
    if (AVRO_CONVERTER.equals(converterClass)) {
      requireTrue(configs, prefix + ".enhanced.avro.schema.support",
          "Restore will fail with AvroTypeException on records that contain "
          + "enum values (e.g. \"value ACTIVE is not a UserStatus\").",
          errors);
    } else if (PROTOBUF_CONVERTER.equals(converterClass)) {
      requireTrue(configs, prefix + ".enhanced.protobuf.schema.support",
          "Package qualification in Connect Schema is not preserved, which "
          + "can break restore.",
          errors);
      requireFalse(configs, prefix + ".wrapper.for.raw.primitives",
          "Default is true. With the default, ProtobufData strips wrapper "
          + "type info from BackupWrapper.data and restore cannot re-create "
          + "the wrappers from the flattened Struct.",
          errors);
      rejectIfTrue(configs, prefix + ".wrapper.for.nullables",
          "Breaks records that omit proto3 optional scalar fields with "
          + "DataException 'Invalid value: null used for required field'.",
          errors);
    }
  }

  private static void validateSourceConverter(
      Map<String, String> configs, String prefix, List<String> errors) {
    String converterClass = configs.get(prefix);
    if (converterClass == null) {
      return;
    }
    if (AVRO_CONVERTER.equals(converterClass)) {
      requireTrue(configs, prefix + ".enhanced.avro.schema.support",
          "Restore will fail with AvroTypeException on records that contain "
          + "enum values (e.g. \"value ACTIVE is not a UserStatus\").",
          errors);
    } else if (PROTOBUF_CONVERTER.equals(converterClass)) {
      requireTrue(configs, prefix + ".enhanced.protobuf.schema.support",
          "Package qualification in Connect Schema is not preserved, which "
          + "can break restore.",
          errors);
      rejectIfTrue(configs, prefix + ".wrapper.for.nullables",
          "Breaks records that omit proto3 optional scalar fields with "
          + "DataException 'Invalid value: null used for required field'.",
          errors);
    }
  }

  private static void requireTrue(
      Map<String, String> configs, String key, String reason,
      List<String> errors) {
    if (!"true".equalsIgnoreCase(configs.get(key))) {
      errors.add(key + " must be set to true. " + reason);
    }
  }

  private static void requireFalse(
      Map<String, String> configs, String key, String reason,
      List<String> errors) {
    String value = configs.get(key);
    if (value == null || !"false".equalsIgnoreCase(value)) {
      errors.add(key + " must be set to false. " + reason);
    }
  }

  private static void rejectIfTrue(
      Map<String, String> configs, String key, String reason,
      List<String> errors) {
    if ("true".equalsIgnoreCase(configs.get(key))) {
      errors.add(key + " must not be set to true. " + reason);
    }
  }

  /**
   * Logs a startup summary for backup mode troubleshooting.
   *
   * @param configs the full connector config map
   * @param formatClassName the format class simple name
   * @param keyType detected key schema type
   * @param valueType detected value schema type
   */
  public static void logSinkStartupSummary(
      Map<String, String> configs,
      String formatClassName, String keyType, String valueType) {
    String valConverter = configs.get(BackupEnvelope.VALUE_CONVERTER_CONFIG);
    String keyConverter = configs.get(BackupEnvelope.KEY_CONVERTER_CONFIG);
    String keyBackupEnabled = configs.get(
        BackupEnvelope.KEY_CONVERTER_CONFIG + "."
        + BackupEnvelope.SCHEMA_BACKUP_ENABLED_CONFIG);
    String valueBackupEnabled = configs.get(
        BackupEnvelope.VALUE_CONVERTER_CONFIG + "."
        + BackupEnvelope.SCHEMA_BACKUP_ENABLED_CONFIG);
    log.info("Backup mode started: format={}, "
        + "keyConverter={} (type={}, schema.backup.enabled={}), "
        + "valueConverter={} (type={}, schema.backup.enabled={})",
        formatClassName,
        keyConverter, keyType, keyBackupEnabled,
        valConverter, valueType, valueBackupEnabled);
  }

  // ── Tier 1: FAIL ──────────────────────────────────────────────

  private static void validateByteArrayFormat(
      String formatClassName, List<String> errors) {
    if (FORMAT_SIMPLE_NAME_BYTE_ARRAY.equals(formatClassName)) {
      errors.add("format.class=ByteArrayFormat cannot be used with "
          + "BACKUP_FULL_RECORD mode. ByteArrayFormat does not support "
          + "structured schema metadata required for envelope wrapping. "
          + "Use AvroFormat, JsonFormat, or ParquetFormat instead.");
    }
  }

  private static void validateJsonFormatSchemaEnable(
      String formatClassName, boolean jsonSchemaEmbedded,
      List<String> errors) {
    if (FORMAT_SIMPLE_NAME_JSON.equals(formatClassName) && !jsonSchemaEmbedded) {
      errors.add("format.json.schema.enable=true is required with "
          + "JsonFormat in BACKUP_FULL_RECORD mode. Without it, the "
          + "envelope schema is not embedded and restore cannot parse "
          + "the records.");
    }
  }

  private static void validatePartitionerSupported(
      Map<String, String> configs, List<String> errors) {
    String partitioner = configs.get("partitioner.class");
    if (partitioner != null && !SUPPORTED_PARTITIONERS.contains(partitioner)) {
      errors.add("partitioner.class=" + partitioner + " is not supported in "
          + "BACKUP_FULL_RECORD mode. The sink task passes a "
          + "KafkaRecordEnvelope Struct to the partitioner, not the original "
          + "payload, so partitioners that read user-data fields (e.g. "
          + "FieldPartitioner) fail. Use DefaultPartitioner, "
          + "TimeBasedPartitioner, DailyPartitioner, or HourlyPartitioner.");
    }
    String extractor = configs.get("timestamp.extractor");
    if (extractor != null && UNSUPPORTED_TIMESTAMP_EXTRACTORS.contains(extractor)) {
      errors.add("timestamp.extractor=" + extractor + " is not supported in "
          + "BACKUP_FULL_RECORD mode. It reads a field from the record value, "
          + "which is now the envelope Struct. Use Wallclock or Record "
          + "extractor instead.");
    }
  }

  private static void validateConverterExplicitlySet(
      Map<String, String> configs, String converterPrefix,
      List<String> errors) {
    if (configs.get(converterPrefix) == null) {
      errors.add(converterPrefix + " must be set explicitly at the connector "
          + "level in BACKUP_FULL_RECORD mode. Relying on worker.properties "
          + "defaults hides the converter class from backup validation, so "
          + "schema type detection falls through to UNKNOWN and schema files "
          + "are not written. Set " + converterPrefix + " on the connector.");
    }
  }

  private static void validateSchemaBackupEnabled(
      Map<String, String> configs, String converterPrefix,
      List<String> errors) {
    String converterClass = configs.get(converterPrefix);
    String schemaType = ConverterTypeDetector.detectSchemaType(
        converterClass, configs, converterPrefix);
    if (!BackupEnvelope.isSrBackedType(schemaType)) {
      return;
    }
    String configKey = converterPrefix + "."
        + BackupEnvelope.SCHEMA_BACKUP_ENABLED_CONFIG;
    if (!"true".equalsIgnoreCase(configs.get(configKey))) {
      errors.add(converterPrefix + " uses SR-backed converter ("
          + converterClass + ") but " + configKey + " is not set to true. "
          + "Without this config, backup will NOT preserve schema metadata "
          + "and restore will produce corrupted data. "
          + "Set " + configKey + "=true.");
    }
  }

  private static void validateTransformsRejected(
      Map<String, String> configs, List<String> errors) {
    String transforms = configs.get("transforms");
    if (transforms != null && !transforms.trim().isEmpty()) {
      errors.add("Single Message Transforms (SMTs) cannot be used with "
          + "BACKUP_FULL_RECORD mode. SMTs modify data before envelope "
          + "wrapping, which corrupts backup fidelity. "
          + "Remove the 'transforms' config to use backup mode.");
    }
  }

  private static void validateParquetCompression(
      Map<String, String> configs, String formatClassName,
      List<String> errors) {
    if (!FORMAT_SIMPLE_NAME_PARQUET.equals(formatClassName)) {
      return;
    }
    String codec = configs.get("parquet.codec");
    if (codec == null || !"none".equalsIgnoreCase(codec)) {
      errors.add("parquet.codec=" + (codec != null ? codec : "snappy (default)")
          + " cannot be used with BACKUP_FULL_RECORD mode. Backup and restore "
          + "does not support compression end-to-end: the sink writes files "
          + "with a codec-prefixed extension (e.g. .snappy.parquet) but the "
          + "restore file matcher only accepts the bare .parquet extension, "
          + "so compressed files are silently skipped. "
          + "Set parquet.codec=none to use backup mode.");
    }
  }

  private static void validateStoreKafkaKeysHeadersRejected(
      Map<String, String> configs, List<String> errors) {
    if ("true".equalsIgnoreCase(configs.get("store.kafka.keys"))) {
      errors.add("store.kafka.keys=true cannot be used with "
          + "BACKUP_FULL_RECORD mode. Envelope mode already captures the "
          + "Kafka key inside each backup record. Setting this flag would "
          + "write duplicate key-only files alongside the envelope files. "
          + "Remove store.kafka.keys (or set to false) to use backup mode.");
    }
    if ("true".equalsIgnoreCase(configs.get("store.kafka.headers"))) {
      errors.add("store.kafka.headers=true cannot be used with "
          + "BACKUP_FULL_RECORD mode. Envelope mode already captures the "
          + "Kafka headers inside each backup record. Setting this flag "
          + "would write duplicate header-only files alongside the envelope "
          + "files. Remove store.kafka.headers (or set to false) to use "
          + "backup mode.");
    }
  }

  // ── Tier 2: WARN (sink) ───────────────────────────────────────

  private static void warnSinkSuboptimalConfigs(
      Map<String, String> configs, String formatClassName) {
    String valConverter = configs.get(BackupEnvelope.VALUE_CONVERTER_CONFIG);
    String keyConverter = configs.get(BackupEnvelope.KEY_CONVERTER_CONFIG);

    warnConverterConfigs(configs, valConverter,
        BackupEnvelope.VALUE_CONVERTER_CONFIG, formatClassName);
    warnConverterConfigs(configs, keyConverter,
        BackupEnvelope.KEY_CONVERTER_CONFIG, formatClassName);

    warnHeaderConverter(configs);
    warnSchemaCompatibilityOverride(configs);
  }

  private static void warnConverterConfigs(
      Map<String, String> configs, String converterClass,
      String prefix, String formatClassName) {
    if (converterClass == null) {
      return;
    }
    if (PROTOBUF_CONVERTER.equals(converterClass)) {
      warnIfNotTrue(configs, prefix + ".optional.for.nullables",
          prefix + ": optional.for.nullables=true is recommended for "
          + "backup mode and must match the source-side setting. Restore "
          + "may drop proto3 optional scalars holding default values "
          + "without it.");
    }

    if (JSON_SCHEMA_CONVERTER.equals(converterClass)) {
      warnJsonTypeAllowedPackages(configs, prefix);
      warnIfNotTrue(configs, prefix + ".preserve.additional.properties",
          prefix + ": preserve.additional.properties=true is recommended "
          + "when the JSON schema uses additionalProperties. Default false "
          + "drops undeclared JSON properties on restore. Must match the "
          + "source-side setting. Uses reserved field name "
          + "__connect_additional_properties__; schemas with that literal "
          + "property name will fail-fast at conversion.");
      if (FORMAT_SIMPLE_NAME_AVRO.equals(formatClassName)) {
        warnIfNotTrue(configs, prefix + ".generalized.sum.type.support",
            prefix + ": generalized.sum.type.support=true is recommended "
            + "when using JsonSchemaConverter with AvroFormat. "
            + "oneOf fields may fail without it.");
        warnIfNotTrue(configs, prefix + ".scrub.invalid.names",
            prefix + ": scrub.invalid.names=true is recommended when using "
            + "JsonSchemaConverter with AvroFormat.");
      }
    }
  }

  private static void warnJsonTypeAllowedPackages(
      Map<String, String> configs, String prefix) {
    String key = prefix + ".json.type.allowed.packages";
    String value = configs.get(key);
    if (value == null || "*".equals(value.trim())) {
      log.warn("{}: json.type.allowed.packages={} allows any class to be "
          + "loaded via javaType. Set an explicit whitelist (e.g. "
          + "\"com.example.models\") or empty string to disallow, and match "
          + "the setting on the restore side.",
          prefix, value != null ? value : "* (default)");
    }
  }

  private static void warnHeaderConverter(Map<String, String> configs) {
    String headerConverter = configs.get("header.converter");
    if (headerConverter == null
        || !headerConverter.contains("ByteArrayConverter")) {
      log.info("header.converter={} — for pristine byte-level header "
          + "preservation, consider using "
          + "org.apache.kafka.connect.converters.ByteArrayConverter.",
          headerConverter != null ? headerConverter : "(default)");
    }
  }

  private static void warnSchemaCompatibilityOverride(
      Map<String, String> configs) {
    String userValue = configs.get("schema.compatibility");
    if (userValue != null && !"NONE".equalsIgnoreCase(userValue)) {
      log.warn("schema.compatibility={} was set but will be overridden to NONE "
          + "in BACKUP_FULL_RECORD mode. Backup preserves each record's own "
          + "schema exactly; compatibility rules do not apply. "
          + "Remove schema.compatibility (or set to NONE) to silence this warning.",
          userValue);
    }
  }

  // ── Tier 2: WARN (source) ─────────────────────────────────────

  private static void warnSourceSuboptimalConfigs(
      Map<String, String> configs) {
    String valConverter = configs.get(BackupEnvelope.VALUE_CONVERTER_CONFIG);

    if (PROTOBUF_CONVERTER.equals(valConverter)) {
      warnIfNotTrue(configs,
          BackupEnvelope.VALUE_CONVERTER_CONFIG + ".optional.for.nullables",
          "value.converter: optional.for.nullables=true is recommended "
          + "for restore mode and must match the sink-side setting.");
    }
    if (JSON_SCHEMA_CONVERTER.equals(valConverter)) {
      warnJsonTypeAllowedPackages(configs, BackupEnvelope.VALUE_CONVERTER_CONFIG);
      warnIfNotTrue(configs,
          BackupEnvelope.VALUE_CONVERTER_CONFIG + ".preserve.additional.properties",
          "value.converter: preserve.additional.properties=true is "
          + "recommended for restore mode and must match the sink-side setting.");
    }

    warnHeaderConverter(configs);
  }

  // ── Helpers ───────────────────────────────────────────────────

  private static void warnIfNotTrue(
      Map<String, String> configs, String key, String message) {
    if (!"true".equalsIgnoreCase(configs.get(key))) {
      log.warn(message);
    }
  }
}
