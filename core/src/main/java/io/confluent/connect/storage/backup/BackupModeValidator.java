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
import java.util.List;
import java.util.Map;

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
    validateSchemaBackupEnabled(configs, BackupEnvelope.KEY_CONVERTER_CONFIG, errors);
    validateSchemaBackupEnabled(configs, BackupEnvelope.VALUE_CONVERTER_CONFIG, errors);
    validateTransformsRejected(configs, errors);

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
    warnSourceSuboptimalConfigs(configs);

    return errors;
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
    String backupEnabled = configs.get(
        BackupEnvelope.VALUE_CONVERTER_CONFIG + "."
        + BackupEnvelope.SCHEMA_BACKUP_ENABLED_CONFIG);
    log.info("Backup mode started: format={}, keyConverter={} (type={}), "
        + "valueConverter={} (type={}), schema.backup.enabled={}",
        formatClassName, keyConverter, keyType, valConverter, valueType,
        backupEnabled);
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

  // ── Tier 2: WARN (sink) ───────────────────────────────────────

  private static void warnSinkSuboptimalConfigs(
      Map<String, String> configs, String formatClassName) {
    String valConverter = configs.get(BackupEnvelope.VALUE_CONVERTER_CONFIG);
    String keyConverter = configs.get(BackupEnvelope.KEY_CONVERTER_CONFIG);

    warnConverterConfigs(configs, valConverter,
        BackupEnvelope.VALUE_CONVERTER_CONFIG, formatClassName);
    warnConverterConfigs(configs, keyConverter,
        BackupEnvelope.KEY_CONVERTER_CONFIG, formatClassName);

    warnParquetCompression(configs, formatClassName);
    warnHeaderConverter(configs);
  }

  private static void warnConverterConfigs(
      Map<String, String> configs, String converterClass,
      String prefix, String formatClassName) {
    if (converterClass == null) {
      return;
    }
    if (AVRO_CONVERTER.equals(converterClass)) {
      warnIfNotTrue(configs, prefix + ".enhanced.avro.schema.support",
          prefix + ": enhanced.avro.schema.support=true is recommended "
          + "for backup mode. Without it, Avro enum types may not be "
          + "preserved during restore.");
    }

    if (PROTOBUF_CONVERTER.equals(converterClass)) {
      warnIfNotTrue(configs, prefix + ".enhanced.protobuf.schema.support",
          prefix + ": enhanced.protobuf.schema.support=true is recommended "
          + "for backup mode. Without it, Protobuf schema fidelity may "
          + "be reduced during restore.");

      String optNullable = configs.get(prefix + ".optional.for.nullables");
      String wrapNullable = configs.get(prefix + ".wrapper.for.nullables");
      if (!"true".equalsIgnoreCase(optNullable)
          && !"true".equalsIgnoreCase(wrapNullable)) {
        log.warn("{}: neither optional.for.nullables nor "
            + "wrapper.for.nullables is set. If the Protobuf schema uses "
            + "nullable fields, they will not be preserved during restore. "
            + "Set optional.for.nullables=true (recommended) or "
            + "wrapper.for.nullables=true.", prefix);
      }
    }

    if (JSON_SCHEMA_CONVERTER.equals(converterClass)
        && FORMAT_SIMPLE_NAME_AVRO.equals(formatClassName)) {
      warnIfNotTrue(configs, prefix + ".generalized.sum.type.support",
          prefix + ": generalized.sum.type.support=true is recommended "
          + "when using JsonSchemaConverter with AvroFormat. "
          + "oneOf fields may fail without it.");
      warnIfNotTrue(configs, prefix + ".scrub.invalid.names",
          prefix + ": scrub.invalid.names=true is recommended when using "
          + "JsonSchemaConverter with AvroFormat.");
    }
  }

  private static void warnParquetCompression(
      Map<String, String> configs, String formatClassName) {
    if (!FORMAT_SIMPLE_NAME_PARQUET.equals(formatClassName)) {
      return;
    }
    String codec = configs.get("parquet.codec");
    if (codec != null && !"none".equalsIgnoreCase(codec)) {
      log.warn("parquet.codec={} detected with backup mode. "
          + "parquet.codec=none is recommended for backup mode. "
          + "Compressed parquet files may cause restore failures.", codec);
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

  // ── Tier 2: WARN (source) ─────────────────────────────────────

  private static void warnSourceSuboptimalConfigs(
      Map<String, String> configs) {
    String valConverter = configs.get(BackupEnvelope.VALUE_CONVERTER_CONFIG);

    if (AVRO_CONVERTER.equals(valConverter)) {
      warnIfNotTrue(configs,
          BackupEnvelope.VALUE_CONVERTER_CONFIG
              + ".enhanced.avro.schema.support",
          "value.converter: enhanced.avro.schema.support=true is "
          + "recommended for restore mode to match backup fidelity.");
    }

    if (PROTOBUF_CONVERTER.equals(valConverter)) {
      warnIfNotTrue(configs,
          BackupEnvelope.VALUE_CONVERTER_CONFIG
              + ".enhanced.protobuf.schema.support",
          "value.converter: enhanced.protobuf.schema.support=true is "
          + "recommended for restore mode to match backup fidelity.");
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
