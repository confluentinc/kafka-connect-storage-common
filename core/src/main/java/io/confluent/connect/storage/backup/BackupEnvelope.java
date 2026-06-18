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

/**
 * Constants for the KafkaRecordEnvelope struct and schema type tags.
 */
public final class BackupEnvelope {

  public static final String NAME =
      "io.confluent.connect.storage.KafkaRecordEnvelope";

  // Envelope field names
  public static final String FIELD_KEY = "key";
  public static final String FIELD_VALUE = "value";
  public static final String FIELD_HEADERS = "headers";
  public static final String FIELD_TOPIC = "topic";
  public static final String FIELD_PARTITION = "partition";
  public static final String FIELD_OFFSET = "offset";
  public static final String FIELD_TIMESTAMP = "timestamp";
  public static final String FIELD_TIMESTAMP_TYPE = "timestampType";
  public static final String FIELD_KEY_SCHEMA_ID = "keySchemaId";
  public static final String FIELD_VALUE_SCHEMA_ID = "valueSchemaId";
  public static final String FIELD_KEY_SCHEMA_TYPE = "keySchemaType";
  public static final String FIELD_VALUE_SCHEMA_TYPE = "valueSchemaType";
  public static final String FIELD_KEY_SCHEMA_SUBJECT = "keySchemaSubject";
  public static final String FIELD_VALUE_SCHEMA_SUBJECT = "valueSchemaSubject";

  // Header struct field names
  public static final String FIELD_HEADER_KEY = "headerKey";
  public static final String FIELD_HEADER_VALUE = "headerValue";
  public static final String FIELD_HEADER_SCHEMA_TYPE = "headerSchemaType";

  // Converter schema backup config key.
  // Must match BackupWrapper.SCHEMA_BACKUP_ENABLED_CONFIG
  // in the schema-registry repo (cross-repo contract).
  // User sets: value.converter.schema.backup.enabled=true
  public static final String SCHEMA_BACKUP_ENABLED_CONFIG = "schema.backup.enabled";

  // Schema metadata directory structure
  public static final String METADATA_DIR = "_metadata";
  public static final String SCHEMAS_DIR = "schemas";
  public static final String ENTRY_FILE_SUFFIX = ".entry.json";

  // Schema file extensions by type
  public static final String EXT_AVRO = ".avsc";
  public static final String EXT_PROTOBUF = ".proto";
  public static final String EXT_JSON = ".json";
  public static final String EXT_DEFAULT = ".schema";

  // Reference tree JSON field names (cross-repo contract).
  // Must match BackupWrapper.REF_FIELD_* in schema-converter.
  public static final String REF_FIELD_SUBJECT = "subject";
  public static final String REF_FIELD_VERSION = "version";
  public static final String REF_FIELD_GLOBAL_ID = "globalId";
  public static final String REF_FIELD_SCHEMA_TYPE = "schemaType";
  public static final String REF_FIELD_SCHEMA = "schema";
  public static final String REF_FIELD_REFERENCES = "references";
  public static final String REF_FIELD_NAME = "name";

  // Converter config key constants
  public static final String KEY_CONVERTER_CONFIG = "key.converter";
  public static final String VALUE_CONVERTER_CONFIG = "value.converter";

  // Schema type constants
  // TYPE_JSON is the SR-native type name; TYPE_JSON_SCHEMA is the converter-reported name.
  // Both refer to JSON Schema — SR stores as "JSON", converters report as "JSON_SCHEMA".
  public static final String TYPE_AVRO = "AVRO";
  public static final String TYPE_PROTOBUF = "PROTOBUF";
  public static final String TYPE_JSON = "JSON";
  public static final String TYPE_JSON_SCHEMA = "JSON_SCHEMA";
  public static final String TYPE_JSON_SCHEMALESS = "JSON_SCHEMALESS";
  public static final String TYPE_JSON_EMBEDDED_SCHEMA = "JSON_EMBEDDED_SCHEMA";
  public static final String TYPE_STRING = "STRING";
  public static final String TYPE_INT16 = "INT16";
  public static final String TYPE_INT32 = "INT32";
  public static final String TYPE_INT64 = "INT64";
  public static final String TYPE_FLOAT32 = "FLOAT32";
  public static final String TYPE_FLOAT64 = "FLOAT64";
  public static final String TYPE_BYTES = "BYTES";
  public static final String TYPE_NONE = "NONE";
  public static final String TYPE_UNKNOWN = "UNKNOWN";

  /**
   * Returns whether a schema type is Schema Registry-backed
   * (requires schema metadata for pristine restore).
   */
  public static boolean isSrBackedType(String schemaType) {
    return TYPE_AVRO.equals(schemaType)
        || TYPE_PROTOBUF.equals(schemaType)
        || TYPE_JSON_SCHEMA.equals(schemaType)
        || TYPE_JSON.equals(schemaType);
  }

  /**
   * Returns the file extension for a schema type.
   */
  public static String extensionForType(String schemaType) {
    if (TYPE_AVRO.equals(schemaType)) {
      return EXT_AVRO;
    }
    if (TYPE_PROTOBUF.equals(schemaType)) {
      return EXT_PROTOBUF;
    }
    if (TYPE_JSON_SCHEMA.equals(schemaType) || TYPE_JSON.equals(schemaType)) {
      return EXT_JSON;
    }
    return EXT_DEFAULT;
  }

  private BackupEnvelope() {
  }
}
