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

import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.connect.schema.backup.api.SchemaBackupConfig;

/**
 * Constants for the KafkaRecordEnvelope struct.
 *
 * <p>Envelope-specific field names and storage metadata constants live here.
 * Schema type tags, reference field names, and backup config keys are
 * defined in {@link SchemaBackupConfig} (single source of truth) and
 * re-exported here for convenience.
 */
public final class BackupEnvelope {

  // Envelope struct schema name
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

  // Schema metadata directory structure
  public static final String METADATA_DIR = "_metadata";
  public static final String SCHEMAS_DIR = "schemas";
  public static final String ENTRY_FILE_SUFFIX = ".entry.json";

  // Schema file extensions by type
  public static final String EXT_AVRO = ".avsc";
  public static final String EXT_PROTOBUF = ".proto";
  public static final String EXT_JSON = ".json";
  public static final String EXT_DEFAULT = ".schema";

  // Converter config key constants
  public static final String KEY_CONVERTER_CONFIG = "key.converter";
  public static final String VALUE_CONVERTER_CONFIG = "value.converter";

  // Delegated to SchemaBackupConfig (single source of truth) — no duplication
  public static final String SCHEMA_BACKUP_ENABLED_CONFIG =
      SchemaBackupConfig.SCHEMA_BACKUP_ENABLED_CONFIG;

  public static final String REF_FIELD_SUBJECT = SchemaBackupConfig.REF_FIELD_SUBJECT;
  public static final String REF_FIELD_VERSION = SchemaBackupConfig.REF_FIELD_VERSION;
  public static final String REF_FIELD_GLOBAL_ID = SchemaBackupConfig.REF_FIELD_GLOBAL_ID;
  public static final String REF_FIELD_SCHEMA_TYPE = SchemaBackupConfig.REF_FIELD_SCHEMA_TYPE;
  public static final String REF_FIELD_SCHEMA = SchemaBackupConfig.REF_FIELD_SCHEMA;
  public static final String REF_FIELD_REFERENCES = SchemaBackupConfig.REF_FIELD_REFERENCES;
  public static final String REF_FIELD_NAME = SchemaBackupConfig.REF_FIELD_NAME;

  public static final String TYPE_AVRO = SchemaBackupConfig.TYPE_AVRO;
  public static final String TYPE_PROTOBUF = SchemaBackupConfig.TYPE_PROTOBUF;
  public static final String TYPE_JSON = SchemaBackupConfig.TYPE_JSON;
  public static final String TYPE_JSON_SCHEMA = SchemaBackupConfig.TYPE_JSON_SCHEMA;
  public static final String TYPE_JSON_SCHEMALESS = SchemaBackupConfig.TYPE_JSON_SCHEMALESS;
  public static final String TYPE_JSON_EMBEDDED_SCHEMA =
      SchemaBackupConfig.TYPE_JSON_EMBEDDED_SCHEMA;
  public static final String TYPE_STRING = SchemaBackupConfig.TYPE_STRING;
  public static final String TYPE_INT16 = SchemaBackupConfig.TYPE_INT16;
  public static final String TYPE_INT32 = SchemaBackupConfig.TYPE_INT32;
  public static final String TYPE_INT64 = SchemaBackupConfig.TYPE_INT64;
  public static final String TYPE_FLOAT32 = SchemaBackupConfig.TYPE_FLOAT32;
  public static final String TYPE_FLOAT64 = SchemaBackupConfig.TYPE_FLOAT64;
  public static final String TYPE_BYTES = SchemaBackupConfig.TYPE_BYTES;
  public static final String TYPE_NONE = SchemaBackupConfig.TYPE_NONE;
  public static final String TYPE_UNKNOWN = SchemaBackupConfig.TYPE_UNKNOWN;

  /**
   * Returns whether a schema type is Schema Registry-backed.
   */
  public static boolean isSrBackedType(String schemaType) {
    return SchemaBackupConfig.isSrBackedType(schemaType);
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
