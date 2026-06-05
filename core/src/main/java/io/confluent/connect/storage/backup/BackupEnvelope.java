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

  // Schema type constants
  public static final String TYPE_AVRO = "AVRO";
  public static final String TYPE_PROTOBUF = "PROTOBUF";
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

  private BackupEnvelope() {
  }
}
