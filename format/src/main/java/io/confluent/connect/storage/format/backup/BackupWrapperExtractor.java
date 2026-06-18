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

package io.confluent.connect.storage.format.backup;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.schema.backup.BackupWrapper;
import io.confluent.connect.storage.backup.BackupEnvelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.util.List;
import java.util.Map;

/**
 * Extracts data and metadata from a backup Wrapper struct.
 * Handles SR-wrapped, tombstone, schemaless JSON, and non-SR record types.
 */
public final class BackupWrapperExtractor {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private BackupWrapperExtractor() {
  }

  /**
   * Unwrap a key or value from a SinkRecord, extracting backup metadata
   * if the data is wrapped in a Wrapper struct.
   */
  public static Unwrapped unwrap(
      Object data, Schema schema, boolean isKey, String schemaTypeDefault) {
    if (BackupWrapper.isWrapper(schema) && data instanceof Struct) {
      return unwrapFromWrapper((Struct) data, schema);
    }
    if (schema == null && data == null) {
      return Unwrapped.tombstone();
    }
    if (schema == null) {
      return unwrapSchemaless(data);
    }
    return Unwrapped.passthrough(data, schema, schemaTypeDefault);
  }

  private static Unwrapped unwrapFromWrapper(Struct w, Schema schema) {
    if (schema == null) {
      throw new DataException("Wrapper schema is null — cannot unwrap backup metadata");
    }
    if (schema.field(BackupWrapper.FIELD_DATA) == null) {
      throw new DataException("Wrapper schema missing 'data' field — corrupt Wrapper struct");
    }
    Integer schemaVersion = optionalInt32(w, schema, BackupWrapper.FIELD_SCHEMA_VERSION);
    String referenceTreeJson = optionalString(w, schema, BackupWrapper.FIELD_REFERENCE_TREE);
    String directRefsJson = optionalString(w, schema, BackupWrapper.FIELD_DIRECT_REFS);
    return new Unwrapped(
        w.get(BackupWrapper.FIELD_DATA),
        schema.field(BackupWrapper.FIELD_DATA).schema(),
        w.getInt32(BackupWrapper.FIELD_SCHEMA_ID),
        schemaVersion,
        w.getString(BackupWrapper.FIELD_SCHEMA_TYPE),
        w.getString(BackupWrapper.FIELD_SCHEMA_SUBJECT),
        w.getString(BackupWrapper.FIELD_RAW_SCHEMA),
        referenceTreeJson,
        directRefsJson);
  }

  private static Unwrapped unwrapSchemaless(Object data) {
    String stringData;
    if (data instanceof Map || data instanceof List) {
      try {
        stringData = OBJECT_MAPPER.writeValueAsString(data);
      } catch (JsonProcessingException e) {
        throw new DataException("Failed to serialize schemaless data as JSON", e);
      }
    } else {
      stringData = data != null ? data.toString() : null;
    }
    return new Unwrapped(stringData, null, null, null,
        BackupEnvelope.TYPE_JSON_SCHEMALESS, null, null, null, null);
  }

  private static Integer optionalInt32(Struct s, Schema schema, String field) {
    return schema.field(field) != null ? s.getInt32(field) : null;
  }

  private static String optionalString(Struct s, Schema schema, String field) {
    return schema.field(field) != null ? s.getString(field) : null;
  }

  /**
   * Result of unwrapping a Wrapper struct, containing all backup metadata.
   */
  public static class Unwrapped {
    private final Object data;
    private final Schema schema;
    private final Integer schemaId;
    private final Integer schemaVersion;
    private final String schemaType;
    private final String subject;
    private final String rawSchema;
    private final String referenceTreeJson;
    private final String directRefsJson;

    Unwrapped(Object data, Schema schema,
        Integer schemaId, Integer schemaVersion,
        String schemaType, String subject, String rawSchema,
        String referenceTreeJson, String directRefsJson) {
      this.data = data;
      this.schema = schema;
      this.schemaId = schemaId;
      this.schemaVersion = schemaVersion;
      this.schemaType = schemaType;
      this.subject = subject;
      this.rawSchema = rawSchema;
      this.referenceTreeJson = referenceTreeJson;
      this.directRefsJson = directRefsJson;
    }

    static Unwrapped tombstone() {
      return new Unwrapped(null, null, null, null,
          BackupEnvelope.TYPE_NONE, null, null, null, null);
    }

    static Unwrapped passthrough(Object data, Schema schema, String schemaType) {
      return new Unwrapped(data, schema, null, null,
          schemaType, null, null, null, null);
    }

    public Object getData() {
      return data;
    }

    public Schema getSchema() {
      return schema;
    }

    public Integer getSchemaId() {
      return schemaId;
    }

    public Integer getSchemaVersion() {
      return schemaVersion;
    }

    public String getSchemaType() {
      return schemaType;
    }

    public String getSubject() {
      return subject;
    }

    public String getRawSchema() {
      return rawSchema;
    }

    public String getReferenceTreeJson() {
      return referenceTreeJson;
    }

    public String getDirectRefsJson() {
      return directRefsJson;
    }
  }
}
