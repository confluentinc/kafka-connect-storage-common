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
import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.connect.storage.backup.BackupEnvelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts data and metadata from a backup Wrapper struct.
 * Handles SR-wrapped, tombstone, schemaless JSON, and non-SR record types.
 */
public final class BackupWrapperExtractor {

  private static final Logger log =
      LoggerFactory.getLogger(BackupWrapperExtractor.class);
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

  private static Unwrapped unwrapFromWrapper(Struct wrapper, Schema schema) {
    if (schema == null) {
      throw new DataException("Wrapper schema is null — cannot unwrap backup metadata");
    }
    if (schema.field(BackupWrapper.FIELD_DATA) == null) {
      throw new DataException("Wrapper schema missing 'data' field — corrupt Wrapper struct");
    }
    Integer schemaId = optionalInt32(wrapper, schema, BackupWrapper.FIELD_SCHEMA_ID);
    Integer schemaVersion = optionalInt32(wrapper, schema, BackupWrapper.FIELD_SCHEMA_VERSION);
    String schemaType = optionalString(wrapper, schema, BackupWrapper.FIELD_SCHEMA_TYPE);
    String subject = optionalString(wrapper, schema, BackupWrapper.FIELD_SCHEMA_SUBJECT);
    String referenceTreeJson = optionalString(wrapper, schema, BackupWrapper.FIELD_REFERENCE_TREE);
    String directRefsJson = optionalString(wrapper, schema, BackupWrapper.FIELD_DIRECT_REFS);
    String schemaGuid = optionalString(wrapper, schema, BackupWrapper.FIELD_SCHEMA_GUID);
    log.debug("Unwrapped Wrapper: schemaType={}, schemaId={}, subject={}, hasReferences={}",
        schemaType, schemaId, subject, referenceTreeJson != null);
    return Unwrapped.builder()
        .data(wrapper.get(BackupWrapper.FIELD_DATA))
        .schema(schema.field(BackupWrapper.FIELD_DATA).schema())
        .schemaId(schemaId)
        .schemaVersion(schemaVersion)
        .schemaType(schemaType)
        .subject(subject)
        .rawSchema(optionalString(wrapper, schema, BackupWrapper.FIELD_RAW_SCHEMA))
        .referenceTreeJson(referenceTreeJson)
        .directRefsJson(directRefsJson)
        .schemaGuid(schemaGuid)
        .build();
  }

  private static Unwrapped unwrapSchemaless(Object data) {
    String stringData;
    if (data == null) {
      stringData = null;
    } else {
      try {
        stringData = OBJECT_MAPPER.writeValueAsString(data);
      } catch (JsonProcessingException e) {
        throw new DataException("Failed to serialize schemaless data as JSON", e);
      }
    }
    return Unwrapped.builder()
        .data(stringData)
        .schemaType(BackupEnvelope.TYPE_JSON_SCHEMALESS)
        .build();
  }

  private static Integer optionalInt32(Struct wrapper, Schema schema, String field) {
    return schema.field(field) != null ? wrapper.getInt32(field) : null;
  }

  private static String optionalString(Struct wrapper, Schema schema, String field) {
    return schema.field(field) != null ? wrapper.getString(field) : null;
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
    private final String schemaGuid;

    private Unwrapped(Builder b) {
      this.data = b.data;
      this.schema = b.schema;
      this.schemaId = b.schemaId;
      this.schemaVersion = b.schemaVersion;
      this.schemaType = b.schemaType;
      this.subject = b.subject;
      this.rawSchema = b.rawSchema;
      this.referenceTreeJson = b.referenceTreeJson;
      this.directRefsJson = b.directRefsJson;
      this.schemaGuid = b.schemaGuid;
    }

    static Builder builder() {
      return new Builder();
    }

    static Unwrapped tombstone() {
      return builder().schemaType(BackupEnvelope.TYPE_NONE).build();
    }

    static Unwrapped passthrough(Object data, Schema schema, String schemaType) {
      return builder().data(data).schema(schema).schemaType(schemaType).build();
    }

    static class Builder {
      private Object data;
      private Schema schema;
      private Integer schemaId;
      private Integer schemaVersion;
      private String schemaType;
      private String subject;
      private String rawSchema;
      private String referenceTreeJson;
      private String directRefsJson;
      private String schemaGuid;

      Builder data(Object v) {
        this.data = v;
        return this;
      }

      Builder schema(Schema v) {
        this.schema = v;
        return this;
      }

      Builder schemaId(Integer v) {
        this.schemaId = v;
        return this;
      }

      Builder schemaVersion(Integer v) {
        this.schemaVersion = v;
        return this;
      }

      Builder schemaType(String v) {
        this.schemaType = v;
        return this;
      }

      Builder subject(String v) {
        this.subject = v;
        return this;
      }

      Builder rawSchema(String v) {
        this.rawSchema = v;
        return this;
      }

      Builder referenceTreeJson(String v) {
        this.referenceTreeJson = v;
        return this;
      }

      Builder directRefsJson(String v) {
        this.directRefsJson = v;
        return this;
      }

      Builder schemaGuid(String v) {
        this.schemaGuid = v;
        return this;
      }

      Unwrapped build() {
        return new Unwrapped(this);
      }
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

    public String getSchemaGuid() {
      return schemaGuid;
    }
  }
}
