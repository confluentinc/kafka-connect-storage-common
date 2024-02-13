/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.connect.storage.schema;

import static io.confluent.connect.storage.schema.SchemaIncompatibilityType.DIFFERENT_NAME;
import static io.confluent.connect.storage.schema.SchemaIncompatibilityType.DIFFERENT_PARAMS;
import static io.confluent.connect.storage.schema.SchemaIncompatibilityType.DIFFERENT_SCHEMA;
import static io.confluent.connect.storage.schema.SchemaIncompatibilityType.DIFFERENT_TYPE;
import static io.confluent.connect.storage.schema.SchemaIncompatibilityType.DIFFERENT_VERSION;
import static io.confluent.connect.storage.schema.SchemaIncompatibilityType.NA;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaProjector;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Set;

public enum StorageSchemaCompatibility implements SchemaCompatibility {
  NONE {
    @Override
    public SourceRecord project(
        SourceRecord record,
        Schema currentKeySchema,
        Schema currentValueSchema
    ) {
      return record;
    }

    @Override
    public SinkRecord project(
        SinkRecord record,
        Schema currentKeySchema,
        Schema currentValueSchema
    ) {
      return record;
    }

    /**
     * Check whether the two schemas are incompatible such that they would prevent successfully
     * {@link #project projecting} a key or value with the original schema into the current schema.
     *
     * <p>This method currently considers schemas to be compatible if and only they are
     * {@link Schema#equals(Object) equal}. Otherwise, the schemas are deemed incompatible.
     *
     * @param originalSchema the original (new) schema; may not be null
     * @param currentSchema  the current schema; may not be null
     * @return CompatibilityResult: true if the schemas are not equal and therefore incompatible,
     *         or false if they are equal and therefore compatible
     */
    @Override
    protected SchemaCompatibilityResult check(
        Schema originalSchema,
        Schema currentSchema
    ) {
      boolean isInCompatible = !originalSchema.equals(currentSchema);
      SchemaIncompatibilityType schemaIncompatibilityType = isInCompatible ? DIFFERENT_SCHEMA : NA;
      return new SchemaCompatibilityResult(isInCompatible, schemaIncompatibilityType);
    }
  },
  BACKWARD,
  FORWARD {
    /**
     * Check whether the two schemas have incompatible versions that prevent successfully
     * {@link #project projecting} a key or value with the original schema into the current schema.
     *
     * <p>This method will consider schemas compatible for projection if the original schema's
     * {@link Schema#version() version} is greater than or equal to the current schema's version.
     *
     * @param originalSchema the original (new) schema; may not be null
     * @param currentSchema  the current schema; may not be null
     * @return CompatibilityResult true if the schema are not compatible for projection,
     *         or false if they are compatible
     */
    @Override
    protected SchemaCompatibilityResult checkVersions(
        Schema originalSchema,
        Schema currentSchema
    ) {
      boolean isInCompatible = (originalSchema.version()).compareTo(currentSchema.version()) < 0;
      SchemaIncompatibilityType schemaIncompatibilityType = isInCompatible ? DIFFERENT_VERSION : NA;
      return new SchemaCompatibilityResult(isInCompatible, schemaIncompatibilityType);
    }
  },
  FULL;

  private static final Map<String, StorageSchemaCompatibility> REVERSE = new HashMap<>();
  private static final Set<SimpleImmutableEntry<Schema.Type, Schema.Type>> PROMOTABLES;

  static {
    for (StorageSchemaCompatibility compat : values()) {
      REVERSE.put(compat.name(), compat);
    }

    Schema.Type[] promotableTypes = new Schema.Type[]{
        Schema.Type.INT8,
        Schema.Type.INT16,
        Schema.Type.INT32,
        Schema.Type.INT64,
        Schema.Type.FLOAT32,
        Schema.Type.FLOAT64
    };
    Set<SimpleImmutableEntry<Schema.Type, Schema.Type>> entries = new HashSet<>();
    for (int i = 0; i < promotableTypes.length; ++i) {
      for (int j = i; j < promotableTypes.length; ++j) {
        entries.add(new AbstractMap.SimpleImmutableEntry<>(promotableTypes[i], promotableTypes[j]));
      }
    }
    PROMOTABLES = Collections.unmodifiableSet(entries);
  }

  public static StorageSchemaCompatibility getCompatibility(String name) {
    StorageSchemaCompatibility compat = REVERSE.get(name);
    return compat != null ? compat : StorageSchemaCompatibility.NONE;
  }

  /**
   * Check whether the two schemas are incompatible such that a value using the supplied value
   * schema could not be successfully {@link #project projected} to the specified current schema.
   *
   * @param valueSchema   the schema of the value to be projected; may not be null
   * @param currentSchema the current schema; may not be null
   * @return CompatibilityResult: true if the schemas are not compatible for projection,
   *         or false if they are compatible
   */
  protected SchemaCompatibilityResult validateAndCheck(
      Schema valueSchema,
      Schema currentSchema
  ) {
    if (currentSchema == null && valueSchema == null) {
      return new SchemaCompatibilityResult(false, NA);
    }
    if (currentSchema == valueSchema) {
      return new SchemaCompatibilityResult(false, NA);
    }

    if (currentSchema == null || valueSchema == null) {
      // Change between schema-based and schema-less or vice versa throws exception.
      throw new SchemaProjectorException(
          "Switch between schema-based and schema-less data is not supported"
      );
    }

    if ((valueSchema.version() == null || currentSchema.version() == null) && this != NONE) {
      throw new SchemaProjectorException(
          "Schema version required for " + toString() + " compatibility"
      );
    }
    return check(valueSchema, currentSchema);
  }

  /**
   * Check whether the two schemas are incompatible such that they would prevent successfully
   * {@link #project projecting} a key or value with the original schema into the current schema.
   *
   * <p>This method currently considers schemas to be incompatible for projection when any of
   * the following are true:
   * <ol>
   *   <li>The {@link Schema#type() Schema types} are different, per
   *       {@link #checkSchemaTypes(Schema, Schema)}</li>
   *   <li>The {@link Schema#name() Schema names} are different, per
   *       {@link #checkSchemaNames(Schema, Schema)}</li>
   *   <li>The {@link Schema#parameters() Schema parameters} are different, per
   *       {@link #checkSchemaParameters(Schema, Schema)}</li>
   *   <li>The {@link Schema#version() Schema versions} don't allow projection, per
   *       {@link #checkVersions(Schema, Schema)}</li>
   * </ol>
   * In all of these cases, a record key or value using the original schema will not be able to
   * be projected to the current schema.
   *
   * <p>Any of the `check*` methods can be overridden to change the individual behavior,
   * or this method can be overridden to alter the logic described above.</p>
   *
   * @param originalSchema the original (new) schema; may not be null
   * @param currentSchema  the current schema; may not be null
   * @return SchemaCompatibilityResult: true if the schemas are not compatible for projection,
   *         or false if they are compatible
   */
  protected SchemaCompatibilityResult check(
      Schema originalSchema,
      Schema currentSchema
  ) {
    if (checkSchemaTypes(originalSchema, currentSchema)) {
      return new SchemaCompatibilityResult(true, DIFFERENT_TYPE);
    }
    if (checkSchemaNames(originalSchema, currentSchema)) {
      return new SchemaCompatibilityResult(true, DIFFERENT_NAME);
    }
    if (checkSchemaParameters(originalSchema, currentSchema)) {
      return new SchemaCompatibilityResult(true, DIFFERENT_PARAMS);
    }

    return checkVersions(originalSchema, currentSchema);
  }

  /**
   * Check whether the two schemas have incompatible versions that prevent successfully
   * {@link #project projecting} a key or value with the original schema into the current schema.
   *
   * <p>This method will consider schemas compatible for projection if the original schema's
   * {@link Schema#version() version} is less than or equal to the current schema's version.
   * IOW, if the original schema has a <em>newer</em> version than the current schema, the schemas
   * will be incompatible.
   *
   * @param originalSchema the original (new) schema; may not be null
   * @param currentSchema  the current schema; may not be null
   * @return SchemaCompatibilityResult: true if the schema versions are not compatible for
   *         projection, or false if they are compatible
   */
  protected SchemaCompatibilityResult checkVersions(
      Schema originalSchema,
      Schema currentSchema
  ) {
    boolean isInCompatible = originalSchema.version().compareTo(currentSchema.version()) > 0;
    SchemaIncompatibilityType schemaIncompatibilityType = isInCompatible ? DIFFERENT_VERSION : NA;
    return new SchemaCompatibilityResult(isInCompatible, schemaIncompatibilityType);
  }

  /**
   * Check whether the two schemas have incompatible schema types that prevent successfully
   * {@link #project projecting} a key or value with the original schema into the current schema.
   *
   * @param originalSchema the original (new) schema; may not be null
   * @param currentSchema  the current schema; may not be null
   * @return true if the schema types are not compatible for projection, or false if they are
   *         compatible
   */
  protected boolean checkSchemaTypes(
      Schema originalSchema,
      Schema currentSchema
  ) {
    return originalSchema.type() != currentSchema.type()
        && !isPromotable(originalSchema.type(), currentSchema.type());
  }

  /**
   * Check whether the two schemas have incompatible names that prevent successfully
   * {@link #project projecting} a key or value with the original schema into the current schema.
   *
   * @param originalSchema the original (new) schema; may not be null
   * @param currentSchema  the current schema; may not be null
   * @return true if the schema names are not compatible for projection, or false if they are
   *         compatible
   */
  protected boolean checkSchemaNames(
      Schema originalSchema,
      Schema currentSchema
  ) {
    return !Objects.equals(originalSchema.name(), currentSchema.name());
  }

  /**
   * Check whether the two schemas have incompatible {@link Schema#parameters() parameters} that
   * prevent successfully {@link #project projecting} a key or value with the original schema
   * into the current schema.
   *
   * @param originalSchema the original (new) schema; may not be null
   * @param currentSchema  the current schema; may not be null
   * @return true if the schema parameters are not compatible for projection, or false if they are
   *         compatible
   */
  protected boolean checkSchemaParameters(
      Schema originalSchema,
      Schema currentSchema
  ) {
    return !Objects.equals(originalSchema.parameters(), currentSchema.parameters());
  }

  protected boolean isPromotable(Schema.Type sourceType, Schema.Type targetType) {
    return PROMOTABLES.contains(new AbstractMap.SimpleImmutableEntry<>(sourceType, targetType));
  }

  /**
   * Determine whether the current key and value schemas should be changed given the current
   * record.
   *
   * <p>This method will return false if the supplied record could be projected to the current
   * schemas using {@link #project(SinkRecord, Schema, Schema)} or
   * {@link #project(SourceRecord, Schema, Schema)}. Those methods currently require the
   * following to be true when comparing the current key schema and record key schema (if
   * provided) and when comparing the current value schema and record value schema
   * (and recursively for any contained schemas within fields), which means this method returns
   * true if any of these conditions are true.
   *
   * <ul>
   *   <li>The {@link Schema#type() schema types} are equivalent or can be promoted (e.g., are
   *   both numeric types).</li>
   *   <li>The {@link Schema#name() schema names} are equivalent.</li>
   *   <li>The {@link Schema#parameters() schema parameters} are equivalent.</li>
   *   <li>The current schema has a default if it is not {@link Schema#isOptional() optional}
   *       but the record schema is optional.</li>
   *   <li>For {@link Schema.Type#STRUCT} schemas, each field in the current schema either
   *   corresponds to an existing field in the record schema, or that field in the current schema
   *   is optional or has a default value.</li>
   * </ul>
   *
   * @param record             the next record; may not be null
   * @param currentKeySchema   the current key schema; may be null
   * @param currentValueSchema the current value schema; may be null
   * @return CompatibilityResult: true if the key or value schema in the supplied record has changed
   *         such that it cannot be projected to the current key schema and value schema, or false
   *         if the record can be projected
   */
  public SchemaCompatibilityResult shouldChangeSchema(
      ConnectRecord<?> record,
      Schema currentKeySchema,
      Schema currentValueSchema
  ) {
    // Currently in Storage only value schemas are considered for compatibility resolution.
    return validateAndCheck(record.valueSchema(), currentValueSchema);
  }

  public SourceRecord project(
      SourceRecord record,
      Schema currentKeySchema,
      Schema currentValueSchema
  ) {
    Map.Entry<Object, Object> projected = projectInternal(
        record,
        currentKeySchema,
        currentValueSchema
    );

    // Just reference comparison here.
    return projected.getKey() == record.key() && projected.getValue() == record.value()
           ? record
           : new SourceRecord(
               record.sourcePartition(),
               record.sourceOffset(),
               record.topic(),
               record.kafkaPartition(),
               currentKeySchema,
               projected.getKey(),
               currentValueSchema,
               projected.getValue(),
               record.timestamp()
           );
  }

  public SinkRecord project(
      SinkRecord record,
      Schema currentKeySchema,
      Schema currentValueSchema
  ) {
    Map.Entry<Object, Object> projected = projectInternal(
        record,
        currentKeySchema,
        currentValueSchema
    );

    // Just reference comparison here.
    if (projected.getKey() == record.key() && projected.getValue() == record.value()) {
      return record;
    }

  return record.newRecord(
      record.topic(),
      record.kafkaPartition(),
      currentKeySchema,
      projected.getKey(),
      currentValueSchema,
      projected.getValue(),
      record.timestamp());
  }

  private static Map.Entry<Object, Object> projectInternal(
      ConnectRecord<?> record,
      Schema currentKeySchema,
      Schema currentValueSchema
  ) {
    // Currently in Storage only value schemas are considered for compatibility resolution.
    Object value = projectInternal(record.valueSchema(), record.value(), currentValueSchema);
    return new AbstractMap.SimpleEntry<>(record.key(), value);
  }

  private static Object projectInternal(
      Schema originalSchema,
      Object value,
      Schema currentSchema
  ) {
    if (Objects.equals(originalSchema, currentSchema)) {
      return value;
    }
    return SchemaProjector.project(originalSchema, value, currentSchema);
  }
}
