/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.storage.schema;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaProjector;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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

    @Override
    protected boolean check(
        Schema originalSchema,
        Schema currentSchema
    ) {
      return !originalSchema.equals(currentSchema);
    }
  },
  BACKWARD,
  FORWARD {
    @Override
    protected boolean check(
        Schema originalSchema,
        Schema currentSchema
    ) {
      return (originalSchema.version()).compareTo(currentSchema.version()) < 0;
    }
  },
  FULL;

  private static final Map<String, StorageSchemaCompatibility> REVERSE = new HashMap<>();

  static {
    for (StorageSchemaCompatibility compat : values()) {
      REVERSE.put(compat.name(), compat);
    }
  }

  public static StorageSchemaCompatibility getCompatibility(String name) {
    StorageSchemaCompatibility compat = REVERSE.get(name);
    return compat != null ? compat : StorageSchemaCompatibility.NONE;
  }

  protected boolean validateAndCheck(
      Schema valueSchema,
      Schema currentSchema
  ) {
    if (currentSchema == null && valueSchema == null) {
      return false;
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

  protected boolean check(
      Schema originalSchema,
      Schema currentSchema
  ) {
    return originalSchema.version().compareTo(currentSchema.version()) > 0;
  }

  public boolean shouldChangeSchema(
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
    return projected.getKey() == record.key() && projected.getValue() == record.value()
           ? record
           : new SinkRecord(
               record.topic(),
               record.kafkaPartition(),
               currentKeySchema,
               projected.getKey(),
               currentValueSchema,
               projected.getValue(),
               record.kafkaOffset(),
               record.timestamp(),
               record.timestampType()
           );
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
