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

import java.util.HashMap;
import java.util.Map;

public enum StorageSchemaCompatibility implements SchemaCompatibility {
  NONE {
    public SinkRecord project(SinkRecord record, Schema currentSchema) {
      return record;
    }
    @Override
    protected boolean check(Schema valueSchema, Schema currentSchema) {
      return !valueSchema.equals(currentSchema);
    }
  },
  BACKWARD,
  FORWARD {
    @Override
    protected boolean check(Schema valueSchema, Schema currentSchema) {
      return (valueSchema.version()).compareTo(currentSchema.version()) < 0;
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

  protected boolean validate(Schema valueSchema, Schema currentSchema) {
    if (currentSchema == null) {
      return true;
    }
    if ((valueSchema.version() == null || currentSchema.version() == null) && this != NONE) {
      throw new SchemaProjectorException("Schema version required for " + toString() + " compatibility");
    }
    return false;
  }

  protected boolean check(Schema valueSchema, Schema currentSchema) {
    return (valueSchema.version()).compareTo(currentSchema.version()) > 0;
  }

  public boolean shouldChangeSchema(Schema valueSchema, Schema currentSchema) {
    return validate(valueSchema, currentSchema) || check(valueSchema, currentSchema);
  }

  public SinkRecord project(SinkRecord record, Schema currentSchema) {
    Object projected = projectInternal(record, currentSchema);

    // Just reference comparison.
    return projected == record ?
        record :
        new SinkRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), currentSchema,
            projected, record.kafkaOffset());
  }

  public SourceRecord project(SourceRecord record, Schema currentSchema) {
    Object projected = projectInternal(record, currentSchema);

    // Just reference comparison.
    return projected == record ?
        record :
        new SourceRecord(record.sourcePartition(), record.sourceOffset(), record.topic(), record.kafkaPartition(),
            record.keySchema(), record.key(), currentSchema, projected, record.timestamp());
  }

  private Object projectInternal(ConnectRecord<?> record, Schema currentSchema) {
    Schema originalSchema = record.valueSchema();
    Object value = record.value();

    if (originalSchema == currentSchema || originalSchema.equals(currentSchema)) {
      return record;
    }
    return SchemaProjector.project(originalSchema, value, currentSchema);
  }
}
