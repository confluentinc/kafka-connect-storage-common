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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;

public final class EnvelopeSchemaBuilder {

  private EnvelopeSchemaBuilder() {
  }

  public static final Schema HEADER_SCHEMA =
      SchemaBuilder.struct()
          .name("io.confluent.connect.storage.Header")
          .field(BackupEnvelope.FIELD_HEADER_KEY, Schema.STRING_SCHEMA)
          .field(BackupEnvelope.FIELD_HEADER_VALUE, Schema.OPTIONAL_STRING_SCHEMA)
          .field(BackupEnvelope.FIELD_HEADER_SCHEMA_TYPE, Schema.OPTIONAL_STRING_SCHEMA)
          .build();

  public static final Schema HEADERS_ARRAY_SCHEMA =
      SchemaBuilder.array(HEADER_SCHEMA).optional().build();

  public static Schema buildEnvelopeSchema(
      Schema keySchema, Schema valueSchema,
      String keySchemaType, String valueSchemaType) {

    SchemaBuilder b = SchemaBuilder.struct().name(BackupEnvelope.NAME);
    b.field(BackupEnvelope.FIELD_KEY,
        keySchema != null ? keySchema : Schema.OPTIONAL_STRING_SCHEMA);
    b.field(BackupEnvelope.FIELD_VALUE,
        valueSchema != null ? valueSchema : Schema.OPTIONAL_STRING_SCHEMA);
    b.field(BackupEnvelope.FIELD_HEADERS, HEADERS_ARRAY_SCHEMA);
    b.field(BackupEnvelope.FIELD_TOPIC, Schema.STRING_SCHEMA);
    b.field(BackupEnvelope.FIELD_PARTITION, Schema.INT32_SCHEMA);
    b.field(BackupEnvelope.FIELD_OFFSET, Schema.INT64_SCHEMA);
    b.field(BackupEnvelope.FIELD_TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA);
    b.field(BackupEnvelope.FIELD_TIMESTAMP_TYPE, Schema.OPTIONAL_STRING_SCHEMA);
    b.field(BackupEnvelope.FIELD_KEY_SCHEMA_ID, Schema.OPTIONAL_INT32_SCHEMA);
    b.field(BackupEnvelope.FIELD_VALUE_SCHEMA_ID, Schema.OPTIONAL_INT32_SCHEMA);
    b.field(BackupEnvelope.FIELD_KEY_SCHEMA_TYPE, Schema.STRING_SCHEMA);
    b.field(BackupEnvelope.FIELD_VALUE_SCHEMA_TYPE, Schema.STRING_SCHEMA);
    b.field(BackupEnvelope.FIELD_KEY_SCHEMA_SUBJECT, Schema.OPTIONAL_STRING_SCHEMA);
    b.field(BackupEnvelope.FIELD_VALUE_SCHEMA_SUBJECT, Schema.OPTIONAL_STRING_SCHEMA);
    return b.build();
  }

  public static Struct buildEnvelopeStruct(
      Schema envelopeSchema, SinkRecord record,
      Object keyData, Object valueData,
      Integer keySchemaId, Integer valueSchemaId,
      String keySchemaType, String valueSchemaType,
      String keySubject, String valueSubject) {

    Struct e = new Struct(envelopeSchema);
    e.put(BackupEnvelope.FIELD_KEY, keyData);
    e.put(BackupEnvelope.FIELD_VALUE, valueData);
    e.put(BackupEnvelope.FIELD_HEADERS, buildHeaders(record));
    e.put(BackupEnvelope.FIELD_TOPIC, record.topic());
    e.put(BackupEnvelope.FIELD_PARTITION, record.kafkaPartition());
    e.put(BackupEnvelope.FIELD_OFFSET, record.kafkaOffset());
    e.put(BackupEnvelope.FIELD_TIMESTAMP, record.timestamp());
    e.put(BackupEnvelope.FIELD_TIMESTAMP_TYPE, record.timestampType() != null
        ? record.timestampType().name() : null);
    e.put(BackupEnvelope.FIELD_KEY_SCHEMA_ID, keySchemaId);
    e.put(BackupEnvelope.FIELD_VALUE_SCHEMA_ID, valueSchemaId);
    e.put(BackupEnvelope.FIELD_KEY_SCHEMA_TYPE, keySchemaType);
    e.put(BackupEnvelope.FIELD_VALUE_SCHEMA_TYPE, valueSchemaType);
    e.put(BackupEnvelope.FIELD_KEY_SCHEMA_SUBJECT, keySubject);
    e.put(BackupEnvelope.FIELD_VALUE_SCHEMA_SUBJECT, valueSubject);
    return e;
  }

  private static List<Struct> buildHeaders(SinkRecord record) {
    List<Struct> result = new ArrayList<>();
    if (record.headers() != null) {
      for (Header h : record.headers()) {
        Struct hs = new Struct(HEADER_SCHEMA);
        hs.put(BackupEnvelope.FIELD_HEADER_KEY, h.key());
        hs.put(BackupEnvelope.FIELD_HEADER_VALUE,
            h.value() != null
                ? Values.convertToString(h.schema(), h.value()) : null);
        hs.put(BackupEnvelope.FIELD_HEADER_SCHEMA_TYPE,
            h.schema() != null ? h.schema().type().name() : null);
        result.add(hs);
      }
    }
    return result;
  }
}
