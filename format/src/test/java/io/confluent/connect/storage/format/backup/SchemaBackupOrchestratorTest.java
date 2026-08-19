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

import io.confluent.connect.schema.backup.api.BackupWrapper;
import io.confluent.connect.storage.backup.BackupEnvelope;
import io.confluent.connect.storage.backup.SchemaBackupStore;
import io.confluent.connect.storage.format.backup.BackupWrapperExtractor.Unwrapped;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class SchemaBackupOrchestratorTest {

  private static final String TOPIC = "test-topic";
  private static final String SUBJECT = "test-value";
  private static final String FIELD_NAME = "name";
  private static final String FIELD_VALUE = "Alice";
  private static final String RAW_SCHEMA = "{\"type\":\"record\",\"name\":\"User\","
      + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";
  private static final String SCHEMA_GUID = "550e8400-e29b-41d4-a716-446655440000";
  private static final Schema DATA_SCHEMA = SchemaBuilder.struct()
      .field(FIELD_NAME, Schema.STRING_SCHEMA).build();

  private SchemaBackupStore backupStore;
  private SchemaBackupOrchestrator orchestrator;

  @Before
  public void setUp() {
    backupStore = mock(SchemaBackupStore.class);
    orchestrator = new SchemaBackupOrchestrator(backupStore);
  }

  @Test
  public void backupSkippedWhenRawSchemaNull() {
    Unwrapped unwrapped = BackupWrapperExtractor.unwrap(
        null, null, false, BackupEnvelope.TYPE_AVRO);

    orchestrator.backupIfNeeded(TOPIC, unwrapped);

    verify(backupStore, never()).backupIfNeeded(
        anyString(), anyString(), anyInt(), anyString(),
        anyString(), anyString(), anyList());
  }

  @Test
  public void backupSkippedWhenNoSchemaIdOrGuid() {
    Unwrapped unwrapped = unwrapWith(new BackupWrapper.WrapperFields(
        null, 1, BackupEnvelope.TYPE_AVRO, SUBJECT, RAW_SCHEMA, null, null));

    orchestrator.backupIfNeeded(TOPIC, unwrapped);

    verify(backupStore, never()).backupIfNeeded(
        anyString(), anyString(), anyInt(), anyString(),
        anyString(), anyString(), anyList());
  }

  @Test
  public void backupCalledWithSchemaId() {
    Unwrapped unwrapped = createUnwrappedWithId(42);

    orchestrator.backupIfNeeded(TOPIC, unwrapped);

    verify(backupStore).backupIfNeeded(
        eq(TOPIC), eq("42"), eq(1), eq(BackupEnvelope.TYPE_AVRO),
        eq(SUBJECT), eq(RAW_SCHEMA), anyList());
  }

  @Test
  public void backupCalledWithSchemaGuid() {
    Unwrapped unwrapped = unwrapWith(new BackupWrapper.WrapperFields(
        null, 1, BackupEnvelope.TYPE_AVRO, SUBJECT, RAW_SCHEMA,
        null, null, SCHEMA_GUID));

    orchestrator.backupIfNeeded(TOPIC, unwrapped);

    verify(backupStore).backupIfNeeded(
        eq(TOPIC), eq(SCHEMA_GUID), eq(1), eq(BackupEnvelope.TYPE_AVRO),
        eq(SUBJECT), eq(RAW_SCHEMA), anyList());
  }

  @Test
  public void backupDefaultsVersionToZeroWhenNull() {
    Unwrapped unwrapped = unwrapWith(new BackupWrapper.WrapperFields(
        42, null, BackupEnvelope.TYPE_AVRO, SUBJECT, RAW_SCHEMA, null, null));

    orchestrator.backupIfNeeded(TOPIC, unwrapped);

    verify(backupStore).backupIfNeeded(
        eq(TOPIC), eq("42"), eq(0), eq(BackupEnvelope.TYPE_AVRO),
        eq(SUBJECT), eq(RAW_SCHEMA), anyList());
  }

  @Test
  public void backupIncludesSubjectAndType() {
    Unwrapped unwrapped = createUnwrappedWithId(10);

    orchestrator.backupIfNeeded(TOPIC, unwrapped);

    verify(backupStore).backupIfNeeded(
        eq(TOPIC), anyString(), anyInt(),
        eq(BackupEnvelope.TYPE_AVRO), eq(SUBJECT), anyString(), anyList());
  }

  @Test
  public void backupCalledWithNoReferences() {
    Unwrapped unwrapped = createUnwrappedWithId(42);

    orchestrator.backupIfNeeded(TOPIC, unwrapped);

    verify(backupStore).backupIfNeeded(
        eq(TOPIC), eq("42"), anyInt(), anyString(),
        anyString(), anyString(), anyList());
  }

  @Test
  public void backupHandlesReferenceTree() {
    String refTree = "{"
        + "\"Country\":{\"subject\":\"Country\",\"version\":1,"
        + "\"globalId\":10,\"schemaType\":\"AVRO\","
        + "\"schema\":\"{}\",\"references\":[]}"
        + "}";
    String directRefs = "[{\"name\":\"Country\","
        + "\"subject\":\"Country\",\"version\":1}]";

    Unwrapped unwrapped = unwrapWith(new BackupWrapper.WrapperFields(
        42, 1, BackupEnvelope.TYPE_AVRO, SUBJECT, RAW_SCHEMA, refTree, directRefs));

    orchestrator.backupIfNeeded(TOPIC, unwrapped);

    verify(backupStore).backupIfNeeded(
        eq(TOPIC), eq("10"), eq(1), eq(BackupEnvelope.TYPE_AVRO),
        eq("Country"), eq("{}"), anyList());
    verify(backupStore).backupIfNeeded(
        eq(TOPIC), eq("42"), eq(1), eq(BackupEnvelope.TYPE_AVRO),
        eq(SUBJECT), eq(RAW_SCHEMA), anyList());
  }

  @Test(expected = DataException.class)
  public void backupThrowsOnMissingSchemaInRef() {
    String refTree = "{"
        + "\"BadRef\":{\"subject\":\"Bad\",\"version\":1,"
        + "\"globalId\":99,\"schemaType\":\"AVRO\"}"
        + "}";

    Unwrapped unwrapped = unwrapWith(new BackupWrapper.WrapperFields(
        42, 1, BackupEnvelope.TYPE_AVRO, SUBJECT, RAW_SCHEMA, refTree, null));

    orchestrator.backupIfNeeded(TOPIC, unwrapped);
  }

  @Test(expected = DataException.class)
  public void backupThrowsOnInvalidGlobalIdInRef() {
    String refTree = "{"
        + "\"BadRef\":{\"subject\":\"Bad\",\"version\":1,"
        + "\"globalId\":0,\"schemaType\":\"AVRO\",\"schema\":\"{}\"}"
        + "}";

    Unwrapped unwrapped = unwrapWith(new BackupWrapper.WrapperFields(
        42, 1, BackupEnvelope.TYPE_AVRO, SUBJECT, RAW_SCHEMA, refTree, null));

    orchestrator.backupIfNeeded(TOPIC, unwrapped);
  }

  private Unwrapped createUnwrappedWithId(int schemaId) {
    return unwrapWith(new BackupWrapper.WrapperFields(
        schemaId, 1, BackupEnvelope.TYPE_AVRO, SUBJECT, RAW_SCHEMA, null, null));
  }

  private Unwrapped unwrapWith(BackupWrapper.WrapperFields fields) {
    Schema wrapperSchema = BackupWrapper.buildSchema(DATA_SCHEMA);
    Struct data = new Struct(DATA_SCHEMA).put(FIELD_NAME, FIELD_VALUE);
    Struct wrapper = BackupWrapper.buildWrapper(wrapperSchema, data, fields);
    return BackupWrapperExtractor.unwrap(
        wrapper, wrapperSchema, false, BackupEnvelope.TYPE_AVRO);
  }
}
