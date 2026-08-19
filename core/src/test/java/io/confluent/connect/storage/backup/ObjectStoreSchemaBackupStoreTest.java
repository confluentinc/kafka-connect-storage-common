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

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ObjectStoreSchemaBackupStoreTest {

  private static final String TOPIC = "orders";
  private static final String SUBJECT = "orders-value";
  private static final String SCHEMA_KEY = "42";
  private static final int VERSION = 1;
  private static final String EMPTY_SCHEMA = "{}";
  private static final String TOPICS_DIR = "topics";
  private static final String DELIMITER = "/";

  private StorageWriter writer;
  private ObjectStoreSchemaBackupStore store;

  @Before
  public void setUp() {
    writer = mock(StorageWriter.class);
    store = new ObjectStoreSchemaBackupStore(writer, TOPICS_DIR, DELIMITER);
  }

  @Test
  public void testBackupWritesSchemaAndEntryFiles() {
    when(writer.exists(anyString())).thenReturn(false);

    store.backupIfNeeded(TOPIC, SCHEMA_KEY, VERSION, BackupEnvelope.TYPE_AVRO,
        SUBJECT, "{\"type\":\"record\"}", null);

    ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
    verify(writer, times(2)).write(pathCaptor.capture(), anyString());

    assertTrue(pathCaptor.getAllValues().get(0).endsWith("42.avsc"));
    assertTrue(pathCaptor.getAllValues().get(1).endsWith("42.entry.json"));
  }

  @Test
  public void testBackupProtobufExtension() {
    when(writer.exists(anyString())).thenReturn(false);

    store.backupIfNeeded(TOPIC, "10", VERSION, BackupEnvelope.TYPE_PROTOBUF,
        SUBJECT, "syntax=\"proto3\";", null);

    ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
    verify(writer, times(2)).write(pathCaptor.capture(), anyString());
    assertTrue(pathCaptor.getAllValues().get(0).endsWith("10.proto"));
  }

  @Test
  public void testBackupJsonSchemaExtension() {
    when(writer.exists(anyString())).thenReturn(false);

    store.backupIfNeeded(TOPIC, "10", VERSION, BackupEnvelope.TYPE_JSON_SCHEMA,
        SUBJECT, "{\"type\":\"object\"}", null);

    ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
    verify(writer, times(2)).write(pathCaptor.capture(), anyString());
    assertTrue(pathCaptor.getAllValues().get(0).endsWith("10.json"));
  }

  @Test
  public void testLevel1DedupInMemory() {
    when(writer.exists(anyString())).thenReturn(false);

    store.backupIfNeeded(TOPIC, SCHEMA_KEY, VERSION, BackupEnvelope.TYPE_AVRO,
        SUBJECT, EMPTY_SCHEMA, null);
    store.backupIfNeeded(TOPIC, SCHEMA_KEY, VERSION, BackupEnvelope.TYPE_AVRO,
        SUBJECT, EMPTY_SCHEMA, null);

    verify(writer, times(2)).write(anyString(), anyString());
  }

  @Test
  public void testLevel2DedupExistsCheck() {
    when(writer.exists(anyString())).thenReturn(true);

    store.backupIfNeeded(TOPIC, SCHEMA_KEY, VERSION, BackupEnvelope.TYPE_AVRO,
        SUBJECT, EMPTY_SCHEMA, null);

    verify(writer, never()).write(anyString(), anyString());
  }

  @Test
  public void testRepeatedCallsShortCircuitBeforeExistsCheck() {
    when(writer.exists(anyString())).thenReturn(false);

    store.backupIfNeeded(TOPIC, SCHEMA_KEY, VERSION, BackupEnvelope.TYPE_AVRO,
        SUBJECT, EMPTY_SCHEMA, null);
    store.backupIfNeeded(TOPIC, SCHEMA_KEY, VERSION, BackupEnvelope.TYPE_AVRO,
        SUBJECT, EMPTY_SCHEMA, null);
    store.backupIfNeeded(TOPIC, SCHEMA_KEY, VERSION, BackupEnvelope.TYPE_AVRO,
        SUBJECT, EMPTY_SCHEMA, null);

    verify(writer, times(1)).exists(anyString());
    verify(writer, times(2)).write(anyString(), anyString());
  }

  @Test
  public void testNullOrEmptySchemaKeySkipped() {
    store.backupIfNeeded(TOPIC, null, VERSION, BackupEnvelope.TYPE_AVRO,
        SUBJECT, EMPTY_SCHEMA, null);
    store.backupIfNeeded(TOPIC, "", VERSION, BackupEnvelope.TYPE_AVRO,
        SUBJECT, EMPTY_SCHEMA, null);

    verify(writer, never()).write(anyString(), anyString());
    verify(writer, never()).exists(anyString());
  }

  @Test
  public void testEntryFileContainsReferences() {
    when(writer.exists(anyString())).thenReturn(false);
    SchemaManifest.SchemaReferenceEntry ref =
        new SchemaManifest.SchemaReferenceEntry("Address", "address-value", 1, 10);

    store.backupIfNeeded(TOPIC, SCHEMA_KEY, VERSION, BackupEnvelope.TYPE_AVRO,
        SUBJECT, EMPTY_SCHEMA, Collections.singletonList(ref));

    ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
    verify(writer, times(2)).write(anyString(), contentCaptor.capture());

    String entryJson = contentCaptor.getAllValues().get(1);
    assertTrue(entryJson.contains("\"Address\""));
    assertTrue(entryJson.contains("address-value"));
  }

  @Test
  public void testEntryWriteFailureAllowsRetry() {
    when(writer.exists(anyString())).thenReturn(false);
    Mockito.doNothing()
        .doThrow(new ConnectException("fail"))
        .when(writer).write(anyString(), anyString());

    try {
      store.backupIfNeeded(TOPIC, SCHEMA_KEY, VERSION, BackupEnvelope.TYPE_AVRO,
          SUBJECT, EMPTY_SCHEMA, null);
    } catch (ConnectException expected) {
      // schema write succeeds, entry write fails; retry below must resend both
    }

    when(writer.exists(anyString())).thenReturn(false);
    Mockito.doNothing().when(writer).write(anyString(), anyString());
    store.backupIfNeeded(TOPIC, SCHEMA_KEY, VERSION, BackupEnvelope.TYPE_AVRO,
        SUBJECT, EMPTY_SCHEMA, null);

    verify(writer, times(4)).write(anyString(), anyString());
  }

  @Test
  public void testSchemaPath() {
    when(writer.exists(anyString())).thenReturn(false);

    store.backupIfNeeded(TOPIC, SCHEMA_KEY, VERSION, BackupEnvelope.TYPE_AVRO,
        SUBJECT, EMPTY_SCHEMA, null);

    ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
    verify(writer, times(2)).write(pathCaptor.capture(), anyString());

    assertTrue(pathCaptor.getAllValues().get(0).startsWith("topics/orders/_metadata/schemas/"));
  }
}
