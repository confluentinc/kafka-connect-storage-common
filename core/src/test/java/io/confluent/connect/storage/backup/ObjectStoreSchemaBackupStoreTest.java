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

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ObjectStoreSchemaBackupStoreTest {

  private StorageWriter writer;
  private ObjectStoreSchemaBackupStore store;

  @Before
  public void setUp() {
    writer = mock(StorageWriter.class);
    store = new ObjectStoreSchemaBackupStore(writer, "topics", "/");
  }

  @Test
  public void testBackupWritesSchemaAndEntryFiles() {
    when(writer.exists(anyString())).thenReturn(false);

    store.backupIfNeeded("orders", 42, 1, "AVRO",
        "orders-value", "{\"type\":\"record\"}", null);

    ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
    verify(writer, times(2)).write(pathCaptor.capture(), anyString());

    assertTrue(pathCaptor.getAllValues().get(0).endsWith("42.avsc"));
    assertTrue(pathCaptor.getAllValues().get(1).endsWith("42.entry.json"));
  }

  @Test
  public void testBackupProtobufExtension() {
    when(writer.exists(anyString())).thenReturn(false);

    store.backupIfNeeded("orders", 10, 1, "PROTOBUF",
        "orders-value", "syntax=\"proto3\";", null);

    ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
    verify(writer, times(2)).write(pathCaptor.capture(), anyString());
    assertTrue(pathCaptor.getAllValues().get(0).endsWith("10.proto"));
  }

  @Test
  public void testBackupJsonSchemaExtension() {
    when(writer.exists(anyString())).thenReturn(false);

    store.backupIfNeeded("orders", 10, 1, "JSON_SCHEMA",
        "orders-value", "{\"type\":\"object\"}", null);

    ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
    verify(writer, times(2)).write(pathCaptor.capture(), anyString());
    assertTrue(pathCaptor.getAllValues().get(0).endsWith("10.json"));
  }

  @Test
  public void testLevel1DedupInMemory() {
    when(writer.exists(anyString())).thenReturn(false);

    store.backupIfNeeded("orders", 42, 1, "AVRO",
        "orders-value", "{}", null);
    store.backupIfNeeded("orders", 42, 1, "AVRO",
        "orders-value", "{}", null);

    verify(writer, times(2)).write(anyString(), anyString());
  }

  @Test
  public void testLevel2DedupExistsCheck() {
    when(writer.exists(anyString())).thenReturn(true);

    store.backupIfNeeded("orders", 42, 1, "AVRO",
        "orders-value", "{}", null);

    verify(writer, never()).write(anyString(), anyString());
  }

  @Test
  public void testInvalidSchemaIdSkipped() {
    store.backupIfNeeded("orders", 0, 1, "AVRO",
        "orders-value", "{}", null);
    store.backupIfNeeded("orders", -1, 1, "AVRO",
        "orders-value", "{}", null);

    verify(writer, never()).write(anyString(), anyString());
    verify(writer, never()).exists(anyString());
  }

  @Test
  public void testEntryFileContainsReferences() {
    when(writer.exists(anyString())).thenReturn(false);
    SchemaManifest.SchemaReferenceEntry ref =
        new SchemaManifest.SchemaReferenceEntry(
            "Address", "address-value", 1, 10);

    store.backupIfNeeded("orders", 42, 1, "AVRO",
        "orders-value", "{}",
        Collections.singletonList(ref));

    ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
    verify(writer, times(2)).write(anyString(), contentCaptor.capture());

    String entryJson = contentCaptor.getAllValues().get(1);
    assertTrue(entryJson.contains("\"Address\""));
    assertTrue(entryJson.contains("address-value"));
  }

  @Test
  public void testEntryWriteFailureAllowsRetry() {
    when(writer.exists(anyString())).thenReturn(false);
    org.mockito.Mockito.doNothing()
        .doThrow(new org.apache.kafka.connect.errors.ConnectException("fail"))
        .when(writer).write(anyString(), anyString());

    store.backupIfNeeded("orders", 42, 1, "AVRO",
        "orders-value", "{}", null);

    when(writer.exists(anyString())).thenReturn(false);
    org.mockito.Mockito.doNothing().when(writer).write(anyString(), anyString());
    store.backupIfNeeded("orders", 42, 1, "AVRO",
        "orders-value", "{}", null);

    verify(writer, times(4)).write(anyString(), anyString());
  }

  @Test
  public void testSchemaPath() {
    when(writer.exists(anyString())).thenReturn(false);

    store.backupIfNeeded("orders", 42, 1, "AVRO",
        "orders-value", "{}", null);

    ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
    verify(writer, times(2)).write(pathCaptor.capture(), anyString());

    String schemaPath = pathCaptor.getAllValues().get(0);
    assertTrue(schemaPath.startsWith("topics/orders/_metadata/schemas/"));
  }
}
