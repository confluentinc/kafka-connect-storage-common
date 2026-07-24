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

import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SchemaManifestTest {

  @Test
  public void testEntryFields() {
    SchemaManifest.SchemaEntry entry = new SchemaManifest.SchemaEntry(
        "42", "AVRO", "test-value", 1, "42.avsc",
        Collections.emptyList());

    assertEquals("42", entry.getId());
    assertEquals("AVRO", entry.getType());
    assertEquals("test-value", entry.getSubject());
    assertEquals(1, entry.getVersion());
    assertEquals("42.avsc", entry.getFile());
    assertFalse(entry.hasReferences());
  }

  @Test
  public void testEntryWithReferences() {
    SchemaManifest.SchemaReferenceEntry ref = new SchemaManifest.SchemaReferenceEntry(
        "Address", "address-value", 1, 10);
    SchemaManifest.SchemaEntry entry = new SchemaManifest.SchemaEntry(
        "42", "AVRO", "order-value", 1, "42.avsc",
        Collections.singletonList(ref));

    assertTrue(entry.hasReferences());
    assertEquals(1, entry.getReferences().size());
    assertEquals("Address", entry.getReferences().get(0).getName());
    assertEquals("address-value", entry.getReferences().get(0).getSubject());
    assertEquals(1, entry.getReferences().get(0).getVersion());
    assertEquals(10, entry.getReferences().get(0).getGlobalId());
  }

  @Test
  public void testEntryWithNullReferences() {
    SchemaManifest.SchemaEntry entry = new SchemaManifest.SchemaEntry(
        "10", "AVRO", "address-value", 1, "10.avsc", null);
    assertFalse(entry.hasReferences());
    assertTrue(entry.getReferences().isEmpty());
  }

  @Test
  public void testEntryJsonRoundTrip() throws Exception {
    SchemaManifest.SchemaReferenceEntry ref = new SchemaManifest.SchemaReferenceEntry(
        "Country", "country-value", 1, 5);
    SchemaManifest.SchemaEntry original = new SchemaManifest.SchemaEntry(
        "42", "AVRO", "order-value", 3, "42.avsc",
        Collections.singletonList(ref));

    String json = original.toJsonString();
    SchemaManifest.SchemaEntry restored = SchemaManifest.SchemaEntry.fromJsonString(json);

    assertEquals(original.getId(), restored.getId());
    assertEquals(original.getType(), restored.getType());
    assertEquals(original.getSubject(), restored.getSubject());
    assertEquals(original.getVersion(), restored.getVersion());
    assertEquals(original.getFile(), restored.getFile());
    assertTrue(restored.hasReferences());
    assertEquals(1, restored.getReferences().size());
    assertEquals("Country", restored.getReferences().get(0).getName());
    assertEquals("country-value", restored.getReferences().get(0).getSubject());
    assertEquals(1, restored.getReferences().get(0).getVersion());
    assertEquals(5, restored.getReferences().get(0).getGlobalId());
  }

  @Test
  public void testEntryJsonRoundTripNoReferences() throws Exception {
    SchemaManifest.SchemaEntry original = new SchemaManifest.SchemaEntry(
        "5", "PROTOBUF", "address-value", 1, "5.proto",
        Collections.emptyList());

    String json = original.toJsonString();
    SchemaManifest.SchemaEntry restored = SchemaManifest.SchemaEntry.fromJsonString(json);

    assertEquals("5", restored.getId());
    assertEquals("PROTOBUF", restored.getType());
    assertEquals("address-value", restored.getSubject());
    assertEquals(1, restored.getVersion());
    assertEquals("5.proto", restored.getFile());
    assertFalse(restored.hasReferences());
  }

  @Test
  public void testEntryJsonContainsFormatVersion() throws Exception {
    SchemaManifest.SchemaEntry entry = new SchemaManifest.SchemaEntry(
        "1", "AVRO", "test", 1, "1.avsc", null);
    String json = entry.toJsonString();
    assertTrue(json.contains("\"format\" : 1"));
  }

  @Test
  public void testEntryFromJsonMissingFields() throws Exception {
    String json = "{\"id\":7,\"type\":\"AVRO\",\"subject\":\"test\",\"file\":\"7.avsc\"}";
    SchemaManifest.SchemaEntry entry =
        SchemaManifest.SchemaEntry.fromJsonString(json);

    assertEquals("7", entry.getId());
    assertEquals("AVRO", entry.getType());
    assertEquals("test", entry.getSubject());
    assertEquals(0, entry.getVersion());
    assertEquals("7.avsc", entry.getFile());
    assertFalse(entry.hasReferences());
  }

  @Test
  public void testEntryFromJsonUnknownFieldsIgnored() throws Exception {
    String json = "{\"id\":1,\"type\":\"AVRO\",\"subject\":\"s\","
        + "\"version\":1,\"file\":\"1.avsc\","
        + "\"references\":[],\"futureField\":\"ignored\"}";
    SchemaManifest.SchemaEntry entry =
        SchemaManifest.SchemaEntry.fromJsonString(json);
    assertNotNull(entry);
    assertEquals("1", entry.getId());
  }

  @Test
  public void testReferenceEntryJsonRoundTrip() throws Exception {
    SchemaManifest.SchemaReferenceEntry original =
        new SchemaManifest.SchemaReferenceEntry("Address", "address-value", 2, 10);

    com.fasterxml.jackson.databind.JsonNode json = original.toJson();
    SchemaManifest.SchemaReferenceEntry restored =
        SchemaManifest.SchemaReferenceEntry.fromJson(json);

    assertEquals("Address", restored.getName());
    assertEquals("address-value", restored.getSubject());
    assertEquals(2, restored.getVersion());
    assertEquals(10, restored.getGlobalId());
  }

  @Test
  public void testMultipleReferencesRoundTrip() throws Exception {
    List<SchemaManifest.SchemaReferenceEntry> refs = java.util.Arrays.asList(
        new SchemaManifest.SchemaReferenceEntry("Address", "address-value", 1, 10),
        new SchemaManifest.SchemaReferenceEntry("Country", "country-value", 1, 5));
    SchemaManifest.SchemaEntry original = new SchemaManifest.SchemaEntry(
        "42", "AVRO", "user-value", 1, "42.avsc", refs);

    String json = original.toJsonString();
    SchemaManifest.SchemaEntry restored = SchemaManifest.SchemaEntry.fromJsonString(json);

    assertEquals(2, restored.getReferences().size());
    assertEquals("Address", restored.getReferences().get(0).getName());
    assertEquals("Country", restored.getReferences().get(1).getName());
  }
}
