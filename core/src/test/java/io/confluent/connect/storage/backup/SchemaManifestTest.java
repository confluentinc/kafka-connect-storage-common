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

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SchemaManifestTest {

  @Test
  public void testEmptyManifest() {
    SchemaManifest manifest = new SchemaManifest();
    assertEquals("1.0", manifest.getVersion());
    assertTrue(manifest.getEntries().isEmpty());
    assertTrue(manifest.getSchemas().isEmpty());
  }

  @Test
  public void testAddAndGetEntry() {
    SchemaManifest manifest = new SchemaManifest();
    SchemaManifest.SchemaEntry entry = new SchemaManifest.SchemaEntry(
        42, "AVRO", "test-value", 1, "42.avsc",
        Collections.emptyList(), "BACKWARD");
    manifest.addEntry(entry);

    assertEquals(1, manifest.getEntries().size());
    assertNotNull(manifest.getSchema(42));
    assertEquals("AVRO", manifest.getSchema(42).getType());
    assertEquals("test-value", manifest.getSchema(42).getSubject());
    assertEquals(1, manifest.getSchema(42).getVersion());
    assertEquals("42.avsc", manifest.getSchema(42).getFile());
    assertEquals("BACKWARD", manifest.getSchema(42).getCompatibility());
  }

  @Test
  public void testEntryWithReferences() {
    SchemaManifest.SchemaReferenceEntry ref = new SchemaManifest.SchemaReferenceEntry(
        "Address", "address-value", 1, 10);
    SchemaManifest.SchemaEntry entry = new SchemaManifest.SchemaEntry(
        42, "AVRO", "order-value", 1, "42.avsc",
        Collections.singletonList(ref), null);

    assertTrue(entry.hasReferences());
    assertEquals(1, entry.getReferences().size());
    assertEquals("Address", entry.getReferences().get(0).getName());
    assertEquals("address-value", entry.getReferences().get(0).getSubject());
    assertEquals(1, entry.getReferences().get(0).getVersion());
    assertEquals(10, entry.getReferences().get(0).getGlobalId());
  }

  @Test
  public void testEntryWithoutReferences() {
    SchemaManifest.SchemaEntry entry = new SchemaManifest.SchemaEntry(
        10, "AVRO", "address-value", 1, "10.avsc",
        null, null);
    assertFalse(entry.hasReferences());
    assertTrue(entry.getReferences().isEmpty());
  }

  @Test
  public void testJsonRoundTrip() throws Exception {
    SchemaManifest manifest = new SchemaManifest();
    SchemaManifest.SchemaReferenceEntry ref = new SchemaManifest.SchemaReferenceEntry(
        "Country", "country-value", 1, 5);
    manifest.addEntry(new SchemaManifest.SchemaEntry(
        42, "AVRO", "order-value", 1, "42.avsc",
        Collections.singletonList(ref), "BACKWARD"));
    manifest.addEntry(new SchemaManifest.SchemaEntry(
        5, "AVRO", "country-value", 1, "5.avsc",
        Collections.emptyList(), null));

    byte[] json = manifest.toJson();
    SchemaManifest restored = SchemaManifest.fromJson(json);

    assertEquals("1.0", restored.getVersion());
    assertEquals(2, restored.getEntries().size());
    assertNotNull(restored.getSchema(42));
    assertNotNull(restored.getSchema(5));
    assertTrue(restored.getSchema(42).hasReferences());
    assertFalse(restored.getSchema(5).hasReferences());
    assertEquals("Country",
        restored.getSchema(42).getReferences().get(0).getName());
  }

  @Test
  public void testGetSchemaNonExistent() {
    SchemaManifest manifest = new SchemaManifest();
    assertNull(manifest.getSchema(999));
  }

  @Test
  public void testAddDuplicateOverwrites() {
    SchemaManifest manifest = new SchemaManifest();
    manifest.addEntry(new SchemaManifest.SchemaEntry(
        42, "AVRO", "old-subject", 1, "42.avsc", null, null));
    manifest.addEntry(new SchemaManifest.SchemaEntry(
        42, "AVRO", "new-subject", 2, "42.avsc", null, null));

    assertEquals(1, manifest.getEntries().size());
    assertEquals("new-subject", manifest.getSchema(42).getSubject());
    assertEquals(2, manifest.getSchema(42).getVersion());
  }
}
