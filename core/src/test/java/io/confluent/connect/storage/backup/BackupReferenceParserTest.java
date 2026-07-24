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

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BackupReferenceParserTest {

  @Test
  public void testParseDirectRefsNullInputs() {
    assertTrue(BackupReferenceParser
        .parseDirectRefsToManifestEntries(null, null).isEmpty());
    assertTrue(BackupReferenceParser
        .parseDirectRefsToManifestEntries("[]", null).isEmpty());
    assertTrue(BackupReferenceParser
        .parseDirectRefsToManifestEntries(null, "{}").isEmpty());
  }

  @Test
  public void testParseDirectRefsEmptyInputs() {
    assertTrue(BackupReferenceParser
        .parseDirectRefsToManifestEntries("", "{}").isEmpty());
    assertTrue(BackupReferenceParser
        .parseDirectRefsToManifestEntries("[]", "").isEmpty());
  }

  @Test
  public void testParseDirectRefsSingleRef() {
    String directRefs = "[{\"name\":\"com.example.Address\","
        + "\"subject\":\"address-value\",\"version\":1}]";
    String tree = "{\"com.example.Address\":"
        + "{\"subject\":\"address-value\",\"version\":1,"
        + "\"globalId\":10,\"schema\":\"{}\","
        + "\"references\":[],\"schemaType\":\"AVRO\"}}";

    List<SchemaManifest.SchemaReferenceEntry> result =
        BackupReferenceParser.parseDirectRefsToManifestEntries(
            directRefs, tree);

    assertEquals(1, result.size());
    assertEquals("com.example.Address", result.get(0).getName());
    assertEquals("address-value", result.get(0).getSubject());
    assertEquals(1, result.get(0).getVersion());
    assertEquals(10, result.get(0).getGlobalId());
  }

  @Test
  public void testParseDirectRefsOnlyDirectNotTransitive() {
    String directRefs = "[{\"name\":\"com.example.Address\","
        + "\"subject\":\"address-value\",\"version\":1}]";
    String tree = "{"
        + "\"com.example.Address\":"
        + "{\"subject\":\"address-value\",\"version\":1,"
        + "\"globalId\":20,\"schema\":\"{}\","
        + "\"references\":[{\"name\":\"com.example.Country\","
        + "\"subject\":\"country-value\",\"version\":1}],"
        + "\"schemaType\":\"AVRO\"},"
        + "\"com.example.Country\":"
        + "{\"subject\":\"country-value\",\"version\":1,"
        + "\"globalId\":10,\"schema\":\"{}\","
        + "\"references\":[],\"schemaType\":\"AVRO\"}"
        + "}";

    List<SchemaManifest.SchemaReferenceEntry> result =
        BackupReferenceParser.parseDirectRefsToManifestEntries(
            directRefs, tree);

    assertEquals(1, result.size());
    assertEquals("com.example.Address", result.get(0).getName());
    assertEquals(20, result.get(0).getGlobalId());
  }

  @Test
  public void testParseDirectRefsMultipleDirectRefs() {
    String directRefs = "["
        + "{\"name\":\"Address\","
        + "\"subject\":\"addr\",\"version\":1},"
        + "{\"name\":\"Country\","
        + "\"subject\":\"country\",\"version\":2}"
        + "]";
    String tree = "{"
        + "\"Address\":{\"subject\":\"addr\",\"version\":1,"
        + "\"globalId\":10,\"schema\":\"{}\","
        + "\"references\":[],\"schemaType\":\"AVRO\"},"
        + "\"Country\":{\"subject\":\"country\",\"version\":2,"
        + "\"globalId\":5,\"schema\":\"{}\","
        + "\"references\":[],\"schemaType\":\"AVRO\"}"
        + "}";

    List<SchemaManifest.SchemaReferenceEntry> result =
        BackupReferenceParser.parseDirectRefsToManifestEntries(
            directRefs, tree);

    assertEquals(2, result.size());
    assertEquals("Address", result.get(0).getName());
    assertEquals(10, result.get(0).getGlobalId());
    assertEquals("Country", result.get(1).getName());
    assertEquals(5, result.get(1).getGlobalId());
  }

  @Test
  public void testParseDirectRefsRefNotInTreeGlobalIdZero() {
    String directRefs = "[{\"name\":\"Missing\","
        + "\"subject\":\"missing-value\",\"version\":1}]";
    String tree = "{}";

    List<SchemaManifest.SchemaReferenceEntry> result =
        BackupReferenceParser.parseDirectRefsToManifestEntries(
            directRefs, tree);

    assertEquals(1, result.size());
    assertEquals("Missing", result.get(0).getName());
    assertEquals(0, result.get(0).getGlobalId());
  }
}
