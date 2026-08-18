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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BackupEnvelopeTest {

  // ── isSrBackedType — delegates to SchemaBackupConfig ─────────────────

  @Test
  public void testIsSrBackedTypeReturnsTrueForAvro() {
    assertTrue("AVRO must be recognized as SR-backed",
        BackupEnvelope.isSrBackedType(BackupEnvelope.TYPE_AVRO));
  }

  @Test
  public void testIsSrBackedTypeReturnsTrueForProtobuf() {
    assertTrue("PROTOBUF must be recognized as SR-backed",
        BackupEnvelope.isSrBackedType(BackupEnvelope.TYPE_PROTOBUF));
  }

  @Test
  public void testIsSrBackedTypeReturnsTrueForJsonSchema() {
    assertTrue("JSON_SCHEMA must be recognized as SR-backed",
        BackupEnvelope.isSrBackedType(BackupEnvelope.TYPE_JSON_SCHEMA));
  }

  @Test
  public void testIsSrBackedTypeReturnsFalseForString() {
    assertFalse("STRING must not be recognized as SR-backed",
        BackupEnvelope.isSrBackedType(BackupEnvelope.TYPE_STRING));
  }

  @Test
  public void testIsSrBackedTypeReturnsFalseForBytes() {
    assertFalse("BYTES must not be recognized as SR-backed",
        BackupEnvelope.isSrBackedType(BackupEnvelope.TYPE_BYTES));
  }

  @Test
  public void testIsSrBackedTypeReturnsFalseForJsonSchemaless() {
    assertFalse("JSON_SCHEMALESS must not be recognized as SR-backed",
        BackupEnvelope.isSrBackedType(BackupEnvelope.TYPE_JSON_SCHEMALESS));
  }

  @Test
  public void testIsSrBackedTypeReturnsFalseForNone() {
    assertFalse("NONE must not be recognized as SR-backed",
        BackupEnvelope.isSrBackedType(BackupEnvelope.TYPE_NONE));
  }

  @Test
  public void testIsSrBackedTypeReturnsFalseForUnknown() {
    assertFalse("UNKNOWN must not be recognized as SR-backed",
        BackupEnvelope.isSrBackedType(BackupEnvelope.TYPE_UNKNOWN));
  }

  @Test
  public void testIsSrBackedTypeReturnsFalseForNull() {
    assertFalse("null must not be recognized as SR-backed",
        BackupEnvelope.isSrBackedType(null));
  }

  // ── extensionForType — 4 branches (Avro / Protobuf / Json family / default)

  @Test
  public void testExtensionForTypeAvroReturnsAvsc() {
    assertEquals(BackupEnvelope.EXT_AVRO,
        BackupEnvelope.extensionForType(BackupEnvelope.TYPE_AVRO));
  }

  @Test
  public void testExtensionForTypeProtobufReturnsProto() {
    assertEquals(BackupEnvelope.EXT_PROTOBUF,
        BackupEnvelope.extensionForType(BackupEnvelope.TYPE_PROTOBUF));
  }

  @Test
  public void testExtensionForTypeJsonSchemaReturnsJson() {
    assertEquals(BackupEnvelope.EXT_JSON,
        BackupEnvelope.extensionForType(BackupEnvelope.TYPE_JSON_SCHEMA));
  }

  @Test
  public void testExtensionForTypeJsonReturnsJson() {
    assertEquals(BackupEnvelope.EXT_JSON,
        BackupEnvelope.extensionForType(BackupEnvelope.TYPE_JSON));
  }

  @Test
  public void testExtensionForTypeStringReturnsDefault() {
    assertEquals("non-schema type falls back to default extension",
        BackupEnvelope.EXT_DEFAULT,
        BackupEnvelope.extensionForType(BackupEnvelope.TYPE_STRING));
  }

  @Test
  public void testExtensionForTypeUnknownReturnsDefault() {
    assertEquals(BackupEnvelope.EXT_DEFAULT,
        BackupEnvelope.extensionForType("SOMETHING_ELSE"));
  }

  @Test
  public void testExtensionForTypeNullReturnsDefault() {
    assertEquals(BackupEnvelope.EXT_DEFAULT,
        BackupEnvelope.extensionForType(null));
  }
}
