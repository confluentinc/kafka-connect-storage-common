/*
 * Copyright [2018 - 2018] Confluent Inc.
 */

package io.confluent.connect.storage.schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.protobuf.ProtobufData;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import io.confluent.connect.avro.AvroData;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class StorageSchemaCompatibilityTest {

  private final StorageSchemaCompatibility none = StorageSchemaCompatibility.NONE;
  private final StorageSchemaCompatibility backward = StorageSchemaCompatibility.BACKWARD;
  private final StorageSchemaCompatibility forward = StorageSchemaCompatibility.FORWARD;
  private final StorageSchemaCompatibility full = StorageSchemaCompatibility.FULL;

  private static final String COLOR = "color";
  private static final String INNER = "inner";
  private static final String RED = "RED";
  private static final String GREEN = "GREEN";
  private static final String BLUE = "BLUE";

  // ---- Schemas for nested-schema compatibility tests ----

  // Struct containing an Avro enum field: version kept equal so version check doesn't fire first.
  private static final Schema STRUCT_WITH_AVRO_ENUM_BLUE =
      SchemaBuilder.struct().name("sr").version(1)
          .field("label", Schema.STRING_SCHEMA)
          .field(COLOR, buildAvroEnumSchema(COLOR, 1, RED, GREEN, BLUE).build())
          .build();

  private static final Schema STRUCT_WITH_AVRO_ENUM_NO_BLUE =
      SchemaBuilder.struct().name("sr").version(1)
          .field("label", Schema.STRING_SCHEMA)
          .field(COLOR, buildAvroEnumSchema(COLOR, 1, RED, GREEN).build())
          .build();

  // Same for Protobuf enums.
  private static final Schema STRUCT_WITH_PROTOBUF_ENUM_BLUE =
      SchemaBuilder.struct().name("sr").version(1)
          .field(COLOR, buildProtobufEnumSchema(COLOR, 1, RED, GREEN, BLUE).build())
          .build();

  private static final Schema STRUCT_WITH_PROTOBUF_ENUM_NO_BLUE =
      SchemaBuilder.struct().name("sr").version(1)
          .field(COLOR, buildProtobufEnumSchema(COLOR, 1, RED, GREEN).build())
          .build();

  // Deeply nested: outer struct → inner struct → enum field.
  private static final Schema DEEPLY_NESTED_WITH_AVRO_ENUM_BLUE =
      SchemaBuilder.struct().name("outer").version(1)
          .field(INNER,
              SchemaBuilder.struct().name(INNER)
                  .field(COLOR, buildAvroEnumSchema(COLOR, 1, RED, GREEN, BLUE).build())
                  .build())
          .build();

  private static final Schema DEEPLY_NESTED_WITH_AVRO_ENUM_NO_BLUE =
      SchemaBuilder.struct().name("outer").version(1)
          .field(INNER,
              SchemaBuilder.struct().name(INNER)
                  .field(COLOR, buildAvroEnumSchema(COLOR, 1, RED, GREEN).build())
                  .build())
          .build();

  // Struct whose field changes type (INT32 → STRING).
  private static final Schema STRUCT_WITH_INT_FIELD =
      SchemaBuilder.struct().name("s1").version(1)
          .field("count", Schema.INT32_SCHEMA)
          .build();

  private static final Schema STRUCT_WITH_STRING_FIELD =
      SchemaBuilder.struct().name("s1").version(1)
          .field("count", Schema.STRING_SCHEMA)
          .build();

  // Struct with a metadata doc parameter on a field — should NOT trigger rotation for
  // BACKWARD/FORWARD/FULL because checkSchemaParameters filters metadata parameters.
  private static final Schema STRUCT_WITH_FIELD_DOC_PARAM =
      SchemaBuilder.struct().name("s1").version(1)
          .field("name",
              SchemaBuilder.string()
                  .parameter("io.confluent.connect.avro.field.doc.name", "The entity name")
                  .build())
          .build();

  private static final Schema STRUCT_WITHOUT_FIELD_DOC_PARAM =
      SchemaBuilder.struct().name("s1").version(1)
          .field("name", Schema.STRING_SCHEMA)
          .build();

  // Array whose element schema is a struct containing an Avro enum.
  // Arrays need a top-level version for validateAndCheck to pass for non-NONE modes.
  private static final Schema ARRAY_OF_STRUCT_WITH_AVRO_ENUM_BLUE =
      SchemaBuilder.array(
          SchemaBuilder.struct().name("elem")
              .field(COLOR, buildAvroEnumSchema(COLOR, 1, RED, GREEN, BLUE).build())
              .build()
      ).name("arr").version(1).build();

  private static final Schema ARRAY_OF_STRUCT_WITH_AVRO_ENUM_NO_BLUE =
      SchemaBuilder.array(
          SchemaBuilder.struct().name("elem")
              .field(COLOR, buildAvroEnumSchema(COLOR, 1, RED, GREEN).build())
              .build()
      ).name("arr").version(1).build();

  // Map whose value schema is a struct containing an Avro enum.
  private static final Schema MAP_WITH_AVRO_ENUM_VALUE_BLUE =
      SchemaBuilder.map(Schema.STRING_SCHEMA,
          SchemaBuilder.struct().name("val")
              .field(COLOR, buildAvroEnumSchema(COLOR, 1, RED, GREEN, BLUE).build())
              .build()
      ).name("m").version(1).build();

  private static final Schema MAP_WITH_AVRO_ENUM_VALUE_NO_BLUE =
      SchemaBuilder.map(Schema.STRING_SCHEMA,
          SchemaBuilder.struct().name("val")
              .field(COLOR, buildAvroEnumSchema(COLOR, 1, RED, GREEN).build())
              .build()
      ).name("m").version(1).build();

  private final SchemaIncompatibilityType diffSchema = SchemaIncompatibilityType.DIFFERENT_SCHEMA;
  private final SchemaIncompatibilityType diffType = SchemaIncompatibilityType.DIFFERENT_TYPE;
  private final SchemaIncompatibilityType diffName = SchemaIncompatibilityType.DIFFERENT_NAME;
  private final SchemaIncompatibilityType diffParams = SchemaIncompatibilityType.DIFFERENT_PARAMS;
  private final SchemaIncompatibilityType diffVersion = SchemaIncompatibilityType.DIFFERENT_VERSION;
  private final SchemaIncompatibilityType na = SchemaIncompatibilityType.NA;

  private static SchemaBuilder buildStringSchema(String name, int version) {
    return SchemaBuilder.string()
                        .version(version)
                        .name(name);
  }

  private static SchemaBuilder buildIntSchema(String name, int version) {
    return SchemaBuilder.int32()
                        .version(version)
                        .name(name);
  }

  private static SchemaBuilder buildAvroEnumSchema(String name, int version, String... values) {
    // Enum schema is unwrapped as strings; symbols are represented as parameters
    SchemaBuilder enumSchema = SchemaBuilder.string()
            .version(version)
            .name(name);
    enumSchema.parameter(AvroData.AVRO_TYPE_ENUM, name);
    for (String value: values) {
      enumSchema.parameter(AvroData.AVRO_TYPE_ENUM + "." + value, value);
    }
    return enumSchema;
  }

  private static SchemaBuilder buildProtobufEnumSchema(String name, int version, String... values) {
    // Enum schema is unwrapped as strings or integers; symbols are represented as parameters
    SchemaBuilder enumSchema = SchemaBuilder.string()
        .version(version)
        .name(name);
    enumSchema.parameter(ProtobufData.PROTOBUF_TYPE_ENUM, name);
    for (String value: values) {
      enumSchema.parameter(ProtobufData.PROTOBUF_TYPE_ENUM + "."  + value, value);
    }
    return enumSchema;
  }

  private static final Schema SCHEMA_A =
      buildIntSchema("a", 2).build();
  private static final Schema SCHEMA_A_COPY =
      buildIntSchema("a", 2).build();
  private static final Schema SCHEMA_A_RENAMED =
      buildIntSchema("b", 2).build();
  private static final Schema SCHEMA_A_OPTIONAL =
      buildIntSchema("a", 2).optional().build();
  private static final Schema SCHEMA_A_OLDER_VERSION =
      buildIntSchema("a", 1).build();
  private static final Schema SCHEMA_A_NEWER_VERSION =
      buildIntSchema("a", 3).build();
  private static final Schema SCHEMA_A_PARAMETERED =
      buildIntSchema("a", 2).parameter("x", "y").build();
  private static final Schema SCHEMA_A_RETYPED =
      buildStringSchema("a", 1).build();
  private static final Schema SCHEMA_A_WITH_DOC =
      buildIntSchema("a", 2).doc("doc").build();

  private static SchemaBuilder buildStructSchema(String name, int version) {
    return SchemaBuilder.struct()
                        .name(name)
                        .version(version)
                        .field("b", Schema.BOOLEAN_SCHEMA)
                        .field("i", Schema.INT32_SCHEMA)
                        .field("d", Schema.FLOAT64_SCHEMA)
                        .field("s", Schema.OPTIONAL_STRING_SCHEMA)
                        .field(
                            "x",
                            SchemaBuilder.struct()
                                         .field("inner1", Schema.BOOLEAN_SCHEMA)
                                         .build()
                        );
  }

  private static final Schema SCHEMA_B =
      buildStructSchema("b", 2).build();
  private static final Schema SCHEMA_B_COPY =
      buildStructSchema("b", 2).build();
  private static final Schema SCHEMA_B_RENAMED =
      buildStructSchema("c", 2).build();
  private static final Schema SCHEMA_B_OPTIONAL =
      buildStructSchema("b", 2).optional().build();
  private static final Schema SCHEMA_B_OLDER_VERSION =
      buildStructSchema("b", 1).build();
  private static final Schema SCHEMA_B_NEWER_VERSION =
      buildStructSchema("b", 3).build();
  private static final Schema SCHEMA_B_PARAMETERED =
      buildStructSchema("b", 2).parameter("x", "y").build();
  private static final Schema SCHEMA_B_RETYPED =
      buildStringSchema("b", 2).build();
  private static final Schema SCHEMA_B_WITH_DOC =
      buildStructSchema("b", 2).doc("doc").build();
  private static final Schema SCHEMA_B_EXTRA_REQUIRED_FIELD =
      buildStructSchema("b", 2).field("extra", Schema.STRING_SCHEMA).build();
  private static final Schema SCHEMA_B_EXTRA_OPTIONAL_FIELD =
      buildStructSchema("b", 2).field("extra", Schema.OPTIONAL_STRING_SCHEMA).build();
  private static final Schema ENUM_SCHEMA_A =
      buildAvroEnumSchema("e1", 1, RED, GREEN, BLUE).build();
  private static final Schema ENUM_SCHEMA_B =
      buildAvroEnumSchema("e1", 1, RED, GREEN).build();
  private static final Schema ENUM_SCHEMA_C =
      buildProtobufEnumSchema("e1", 1, RED, GREEN, BLUE).build();
  private static final Schema ENUM_SCHEMA_D =
      buildProtobufEnumSchema("e1", 1, RED, GREEN).build();

  @Test
  public void testShouldChangeSchemaWithEnumAdditionAndBackwardCompatibility() {
    String value = BLUE;

    // Avro schema test
    SinkRecord sinkRecordAvro = new SinkRecord(
            "test-topic",
            0,
            null,
            null,
            ENUM_SCHEMA_A,
            value,
            0
    );

    SchemaCompatibilityResult result = StorageSchemaCompatibility.BACKWARD.shouldChangeSchema(sinkRecordAvro, null, ENUM_SCHEMA_B);
    assertTrue(result.isInCompatible());
    assertEquals(SchemaIncompatibilityType.DIFFERENT_PARAMS, result.getSchemaIncompatibilityType());

    // Protobuf schema test
    SinkRecord sinkRecordProtobuf = new SinkRecord(
        "test-topic",
        0,
        null,
        null,
        ENUM_SCHEMA_C,
        value,
        0
    );

    result = StorageSchemaCompatibility.BACKWARD.shouldChangeSchema(sinkRecordProtobuf, null, ENUM_SCHEMA_D);
    assertTrue(result.isInCompatible());
    assertEquals(SchemaIncompatibilityType.DIFFERENT_PARAMS, result.getSchemaIncompatibilityType());
  }

  @Test
  public void testShouldChangeSchemaWithEnumDeletionAndBackwardCompatibility() {
    String value = RED;

    // Avro schema test
    SinkRecord sinkRecordAvro = new SinkRecord(
            "test-topic",
            0,
            null,
            null,
            ENUM_SCHEMA_B,
            value,
            0
    );

    SchemaCompatibilityResult result = StorageSchemaCompatibility.BACKWARD.shouldChangeSchema(sinkRecordAvro, null, ENUM_SCHEMA_A);
    assertFalse(result.isInCompatible());

    // Protobuf schema test
    SinkRecord sinkRecordProtobuf = new SinkRecord(
        "test-topic",
        0,
        null,
        null,
        ENUM_SCHEMA_D,
        value,
        0
    );

    result = StorageSchemaCompatibility.BACKWARD.shouldChangeSchema(sinkRecordProtobuf, null, ENUM_SCHEMA_C);
    assertFalse(result.isInCompatible());
  }

  @Test
  public void testShouldChangeSchemaWithEnumAdditionAndForwardCompatibility() {
    String value = BLUE;

    // Avro schema test
    SinkRecord sinkRecordAvro = new SinkRecord(
        "test-topic",
        0,
        null,
        null,
        ENUM_SCHEMA_A,
        value,
        0
    );

    SchemaCompatibilityResult result = StorageSchemaCompatibility.FORWARD.shouldChangeSchema(sinkRecordAvro, null, ENUM_SCHEMA_B);
    assertTrue(result.isInCompatible());
    assertEquals(SchemaIncompatibilityType.DIFFERENT_PARAMS, result.getSchemaIncompatibilityType());

    // Protobuf schema test
    SinkRecord sinkRecordProtobuf = new SinkRecord(
        "test-topic",
        0,
        null,
        null,
        ENUM_SCHEMA_C,
        value,
        0
    );

    result = StorageSchemaCompatibility.FORWARD.shouldChangeSchema(sinkRecordProtobuf, null, ENUM_SCHEMA_D);
    assertTrue(result.isInCompatible());
    assertEquals(SchemaIncompatibilityType.DIFFERENT_PARAMS, result.getSchemaIncompatibilityType());
  }

  @Test
  public void testShouldChangeSchemaWithEnumDeletionAndForwardCompatibility() {
    String value = RED;

    // Avro schema test
    SinkRecord sinkRecordAvro = new SinkRecord(
        "test-topic",
        0,
        null,
        null,
        ENUM_SCHEMA_B,
        value,
        0
    );

    SchemaCompatibilityResult result = StorageSchemaCompatibility.FORWARD.shouldChangeSchema(sinkRecordAvro, null, ENUM_SCHEMA_A);
    assertFalse(result.isInCompatible());

    // Protobuf schema test
    SinkRecord sinkRecordProtobuf = new SinkRecord(
        "test-topic",
        0,
        null,
        null,
        ENUM_SCHEMA_D,
        value,
        0
    );

    result = StorageSchemaCompatibility.FORWARD.shouldChangeSchema(sinkRecordProtobuf, null, ENUM_SCHEMA_C);
    assertFalse(result.isInCompatible());
  }

  @Test
  public void testProjectSchemaAfterAddingEnumSymbol() {
    String value = GREEN;

    // Avro schema test
    assertThrows(SchemaProjectorException.class, () -> SchemaProjector.project(ENUM_SCHEMA_A, value, ENUM_SCHEMA_B));

    // Protobuf schema test
    assertThrows(SchemaProjectorException.class, () -> SchemaProjector.project(ENUM_SCHEMA_C, value, ENUM_SCHEMA_D));
  }

  @Test
  public void noneCompatibilityShouldConsiderSameVersionsAsUnchanged() {
    assertUnchanged(none, SCHEMA_A, SCHEMA_A_COPY);
    assertUnchanged(none, SCHEMA_B, SCHEMA_B_COPY);
  }

  @Test
  public void noneCompatibilityShouldConsiderDifferentVersionsAsChanged() {
    assertChanged(none, SCHEMA_A, SCHEMA_A_NEWER_VERSION, diffSchema);
    assertChanged(none, SCHEMA_A, SCHEMA_A_OLDER_VERSION, diffSchema);
    assertChanged(none, SCHEMA_B, SCHEMA_B_OLDER_VERSION, diffSchema);
    assertChanged(none, SCHEMA_B, SCHEMA_B_PARAMETERED, diffSchema);
  }

  @Test
  public void noneCompatibilityShouldConsiderAnyDifferencesAsChanged() {
    assertChanged(none, SCHEMA_A, SCHEMA_A_RENAMED, diffSchema);
    assertChanged(none, SCHEMA_A, SCHEMA_A_RETYPED, diffSchema);
    assertChanged(none, SCHEMA_A, SCHEMA_A_PARAMETERED, diffSchema);
    assertChanged(none, SCHEMA_A, SCHEMA_A_WITH_DOC, diffSchema);
    assertChanged(none, SCHEMA_A, SCHEMA_A_OPTIONAL, diffSchema);

    assertChanged(none, SCHEMA_B, SCHEMA_B_RENAMED, diffSchema);
    assertChanged(none, SCHEMA_B, SCHEMA_B_RETYPED, diffSchema);
    assertChanged(none, SCHEMA_B, SCHEMA_B_PARAMETERED, diffSchema);
    assertChanged(none, SCHEMA_B, SCHEMA_B_WITH_DOC, diffSchema);
    assertChanged(none, SCHEMA_B, SCHEMA_B_OPTIONAL, diffSchema);
    assertChanged(none, SCHEMA_B, SCHEMA_B_EXTRA_REQUIRED_FIELD, diffSchema);
    assertChanged(none, SCHEMA_B, SCHEMA_B_EXTRA_OPTIONAL_FIELD, diffSchema);

    assertUnchanged(none, SCHEMA_A, SCHEMA_A);
    assertUnchanged(none, SCHEMA_A, SCHEMA_A_COPY);
    assertUnchanged(none, SCHEMA_B, SCHEMA_B);
    assertUnchanged(none, SCHEMA_B, SCHEMA_B_COPY);
  }

  @Test
  public void backwardCompatibilityShouldConsiderOlderSchemaVersionsAsChanged() {
    assertChanged(backward, SCHEMA_A, SCHEMA_A_OLDER_VERSION, diffVersion);
    assertChanged(backward, SCHEMA_B, SCHEMA_B_OLDER_VERSION, diffVersion);
  }

  @Test
  public void backwardCompatibilityShouldConsiderSameOrNewerSchemaVersionsAsUnchanged() {
    assertUnchanged(backward, SCHEMA_A, SCHEMA_A_COPY);
    assertUnchanged(backward, SCHEMA_B, SCHEMA_B_COPY);
    assertUnchanged(backward, SCHEMA_A, SCHEMA_A_NEWER_VERSION);
    assertUnchanged(backward, SCHEMA_B, SCHEMA_B_NEWER_VERSION);
  }

  @Test
  public void forwardCompatibilityShouldConsiderNewerSchemaVersionsAsChanged() {
    assertChanged(forward, SCHEMA_A, SCHEMA_A_NEWER_VERSION, diffVersion);
    assertChanged(forward, SCHEMA_B, SCHEMA_B_NEWER_VERSION, diffVersion);
  }

  @Test
  public void forwardCompatibilityShouldConsiderSameOrOlderSchemaVersionsAsUnchanged() {
    assertUnchanged(forward, SCHEMA_A, SCHEMA_A_COPY);
    assertUnchanged(forward, SCHEMA_B, SCHEMA_B_COPY);
    assertUnchanged(forward, SCHEMA_A, SCHEMA_A_OLDER_VERSION);
    assertUnchanged(forward, SCHEMA_B, SCHEMA_B_OLDER_VERSION);
  }

  @Test
  public void fullCompatibilityShouldConsiderOlderSchemaVersionsAsChanged() {
    assertChanged(full, SCHEMA_A, SCHEMA_A_OLDER_VERSION, diffVersion);
    assertChanged(full, SCHEMA_B, SCHEMA_B_OLDER_VERSION, diffVersion);
  }

  @Test
  public void fullCompatibilityShouldConsiderSameOrNewerSchemaVersionsAsUnchanged() {
    assertUnchanged(full, SCHEMA_A, SCHEMA_A_COPY);
    assertUnchanged(full, SCHEMA_B, SCHEMA_B_COPY);
    assertUnchanged(full, SCHEMA_A, SCHEMA_A_NEWER_VERSION);
    assertUnchanged(full, SCHEMA_B, SCHEMA_B_NEWER_VERSION);
  }

  @Test
  public void allCompatibilitiesShouldConsiderSameOrEqualSchemasAsUnchanged() {
    assertUnchanged(none, SCHEMA_A, SCHEMA_A);
    assertUnchanged(none, SCHEMA_A, SCHEMA_A_COPY);
    assertUnchanged(none, SCHEMA_B, SCHEMA_B);
    assertUnchanged(none, SCHEMA_B, SCHEMA_B_COPY);

    assertUnchanged(backward, SCHEMA_A, SCHEMA_A);
    assertUnchanged(backward, SCHEMA_A, SCHEMA_A_COPY);
    assertUnchanged(backward, SCHEMA_B, SCHEMA_B);
    assertUnchanged(backward, SCHEMA_B, SCHEMA_B_COPY);

    assertUnchanged(forward, SCHEMA_A, SCHEMA_A);
    assertUnchanged(forward, SCHEMA_A, SCHEMA_A_COPY);
    assertUnchanged(forward, SCHEMA_B, SCHEMA_B);
    assertUnchanged(forward, SCHEMA_B, SCHEMA_B_COPY);

    assertUnchanged(full, SCHEMA_A, SCHEMA_A);
    assertUnchanged(full, SCHEMA_A, SCHEMA_A_COPY);
    assertUnchanged(full, SCHEMA_B, SCHEMA_B);
    assertUnchanged(full, SCHEMA_B, SCHEMA_B_COPY);
  }

  @Test
  public void allCompatibilitiesShouldConsiderDifferentSchemaNamesAsChanged() {
    assertChanged(none, SCHEMA_A, SCHEMA_A_RENAMED, diffSchema);
    assertChanged(backward, SCHEMA_A, SCHEMA_A_RENAMED, diffName);
    assertChanged(forward, SCHEMA_A, SCHEMA_A_RENAMED, diffName);
    assertChanged(full, SCHEMA_A, SCHEMA_A_RENAMED, diffName);

    assertChanged(none, SCHEMA_B, SCHEMA_B_RENAMED, diffSchema);
    assertChanged(backward, SCHEMA_B, SCHEMA_B_RENAMED, diffName);
    assertChanged(forward, SCHEMA_B, SCHEMA_B_RENAMED, diffName);
    assertChanged(full, SCHEMA_B, SCHEMA_B_RENAMED, diffName);
  }

  @Test
  public void allCompatibilitiesShouldConsiderDifferentSchemaTypesAsChanged() {
    assertChanged(none, SCHEMA_A, SCHEMA_A_RETYPED, diffSchema);
    assertChanged(backward, SCHEMA_A, SCHEMA_A_RETYPED, diffType);
    assertChanged(forward, SCHEMA_A, SCHEMA_A_RETYPED, diffType);
    assertChanged(full, SCHEMA_A, SCHEMA_A_RETYPED, diffType);

    assertChanged(none, SCHEMA_B, SCHEMA_B_RETYPED, diffSchema);
    assertChanged(backward, SCHEMA_B, SCHEMA_B_RETYPED, diffType);
    assertChanged(forward, SCHEMA_B, SCHEMA_B_RETYPED, diffType);
    assertChanged(full, SCHEMA_B, SCHEMA_B_RETYPED, diffType);
  }

  @Test
  public void allCompatibilitiesShouldConsiderDifferentSchemaParametersAsChanged() {
    assertChanged(none, SCHEMA_A, SCHEMA_A_PARAMETERED, diffSchema);
    assertChanged(backward, SCHEMA_A, SCHEMA_A_PARAMETERED, diffParams);
    assertChanged(forward, SCHEMA_A, SCHEMA_A_PARAMETERED, diffParams);
    assertChanged(full, SCHEMA_A, SCHEMA_A_PARAMETERED, diffParams);

    assertChanged(none, SCHEMA_B, SCHEMA_B_PARAMETERED, diffSchema);
    assertChanged(backward, SCHEMA_B, SCHEMA_B_PARAMETERED, diffParams);
    assertChanged(forward, SCHEMA_B, SCHEMA_B_PARAMETERED, diffParams);
    assertChanged(full, SCHEMA_B, SCHEMA_B_PARAMETERED, diffParams);
  }

  @Test
  public void backwardCompatibilityShouldConsiderDifferentSchemaDocsAsUnchanged() {
    assertUnchanged(backward, SCHEMA_A, SCHEMA_A_WITH_DOC);
    assertUnchanged(backward, SCHEMA_B, SCHEMA_B_WITH_DOC);
  }

  @Test
  public void forwardCompatibilityShouldConsiderDifferentSchemaDocsAsUnchanged() {
    assertUnchanged(forward, SCHEMA_A, SCHEMA_A_WITH_DOC);
    assertUnchanged(forward, SCHEMA_B, SCHEMA_B_WITH_DOC);
  }

  @Test
  public void fullCompatibilityShouldConsiderDifferentSchemaDocsAsUnchanged() {
    assertUnchanged(full, SCHEMA_A, SCHEMA_A_WITH_DOC);
    assertUnchanged(full, SCHEMA_B, SCHEMA_B_WITH_DOC);
  }

  @Test
  public void backwardCompatibilityShouldConsiderDifferentSchemaOptionalityAsUnchanged() {
    assertUnchanged(backward, SCHEMA_A, SCHEMA_A_OPTIONAL);
    assertUnchanged(backward, SCHEMA_B, SCHEMA_B_OPTIONAL);
  }

  @Test
  public void forwardCompatibilityShouldConsiderDifferentSchemaOptionalityAsUnchanged() {
    assertUnchanged(forward, SCHEMA_A, SCHEMA_A_OPTIONAL);
    assertUnchanged(forward, SCHEMA_B, SCHEMA_B_OPTIONAL);
  }

  @Test
  public void fullCompatibilityShouldConsiderDifferentSchemaOptionalityAsUnchanged() {
    assertUnchanged(full, SCHEMA_A, SCHEMA_A_OPTIONAL);
    assertUnchanged(full, SCHEMA_B, SCHEMA_B_OPTIONAL);

  }

  @Test
  public void backwardCompatibilityShouldConsiderDifferentFieldsAsUnchanged() {
    assertUnchanged(backward, SCHEMA_B, SCHEMA_B_EXTRA_OPTIONAL_FIELD);
    assertUnchanged(backward, SCHEMA_B_EXTRA_OPTIONAL_FIELD, SCHEMA_B);
    assertUnchanged(backward, SCHEMA_B_EXTRA_REQUIRED_FIELD, SCHEMA_B);
  }

  @Test
  public void forwardCompatibilityShouldConsiderDifferentFieldsAsUnchanged() {
    assertUnchanged(forward, SCHEMA_B, SCHEMA_B_EXTRA_OPTIONAL_FIELD);
    assertUnchanged(forward, SCHEMA_B_EXTRA_OPTIONAL_FIELD, SCHEMA_B);
    assertUnchanged(forward, SCHEMA_B_EXTRA_REQUIRED_FIELD, SCHEMA_B);
  }

  @Test
  public void fullCompatibilityShouldConsiderDifferentFieldsAsUnchanged() {
    assertUnchanged(full, SCHEMA_B, SCHEMA_B_EXTRA_OPTIONAL_FIELD);
    assertUnchanged(full, SCHEMA_B_EXTRA_OPTIONAL_FIELD, SCHEMA_B);
    assertUnchanged(full, SCHEMA_B_EXTRA_REQUIRED_FIELD, SCHEMA_B);
  }

  @Test
  public void allCompatibilitiesShouldConsiderExtraRequiredFieldsAsUnchangedButNotProjectable() {
    // The following are considered unchanged, but we cannot project from first to second.
    // The projection should catch this case if it happens, but SR should prevent records
    // being written to the topic if using Avro and `forward` SR compatibility is enabled
    assertUnchanged(backward, SCHEMA_B, SCHEMA_B_EXTRA_REQUIRED_FIELD, false);
    assertUnchanged(forward, SCHEMA_B, SCHEMA_B_EXTRA_REQUIRED_FIELD, false);
    assertUnchanged(full, SCHEMA_B, SCHEMA_B_EXTRA_REQUIRED_FIELD, false);
  }

  protected void assertUnchanged(
      StorageSchemaCompatibility compatibility,
      Schema recordSchema,
      Schema currentSchema
  ) {
    assertUnchanged(compatibility, recordSchema, currentSchema, true);
  }

  protected void assertUnchanged(
      StorageSchemaCompatibility compatibility,
      Schema recordSchema,
      Schema currentSchema,
      boolean isProjectable
  ) {
    final SchemaCompatibilityResult schemaCompatibilityResult =
        compatibility.validateAndCheck(recordSchema, currentSchema);
    assertFalse(
        "Expected " + currentSchema + " to not be change in order to project " + recordSchema,
        schemaCompatibilityResult.isInCompatible()
    );
    assertEquals(na, schemaCompatibilityResult.getSchemaIncompatibilityType());
    assertProjectable(compatibility, recordSchema, currentSchema, isProjectable);
  }

  protected void assertChanged(
      StorageSchemaCompatibility compatibility,
      Schema recordSchema,
      Schema currentSchema,
      SchemaIncompatibilityType schemaIncompatibilityType
  ) {
    final SchemaCompatibilityResult schemaCompatibilityResult =
        compatibility.validateAndCheck(recordSchema, currentSchema);
    assertTrue(
        "Expected " + currentSchema + " to change in order to project " + recordSchema,
        schemaCompatibilityResult.isInCompatible()
    );
    assertEquals(
        schemaIncompatibilityType,
        schemaCompatibilityResult.getSchemaIncompatibilityType()
    );
    // we don't care whether the schema are able to be projected
  }

  protected void assertProjectable(
      StorageSchemaCompatibility compatibility,
      Schema recordSchema,
      Schema currentSchema,
      boolean isProjectable
  ) {
    if (isProjectable) {
      // verify that the schema can be projected
      compatibility.project(sinkRecordWith(recordSchema), null, currentSchema);
      compatibility.project(sourceRecordWith(recordSchema), null, currentSchema);
    } else {
      try {
        compatibility.project(sinkRecordWith(recordSchema), null, currentSchema);
        fail(
            "Expected " + recordSchema + " to not be able to be projected to " + currentSchema
            );
      } catch (ConnectException e) {
        // expected
      }
      try {
        compatibility.project(sourceRecordWith(recordSchema), null, currentSchema);
        fail(
            "Expected " + recordSchema + " to not be able to be projected to " + currentSchema
            );
      } catch (ConnectException e) {
        // expected
      }
    }
  }

  protected SinkRecord sinkRecordWith(Schema valueSchema) {
    return new SinkRecord(
        "topic",
        0,
        null,
        null,
        valueSchema,
        valueFor(valueSchema),
        0
    );
  }

  protected SourceRecord sourceRecordWith(Schema valueSchema) {
    Map<String, ?> empty = Collections.emptyMap();
    return new SourceRecord(
        empty,
        empty,
        "topic",
        null,
        null,
        valueSchema,
        valueFor(valueSchema)
    );
  }

  /**
   * Adding an enum symbol inside a struct field must be detected by shouldChangeSchema so the
   * connector rotates the file instead of attempting projection (which would DLQ the record).
   * This is the core scenario from CC-38689.
   */
  @Test
  public void nestedAvroEnumAdditionInStructShouldTriggerRotation() {
    // record has BLUE (new symbol), current file schema does not → incompatible
    assertChanged(backward, STRUCT_WITH_AVRO_ENUM_BLUE, STRUCT_WITH_AVRO_ENUM_NO_BLUE, diffParams);
    assertChanged(forward,  STRUCT_WITH_AVRO_ENUM_BLUE, STRUCT_WITH_AVRO_ENUM_NO_BLUE, diffParams);
    assertChanged(full,     STRUCT_WITH_AVRO_ENUM_BLUE, STRUCT_WITH_AVRO_ENUM_NO_BLUE, diffParams);
  }

  @Test
  public void nestedProtobufEnumAdditionInStructShouldTriggerRotation() {
    assertChanged(backward, STRUCT_WITH_PROTOBUF_ENUM_BLUE, STRUCT_WITH_PROTOBUF_ENUM_NO_BLUE,
        diffParams);
    assertChanged(forward,  STRUCT_WITH_PROTOBUF_ENUM_BLUE, STRUCT_WITH_PROTOBUF_ENUM_NO_BLUE,
        diffParams);
    assertChanged(full,     STRUCT_WITH_PROTOBUF_ENUM_BLUE, STRUCT_WITH_PROTOBUF_ENUM_NO_BLUE,
        diffParams);
  }

  /**
   * A record whose enum field has fewer symbols than the current file schema is compatible:
   * the current schema already knows all symbols the record can produce.
   * Projection must also succeed (current schema is a superset).
   */
  @Test
  public void nestedEnumDeletionInStructShouldNotTriggerRotation() {
    // record has only RED+GREEN, current file schema has RED+GREEN+BLUE → compatible
    assertUnchanged(backward, STRUCT_WITH_AVRO_ENUM_NO_BLUE, STRUCT_WITH_AVRO_ENUM_BLUE);
    assertUnchanged(forward,  STRUCT_WITH_AVRO_ENUM_NO_BLUE, STRUCT_WITH_AVRO_ENUM_BLUE);
    assertUnchanged(full,     STRUCT_WITH_AVRO_ENUM_NO_BLUE, STRUCT_WITH_AVRO_ENUM_BLUE);
    assertUnchanged(backward, STRUCT_WITH_PROTOBUF_ENUM_NO_BLUE, STRUCT_WITH_PROTOBUF_ENUM_BLUE);
    assertUnchanged(forward,  STRUCT_WITH_PROTOBUF_ENUM_NO_BLUE, STRUCT_WITH_PROTOBUF_ENUM_BLUE);
    assertUnchanged(full,     STRUCT_WITH_PROTOBUF_ENUM_NO_BLUE, STRUCT_WITH_PROTOBUF_ENUM_BLUE);
  }

  /**
   * The enum addition must be detected even when it is two levels deep in the schema tree.
   */
  @Test
  public void deeplyNestedAvroEnumAdditionShouldTriggerRotation() {
    assertChanged(backward,
        DEEPLY_NESTED_WITH_AVRO_ENUM_BLUE,
        DEEPLY_NESTED_WITH_AVRO_ENUM_NO_BLUE,
        diffParams);
    assertChanged(forward,
        DEEPLY_NESTED_WITH_AVRO_ENUM_BLUE,
        DEEPLY_NESTED_WITH_AVRO_ENUM_NO_BLUE,
        diffParams);
    assertChanged(full,
        DEEPLY_NESTED_WITH_AVRO_ENUM_BLUE,
        DEEPLY_NESTED_WITH_AVRO_ENUM_NO_BLUE,
        diffParams);
  }

  /**
   * A field inside a struct changing type (INT32 → STRING) must trigger rotation so that
   * the projection engine does not receive schemas it cannot project.
   */
  @Test
  public void nestedFieldTypeChangeShouldTriggerRotation() {
    assertChanged(backward, STRUCT_WITH_STRING_FIELD, STRUCT_WITH_INT_FIELD, diffType);
    assertChanged(forward,  STRUCT_WITH_STRING_FIELD, STRUCT_WITH_INT_FIELD, diffType);
    assertChanged(full,     STRUCT_WITH_STRING_FIELD, STRUCT_WITH_INT_FIELD, diffType);

    // Opposite direction: record has INT32, current has STRING.
    assertChanged(backward, STRUCT_WITH_INT_FIELD, STRUCT_WITH_STRING_FIELD, diffType);
  }

  /**
   * An enum addition inside an array's element schema must also be caught recursively.
   */
  @Test
  public void arrayWithNestedEnumAdditionShouldTriggerRotation() {
    assertChanged(backward,
        ARRAY_OF_STRUCT_WITH_AVRO_ENUM_BLUE,
        ARRAY_OF_STRUCT_WITH_AVRO_ENUM_NO_BLUE,
        diffParams);
    assertChanged(forward,
        ARRAY_OF_STRUCT_WITH_AVRO_ENUM_BLUE,
        ARRAY_OF_STRUCT_WITH_AVRO_ENUM_NO_BLUE,
        diffParams);
    assertChanged(full,
        ARRAY_OF_STRUCT_WITH_AVRO_ENUM_BLUE,
        ARRAY_OF_STRUCT_WITH_AVRO_ENUM_NO_BLUE,
        diffParams);
  }

  /**
   * An enum addition inside a map's value schema must also be caught recursively.
   */
  @Test
  public void mapWithNestedEnumAdditionShouldTriggerRotation() {
    assertChanged(backward,
        MAP_WITH_AVRO_ENUM_VALUE_BLUE,
        MAP_WITH_AVRO_ENUM_VALUE_NO_BLUE,
        diffParams);
    assertChanged(forward,
        MAP_WITH_AVRO_ENUM_VALUE_BLUE,
        MAP_WITH_AVRO_ENUM_VALUE_NO_BLUE,
        diffParams);
    assertChanged(full,
        MAP_WITH_AVRO_ENUM_VALUE_BLUE,
        MAP_WITH_AVRO_ENUM_VALUE_NO_BLUE,
        diffParams);
  }

  /**
   * Metadata documentation parameters on nested fields (e.g., io.confluent.connect.avro.field.doc.*)
   * must not trigger rotation. These are filtered by checkSchemaParameters before comparison,
   * matching SchemaProjector's behaviour.
   */
  @Test
  public void nestedFieldDocParamShouldNotTriggerRotation() {
    assertUnchanged(backward, STRUCT_WITH_FIELD_DOC_PARAM, STRUCT_WITHOUT_FIELD_DOC_PARAM);
    assertUnchanged(forward,  STRUCT_WITH_FIELD_DOC_PARAM, STRUCT_WITHOUT_FIELD_DOC_PARAM);
    assertUnchanged(full,     STRUCT_WITH_FIELD_DOC_PARAM, STRUCT_WITHOUT_FIELD_DOC_PARAM);

    // Opposite direction: current has doc param, record does not
    assertUnchanged(backward, STRUCT_WITHOUT_FIELD_DOC_PARAM, STRUCT_WITH_FIELD_DOC_PARAM);
    assertUnchanged(forward,  STRUCT_WITHOUT_FIELD_DOC_PARAM, STRUCT_WITH_FIELD_DOC_PARAM);
    assertUnchanged(full,     STRUCT_WITHOUT_FIELD_DOC_PARAM, STRUCT_WITH_FIELD_DOC_PARAM);
  }

  /**
   * NONE compatibility must be unaffected: it overrides check() with Schema.equals() and
   * never calls checkSchemaCompatibility(), so nested schema details don't change its
   * rotation decision.
   */
  @Test
  public void noneCompatibilityShouldUseSchemaEqualsForNestedSchemas() {
    // Identical struct+enum schemas → no rotation
    assertUnchanged(none, STRUCT_WITH_AVRO_ENUM_BLUE, STRUCT_WITH_AVRO_ENUM_BLUE);

    // Struct schemas that differ in any way → rotation (diffSchema, not diffParams)
    assertChanged(none, STRUCT_WITH_AVRO_ENUM_BLUE, STRUCT_WITH_AVRO_ENUM_NO_BLUE, diffSchema);
    assertChanged(none, STRUCT_WITH_INT_FIELD,       STRUCT_WITH_STRING_FIELD,       diffSchema);
    assertChanged(none, STRUCT_WITH_FIELD_DOC_PARAM, STRUCT_WITHOUT_FIELD_DOC_PARAM, diffSchema);
    assertChanged(none,
        DEEPLY_NESTED_WITH_AVRO_ENUM_BLUE,
        DEEPLY_NESTED_WITH_AVRO_ENUM_NO_BLUE,
        diffSchema);
  }

  protected Object valueFor(Schema schema) {
    switch (schema.type()) {
      case INT8:
        return (byte) 100;
      case INT16:
        return (short) 100;
      case INT32:
        return 100;
      case INT64:
        return 100L;
      case FLOAT32:
        return 101.1;
      case FLOAT64:
        return 101.2d;
      case ARRAY:
        return new ArrayList<>();
      case MAP:
        return new HashMap<>();
      case BOOLEAN:
        return true;
      case BYTES:
        return "hello".getBytes(StandardCharsets.UTF_8);
      case STRING:
        return "hello";
      case STRUCT:
        Struct struct = new Struct(schema);
        for (Field field : schema.fields()) {
          struct.put(field, valueFor(field.schema()));
        }
        return struct;
    }
    return null;
  }

}
