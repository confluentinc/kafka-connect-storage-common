/*
 * Copyright [2018 - 2018] Confluent Inc.
 */

package io.confluent.connect.storage.schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import static org.junit.Assert.*;

public class StorageSchemaCompatibilityTest {

  private final StorageSchemaCompatibility none = StorageSchemaCompatibility.NONE;
  private final StorageSchemaCompatibility backward = StorageSchemaCompatibility.BACKWARD;
  private final StorageSchemaCompatibility forward = StorageSchemaCompatibility.FORWARD;
  private final StorageSchemaCompatibility full = StorageSchemaCompatibility.FULL;

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
