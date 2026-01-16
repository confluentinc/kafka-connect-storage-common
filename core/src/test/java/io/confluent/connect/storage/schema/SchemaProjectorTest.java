package io.confluent.connect.storage.schema;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.protobuf.ProtobufData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class SchemaProjectorTest {

  private static SchemaBuilder buildAvroEnumSchema(String name, int version, String... values) {
    // Enum schema is unwrapped as strings; symbols are represented as parameters
    SchemaBuilder enumSchema = SchemaBuilder.string()
        .version(version)
        .name(name);
    enumSchema.parameter(AvroData.AVRO_TYPE_ENUM, name);
    for (String value: values) {
      enumSchema.parameter(AvroData.AVRO_TYPE_ENUM + "."  + value, value);
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

  private static SchemaBuilder buildStringSchema(String name, int version) {
    return SchemaBuilder.string()
        .version(version)
        .name(name);
  }

  private static final Schema ENUM_SCHEMA_A =
      buildAvroEnumSchema("e1", 1, "RED", "GREEN", "BLUE").build();
  private static final Schema ENUM_SCHEMA_A2 =
      buildAvroEnumSchema("e1", 2, "RED", "GREEN").build();
  private static final Schema ENUM_SCHEMA_B =
      buildAvroEnumSchema("e1", 1, "RED", "GREEN", "BLUE", "YELLOW").build();
  private static final Schema ENUM_SCHEMA_C =
      buildProtobufEnumSchema("e1", 1, "RED", "GREEN", "BLUE").build();
  private static final Schema ENUM_SCHEMA_C2 =
      buildProtobufEnumSchema("e1", 2, "RED", "GREEN").build();
  private static final Schema ENUM_SCHEMA_D =
      buildProtobufEnumSchema("e1", 1, "RED", "GREEN", "BLUE", "YELLOW").build();
  private static final Schema STRING_SCHEMA_A =
      buildStringSchema("schema1", 1).build();
  private static final Schema STRING_SCHEMA_B =
      buildStringSchema("schema2", 1).build();

  @Test
  public void testCheckMaybeCompatibleWithStringSchema() {
    String value = "test";

    // Test String schema and Enum schema are treated differently; String schema name mismatch
    assertThrows(SchemaProjectorException.class, () -> SchemaProjector.project(STRING_SCHEMA_A, value, STRING_SCHEMA_B));
  }

  @Test
  public void testCheckMaybeCompatibleWithAvroEnumSchema() {
    String value = "RED";

    // Exception on addition of enum symbol
    assertThrows(SchemaProjectorException.class, () -> SchemaProjector.project(ENUM_SCHEMA_B, value, ENUM_SCHEMA_A));

    // No exception on removal of enum symbol
    SchemaProjector.project(ENUM_SCHEMA_A2, value, ENUM_SCHEMA_A);
  }

  @Test
  public void testCheckMaybeCompatibleWithProtobufEnumSchema() {
    String value = "RED";

    // Exception on addition of enum symbol
    assertThrows(SchemaProjectorException.class, () -> SchemaProjector.project(ENUM_SCHEMA_D, value, ENUM_SCHEMA_C));

    // No exception on removal of enum symbol
    SchemaProjector.project(ENUM_SCHEMA_C2, value, ENUM_SCHEMA_C);
  }

  // ========== Tests for: Metadata parameters should not cause schema projection failures ==========

  @Test
  public void testConnectRecordDocParameterIgnored() {
    // Test that connect.record.doc parameter differences don't cause failures
    Schema source = SchemaBuilder.int32().name("test")
        .parameters(Collections.singletonMap("connect.record.doc", "source doc"))
        .build();
    Schema target = SchemaBuilder.int32().name("test")
        .parameters(Collections.singletonMap("connect.record.doc", "target doc"))
        .build();
    
    Object projected = SchemaProjector.project(source, 42, target);
    assertEquals(42, projected);
  }

  @Test
  public void testConnectRecordAliasesParameterIgnored() {
    // Test that connect.record.aliases parameter differences don't cause failures
    Schema source = SchemaBuilder.int32().name("test")
        .parameters(Collections.singletonMap("connect.record.aliases", "alias1,alias2"))
        .build();
    Schema target = SchemaBuilder.int32().name("test")
        .parameters(Collections.singletonMap("connect.record.aliases", "alias3,alias4"))
        .build();
    
    Object projected = SchemaProjector.project(source, 42, target);
    assertEquals(42, projected);
  }

  @Test
  public void testConnectRecordNamespaceParameterIgnored() {
    // Test that connect.record.namespace parameter differences don't cause failures
    Schema source = SchemaBuilder.int32().name("test")
        .parameters(Collections.singletonMap("connect.record.namespace", "com.example.source"))
        .build();
    Schema target = SchemaBuilder.int32().name("test")
        .parameters(Collections.singletonMap("connect.record.namespace", "com.example.target"))
        .build();
    
    Object projected = SchemaProjector.project(source, 42, target);
    assertEquals(42, projected);
  }

  @Test
  public void testAllConnectRecordMetadataParametersIgnored() {
    // Test that all connect.record metadata parameters can differ simultaneously
    Map<String, String> sourceParams = new HashMap<>();
    sourceParams.put("connect.record.doc", "source doc");
    sourceParams.put("connect.record.aliases", "alias1");
    sourceParams.put("connect.record.namespace", "com.source");
    
    Map<String, String> targetParams = new HashMap<>();
    targetParams.put("connect.record.doc", "target doc");
    targetParams.put("connect.record.aliases", "alias2");
    targetParams.put("connect.record.namespace", "com.target");
    
    Schema source = SchemaBuilder.int32().name("test").parameters(sourceParams).build();
    Schema target = SchemaBuilder.int32().name("test").parameters(targetParams).build();
    
    Object projected = SchemaProjector.project(source, 42, target);
    assertEquals(42, projected);
  }

  @Test
  public void testConfluentAvroFieldDocParametersIgnored() {
    // Test that io.confluent.connect.avro.field.doc.* parameters are ignored
    // This covers the case where new fields with doc are added to nested schemas
    Map<String, String> sourceParams = new HashMap<>();
    sourceParams.put("io.confluent.connect.avro.field.doc.innerField1", "blah");
    
    Map<String, String> targetParams = new HashMap<>();
    targetParams.put("io.confluent.connect.avro.field.doc.innerField1", "blah");
    targetParams.put("io.confluent.connect.avro.field.doc.innerField2", "blah");
    
    Schema source = SchemaBuilder.int32().name("test").parameters(sourceParams).build();
    Schema target = SchemaBuilder.int32().name("test").parameters(targetParams).build();
    
    // Should succeed even though new Confluent Avro field doc parameter is added
    Object projected = SchemaProjector.project(source, 42, target);
    assertEquals(42, projected);
  }

  @Test
  public void testFunctionalParametersStillChecked() {
    // Test that functional parameters (like scale for Decimal) are still checked
    Schema sourceDecimal = SchemaBuilder.bytes().name("org.apache.kafka.connect.data.Decimal")
        .parameter("connect.decimal.scale", "2").build();
    Schema targetDecimal = SchemaBuilder.bytes().name("org.apache.kafka.connect.data.Decimal")
        .parameter("connect.decimal.scale", "3").build();
    
    // Should fail because scale is a functional parameter that affects compatibility
    assertThrows(SchemaProjectorException.class,
        () -> SchemaProjector.project(sourceDecimal, new byte[]{1, 2}, targetDecimal));
  }

  @Test
  public void testNonMetadataParametersStillCauseFailures() {
    // Test that non-metadata parameters still cause failures
    Map<String, String> sourceParams = new HashMap<>();
    sourceParams.put("connect.record.doc", "source doc");
    sourceParams.put("important.param", "value1");
    
    Map<String, String> targetParams = new HashMap<>();
    targetParams.put("connect.record.doc", "target doc");
    targetParams.put("important.param", "value2");
    
    Schema source = SchemaBuilder.int32().name("test").parameters(sourceParams).build();
    Schema target = SchemaBuilder.int32().name("test").parameters(targetParams).build();
    
    assertThrows(SchemaProjectorException.class,
        () -> SchemaProjector.project(source, 42, target));
  }

  @Test
  public void testMetadataParametersInNestedSchemas() {
    // Test metadata parameters in nested struct schemas are ignored
    Schema innerSource = SchemaBuilder.struct()
        .name("Inner")
        .field("field", Schema.INT32_SCHEMA)
        .parameters(Collections.singletonMap("connect.record.doc", "source inner doc"))
        .build();
    
    Schema innerTarget = SchemaBuilder.struct()
        .name("Inner")
        .field("field", Schema.INT32_SCHEMA)
        .parameters(Collections.singletonMap("connect.record.doc", "target inner doc"))
        .build();
    
    Schema source = SchemaBuilder.struct()
        .field("nested", innerSource)
        .build();
    
    Schema target = SchemaBuilder.struct()
        .field("nested", innerTarget)
        .build();
    
    Struct sourceStruct = new Struct(source);
    Struct innerStruct = new Struct(innerSource);
    innerStruct.put("field", 42);
    sourceStruct.put("nested", innerStruct);
    
    Struct targetStruct = (Struct) SchemaProjector.project(source, sourceStruct, target);
    assertEquals(42, ((Struct) targetStruct.get("nested")).get("field"));
  }

  @Test
  public void testMetadataParametersInArraySchemas() {
    // Test metadata parameters in array value schemas are ignored
    Schema arrayValueSource = SchemaBuilder.int32()
        .parameters(Collections.singletonMap("connect.record.doc", "source doc"))
        .build();
    Schema arrayValueTarget = SchemaBuilder.int32()
        .parameters(Collections.singletonMap("connect.record.doc", "target doc"))
        .build();
    
    Schema source = SchemaBuilder.array(arrayValueSource).build();
    Schema target = SchemaBuilder.array(arrayValueTarget).build();
    
    List<Integer> array = Arrays.asList(1, 2, 3);
    Object projected = SchemaProjector.project(source, array, target);
    assertEquals(array, projected);
  }

  @Test
  public void testEmptyParametersHandled() {
    // Test that empty parameters are handled correctly
    Schema source = SchemaBuilder.int32().name("test").parameters(Collections.emptyMap()).build();
    Schema target = SchemaBuilder.int32().name("test").parameters(Collections.emptyMap()).build();
    
    Object projected = SchemaProjector.project(source, 42, target);
    assertEquals(42, projected);
  }

  @Test
  public void testMetadataParameterPresentInSourceButNotTarget() {
    // Test when metadata parameter exists in source but not target
    Schema source = SchemaBuilder.int32().name("test")
        .parameters(Collections.singletonMap("connect.record.doc", "some doc"))
        .build();
    Schema target = SchemaBuilder.int32().name("test").build();
    
    Object projected = SchemaProjector.project(source, 42, target);
    assertEquals(42, projected);
  }

  @Test
  public void testMetadataParameterPresentInTargetButNotSource() {
    // Test when metadata parameter exists in target but not source
    Schema source = SchemaBuilder.int32().name("test").build();
    Schema target = SchemaBuilder.int32().name("test")
        .parameters(Collections.singletonMap("connect.record.doc", "some doc"))
        .build();
    
    Object projected = SchemaProjector.project(source, 42, target);
    assertEquals(42, projected);
  }
}
