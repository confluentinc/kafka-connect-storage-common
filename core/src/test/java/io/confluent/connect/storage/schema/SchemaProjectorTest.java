package io.confluent.connect.storage.schema;

import io.confluent.connect.avro.AvroData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.junit.Test;

import static org.junit.Assert.assertThrows;

public class SchemaProjectorTest {

  private static SchemaBuilder buildEnumSchema(String name, int version, String... values) {
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

  private static SchemaBuilder buildStringSchema(String name, int version) {
    return SchemaBuilder.string()
        .version(version)
        .name(name);
  }

  private static final Schema ENUM_SCHEMA_A =
      buildEnumSchema("e1", 1, "RED", "GREEN", "BLUE").build();
  private static final Schema ENUM_SCHEMA_A2 =
      buildEnumSchema("e1", 2, "RED", "GREEN").build();
  private static final Schema ENUM_SCHEMA_B =
      buildEnumSchema("e1", 1, "RED", "GREEN", "BLUE", "YELLOW").build();
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
  public void testCheckMaybeCompatibleWithEnumSchema() {
    String value = "RED";

    // Exception on addition of enum symbol
    assertThrows(SchemaProjectorException.class, () -> SchemaProjector.project(ENUM_SCHEMA_B, value, ENUM_SCHEMA_A));

    // No exception on removal of enum symbol
    SchemaProjector.project(ENUM_SCHEMA_A2, value, ENUM_SCHEMA_A);
  }
}
