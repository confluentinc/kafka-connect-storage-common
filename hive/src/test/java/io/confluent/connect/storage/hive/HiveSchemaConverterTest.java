package io.confluent.connect.storage.hive;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

public class HiveSchemaConverterTest {
  @Test
  public void convertPrimitiveMaybeLogicalAllExceptDecimalTest() {
    // The only decimal type supported by Hive with parquet is decimal,
    // All other types should be parsed as primitive.
    Schema dateSchema = SchemaBuilder.int32().name(Date.LOGICAL_NAME).build();
    assertEquals(TypeInfoFactory.dateTypeInfo,
        HiveSchemaConverter.convertPrimitiveMaybeLogical(dateSchema));


    // logical type time is not supported by Hive serde, convert it to Hive INT
    Schema timeSchema = SchemaBuilder.int32().name(Time.LOGICAL_NAME).build();
    assertEquals(TypeInfoFactory.intTypeInfo,
        HiveSchemaConverter.convertPrimitiveMaybeLogical(timeSchema));

    // time when represented in microseconds is long
    Schema timeSchema64 = SchemaBuilder.int64().name(Time.LOGICAL_NAME).build();
    assertEquals(TypeInfoFactory.longTypeInfo,
        HiveSchemaConverter.convertPrimitiveMaybeLogical(timeSchema64));

    Schema timestampSchema = SchemaBuilder.int64().name(Timestamp.LOGICAL_NAME).build();
    assertEquals(TypeInfoFactory.timestampTypeInfo,
        HiveSchemaConverter.convertPrimitiveMaybeLogical(timestampSchema));
  }

  @Test
  public void convertPrimitiveMaybeLogicalDecimalValidTest() {
    Map<String, String> props1 = new HashMap<>();
    String someScale = "2";
    String validPrecision1 = String.valueOf(HiveSchemaConverter.HIVE_DECIMAL_PRECISION_MAX);
    props1.put(Decimal.SCALE_FIELD, someScale);
    props1.put(HiveSchemaConverter.CONNECT_AVRO_DECIMAL_PRECISION_PROP, validPrecision1);
    Schema decimalSchema1 = SchemaBuilder.bytes()
        .name(Decimal.LOGICAL_NAME)
        .parameters(props1)
        .build();

    Map<String, String> props2 = new HashMap<>();
    String validPrecision2 = "10";
    props2.put(Decimal.SCALE_FIELD, someScale);
    props2.put(HiveSchemaConverter.CONNECT_AVRO_DECIMAL_PRECISION_PROP, validPrecision2);
    Schema decimalSchema2 = SchemaBuilder.bytes()
        .name(Decimal.LOGICAL_NAME)
        .parameters(props2)
        .build();

    String scale = decimalSchema1.parameters().get(Decimal.SCALE_FIELD);
    String precision = decimalSchema1.parameters()
        .get(HiveSchemaConverter.CONNECT_AVRO_DECIMAL_PRECISION_PROP);

    assertEquals(new DecimalTypeInfo(Integer.parseInt(precision), Integer.parseInt(scale)),
        HiveSchemaConverter.convertPrimitiveMaybeLogical(decimalSchema1));

    // precision in decimalSchema2 is 10, but our schema converter will still convert it to
    // the maximum value, HiveSchemaConverter.HIVE_DECIMAL_PRECISION_MAX.

    assertEquals(new DecimalTypeInfo(Integer.parseInt(precision), Integer.parseInt(scale)),
        HiveSchemaConverter.convertPrimitiveMaybeLogical(decimalSchema2));
  }

  @Test
  public void convertPrimitiveMaybeLogicalDecimalAbsentPrecisionTest() {
    String someScale = "2";

    Schema decimalSchema = SchemaBuilder.bytes()
        .name(Decimal.LOGICAL_NAME)
        .parameter(Decimal.SCALE_FIELD, someScale)
        .build();

    assertEquals(
        new DecimalTypeInfo(HiveSchemaConverter.HIVE_DECIMAL_PRECISION_MAX, Integer.parseInt(someScale)),
        HiveSchemaConverter.convertPrimitiveMaybeLogical(decimalSchema));
  }

  @Test(expected = ConnectException.class)
  public void convertPrimitiveMaybeLogicalDecimalInvalidPrecisionTest() {
    Map<String, String> props = new HashMap<>();
    String someScale = "2";
    String invalidPrecision = String.valueOf(HiveSchemaConverter.HIVE_DECIMAL_PRECISION_MAX + 1);
    props.put(Decimal.SCALE_FIELD, someScale);
    props.put(HiveSchemaConverter.CONNECT_AVRO_DECIMAL_PRECISION_PROP, invalidPrecision);

    Schema decimalSchema = SchemaBuilder.bytes()
        .name(Decimal.LOGICAL_NAME)
        .parameters(props)
        .build();

    HiveSchemaConverter.convertPrimitiveMaybeLogical(decimalSchema);
  }

  @Test
  public void convertPrimitiveMaybeLogicalNotLogicalTest() {
    Map<String, String> props = new HashMap<>();
    String someScale = "2";
    String validPrecision = String.valueOf(HiveSchemaConverter.HIVE_DECIMAL_PRECISION_MAX);
    props.put(Decimal.SCALE_FIELD, someScale);
    props.put(HiveSchemaConverter.CONNECT_AVRO_DECIMAL_PRECISION_PROP, validPrecision);

    //without a name, not logical type
    Schema decimalSchema = SchemaBuilder.bytes()
        .parameters(props)
        .build();

    assertEquals(TypeInfoFactory.binaryTypeInfo,
        HiveSchemaConverter.convertPrimitiveMaybeLogical(decimalSchema));
  }

  private Schema createLogicalSchema() {
    return SchemaBuilder.struct().version(1)
        .field("timeInt", Time.SCHEMA)
        .field("timestamp", Timestamp.SCHEMA)
        .field("date", Date.SCHEMA)
        .field("decimal", Decimal.schema(2))
        .build();
  }
  @Test
  public void convertLogicalPrimitivesStruct() {

    Schema schema = createLogicalSchema();

    TypeInfo typeInfo = HiveSchemaConverter.convertMaybeLogical(schema);
    assertEquals(typeInfo.getCategory(), Category.STRUCT);

    List<TypeInfo> tp = ((StructTypeInfo) typeInfo).getAllStructFieldTypeInfos();
    for (int i = 0; i < tp.size(); i++) {
      assertEquals(tp.get(i).getTypeName(), HiveSchemaConverter.convertPrimitiveMaybeLogical(
          schema.fields().get(i).schema()).getTypeName());
    }
  }

  @Test
  public void convertNestedLogicalSchemas() {

    Schema innerSchema = createLogicalSchema();
    Schema schema = SchemaBuilder.struct().version(1)
        .field("innerStruct", innerSchema)
        .field("int", Schema.INT32_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .build();

    TypeInfo typeInfo = HiveSchemaConverter.convertMaybeLogical(schema);
    assertEquals(typeInfo.getCategory(), Category.STRUCT);

    List<TypeInfo> tp = ((StructTypeInfo) typeInfo).getAllStructFieldTypeInfos();
    TypeInfo innerStruct = tp.get(0);
    assertEquals(innerStruct.getCategory(), Category.STRUCT);

    List<TypeInfo> innerFields = ((StructTypeInfo) innerStruct).getAllStructFieldTypeInfos();
    for (int i = 0; i < innerFields.size(); i++) {
      assertEquals(HiveSchemaConverter.convertPrimitiveMaybeLogical(
          innerSchema.fields().get(i).schema()).getTypeName(), innerFields.get(i).getTypeName()
      );
    }

    for (int i = 1; i < schema.fields().size(); i++) {
      assertEquals(HiveSchemaConverter.convertPrimitiveMaybeLogical(
          schema.fields().get(i).schema()).getTypeName(), tp.get(i).getTypeName()
      );
    }
  }
}