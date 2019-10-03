package io.confluent.connect.storage.hive;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
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

    Schema dateSchema = SchemaBuilder.int32().name(Date.LOGICAL_NAME).build();
    assertEquals(TypeInfoFactory.dateTypeInfo,
        HiveSchemaConverter.convertPrimitiveMaybeLogical(dateSchema));

    Schema timeSchema = SchemaBuilder.int32().name(Time.LOGICAL_NAME).build();
    assertEquals(TypeInfoFactory.intervalDayTimeTypeInfo,
        HiveSchemaConverter.convertPrimitiveMaybeLogical(timeSchema));

    Schema timestampSchema = SchemaBuilder.int64().name(Timestamp.LOGICAL_NAME).build();
    assertEquals(TypeInfoFactory.timestampTypeInfo,
        HiveSchemaConverter.convertPrimitiveMaybeLogical(timestampSchema));
  }

  @Test
  public void convertPrimitiveMaybeLogicalDecimalValidTest() {
    Map<String, String> props1 = new HashMap<>();
    String someScale = "2";
    String validPrecision1 = "38";
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
    // the maximum value, 38.

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
        new DecimalTypeInfo(HiveSchemaConverter.DECIMAL_PRECISION_DEFAULT, Integer.parseInt(someScale)),
        HiveSchemaConverter.convertPrimitiveMaybeLogical(decimalSchema));
  }

  @Test(expected = ConnectException.class)
  public void convertPrimitiveMaybeLogicalDecimalInvalidPrecisionTest() {
    Map<String, String> props = new HashMap<>();
    String someScale = "2";
    String invalidPrecision = "39";
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
    String validPrecision = "38";
    props.put(Decimal.SCALE_FIELD, someScale);
    props.put(HiveSchemaConverter.CONNECT_AVRO_DECIMAL_PRECISION_PROP, validPrecision);

    //without a name, not logical type
    Schema decimalSchema = SchemaBuilder.bytes()
        .parameters(props)
        .build();

    assertEquals(TypeInfoFactory.binaryTypeInfo,
        HiveSchemaConverter.convertPrimitiveMaybeLogical(decimalSchema));
  }
}