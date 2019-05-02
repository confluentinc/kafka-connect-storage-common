package io.confluent.connect.storage.hive;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;

public class HiveSchemaConverterTest {

  @Test
  public void convertPrimitiveMaybeLogicalAllButDecimalTest() {

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
    Map<String, String> props = new HashMap<>();
    String someScale = "2";
    String validPrecision = "38";
    props.put(Decimal.SCALE_FIELD, someScale);
    props.put(HiveSchemaConverter.CONNECT_AVRO_DECIMAL_PRECISION_PROP, validPrecision);

    Schema decimalSchema = SchemaBuilder.bytes()
        .name(Decimal.LOGICAL_NAME)
        .parameters(props)
        .build();

    String scale = decimalSchema.parameters().get(Decimal.SCALE_FIELD);
    String precision = decimalSchema.parameters()
        .get(HiveSchemaConverter.CONNECT_AVRO_DECIMAL_PRECISION_PROP);

    assertEquals(new DecimalTypeInfo(Integer.parseInt(precision), Integer.parseInt(scale)),
        HiveSchemaConverter.convertPrimitiveMaybeLogical(decimalSchema));
  }

  @Test
  public void convertPrimitiveMaybeLogicalDecimalNoPrecisionTest() {
    String someScale = "2";

    Schema decimalSchema = SchemaBuilder.bytes()
        .name(Decimal.LOGICAL_NAME)
        .parameter(Decimal.SCALE_FIELD, someScale)
        .build();

    assertEquals(TypeInfoFactory.binaryTypeInfo,
        HiveSchemaConverter.convertPrimitiveMaybeLogical(decimalSchema));
  }

  @Test
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

    assertEquals(TypeInfoFactory.binaryTypeInfo,
        HiveSchemaConverter.convertPrimitiveMaybeLogical(decimalSchema));
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