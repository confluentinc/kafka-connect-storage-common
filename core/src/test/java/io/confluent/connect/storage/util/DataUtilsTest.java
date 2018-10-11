package io.confluent.connect.storage.util;

import io.confluent.connect.storage.StorageSinkTestBase;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

public class DataUtilsTest extends StorageSinkTestBase {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static final Date DATE = new Date(TIMESTAMP);

  private void assertDate(Object o, Date value) {
    assertThat(o, is(notNullValue()));
    assertThat(o, is(instanceOf(Date.class)));
    assertThat((Date) o, is(value));
  }

  private void assertLong(Object o, Long value) {
    assertThat(o, is(notNullValue()));
    assertThat(o, is(instanceOf(Long.class)));
    assertThat((Long) o, is(value));
  }

  private void assertStruct(Object o, Struct value) {
    assertThat(o, is(notNullValue()));
    assertThat(o, is(instanceOf(Struct.class)));
    assertThat((Struct) o, is(value));
  }

  private void assertMap(Object o, Map<?, ?> value) {
    assertThat(o, is(notNullValue()));
    assertThat(o, is(instanceOf(Map.class)));
    assertThat(o, CoreMatchers.<Object>is(value));
  }

  @Test
  public void testGetDateFieldStruct() {
    Schema schema = createSchemaWithTimestampField(Timestamp.SCHEMA);
    Struct struct = createRecordWithTimestampField(schema, DATE);

    Object timeField = DataUtils.getNestedFieldValue(struct, "timestamp");
    assertDate(timeField, DATE);
  }

  @Test
  public void testGetLongFieldStruct() {
    Schema schema = createSchemaWithTimestampField();
    Struct struct = createRecordWithTimestampField(schema, TIMESTAMP);

    Object timeField = DataUtils.getNestedFieldValue(struct, "timestamp");
    assertLong(timeField, DATE.getTime());
  }

  @Test
  public void testNestedGetFieldStruct() {
    Struct struct = createRecordWithNestedTimestampField(TIMESTAMP);
    Struct expectedNestedStruct = (Struct) struct.get("nested");
    assertThat(expectedNestedStruct, is(notNullValue()));

    Object actualNestedStruct = DataUtils.getField(struct, "nested");
    assertStruct(actualNestedStruct, expectedNestedStruct);

    Object timeField = DataUtils.getNestedFieldValue(struct, "nested.timestamp");
    assertLong(timeField, DATE.getTime());
  }

  @Test
  public void testGetFieldMap() {
    Map<String, Object> map = createMapWithTimestampField(TIMESTAMP);

    Object timeField = DataUtils.getNestedFieldValue(map, "timestamp");
    assertLong(timeField, TIMESTAMP);
  }

  @Test
  public void testNestedGetFieldMap() {
    Map<String, Object> expectedNestedMap = createMapWithTimestampField(TIMESTAMP);
    Map<String, Object> map = new HashMap<>();
    map.put("nested", expectedNestedMap);

    Object actualNestedMap = DataUtils.getField(map, "nested");
    assertMap(actualNestedMap, expectedNestedMap);

    Object timeField = DataUtils.getNestedFieldValue(map, "nested.timestamp");
    assertLong(timeField, TIMESTAMP);
  }

  @Test
  public void testDoubleNestedGetFieldMap() {
    Map<String, Object> expectedNestedMap = createMapWithTimestampField(TIMESTAMP);
    Map<String, Object> middleMap = new HashMap<>();
    middleMap.put("nested", expectedNestedMap);
    Map<String, Object> map = new HashMap<>();
    map.put("top", middleMap);

    Object actualMiddleMap = DataUtils.getField(map, "top");
    assertMap(actualMiddleMap, middleMap);

    Object actualNestedMap = DataUtils.getNestedFieldValue(map, "top.nested");
    assertMap(actualNestedMap, expectedNestedMap);

    Object timeField = DataUtils.getNestedFieldValue(map, "top.nested.timestamp");
    assertLong(timeField, TIMESTAMP);
  }

  @Test
  public void testGetFieldSchema() {
    Schema schema = createSchemaWithTimestampField();
    Field timeField = DataUtils.getNestedField(schema, "timestamp");

    assertThat(timeField, is(notNullValue()));
    assertThat(timeField.schema(), is(Schema.INT64_SCHEMA));
  }

  @Test
  public void testGetNestedFieldSchema() {
    Schema nestedChildSchema = createSchemaWithTimestampField();
    Schema schema = SchemaBuilder.struct().field("nested", nestedChildSchema);

    Field nestedField = DataUtils.getNestedField(schema, "nested");
    assertThat(nestedField, is(notNullValue()));
    assertThat(nestedField.index(), is(0));
    assertThat(nestedField.schema().type(), is(Schema.Type.STRUCT));

    nestedField = DataUtils.getNestedField(schema, "nested.timestamp");
    assertThat(nestedField, is(notNullValue()));
    assertThat(nestedField.schema(), is(Schema.INT64_SCHEMA));
  }

  @Test
  public void testMissingTopFieldSchema() {
    Schema schema = createSchemaWithTimestampField();
    Field f = DataUtils.getNestedField(schema, "foo");
    assertThat(f, is(nullValue()));
  }

  @Test
  public void testValidateNullObject() throws ConnectException {
    thrown.expect(ConnectException.class);
    thrown.expectMessage(is("Attempted to extract a field from a null object."));

    DataUtils.getNestedField(null, "foo");
  }

  @Test
  public void testValidateNullField() throws ConnectException {
    thrown.expect(ConnectException.class);
    thrown.expectMessage(is("The field to extract cannot be null or empty."));

    Schema schema = createSchemaWithTimestampField();
    DataUtils.getNestedField(schema, null);
  }

  @Test
  public void testValidateEmptyField() throws ConnectException {
    thrown.expect(ConnectException.class);
    thrown.expectMessage(is("The field to extract cannot be null or empty."));

    Schema schema = createSchemaWithTimestampField();
    DataUtils.getNestedField(schema, "");
  }

  @Test
  public void testWrongDataStructure() throws DataException {
    thrown.expect(DataException.class);
    thrown.expectMessage(startsWith("Argument not a Struct or Map"));

    List<Integer> x = Arrays.asList(1, 2, 3);
    DataUtils.getField(x, "foo");
  }

  @Test
  public void testMissingTopField() throws DataException {
    String fieldName = "foo";
    thrown.expect(DataException.class);
    thrown.expectMessage(startsWith(String.format("The field '%s' does not exist in", fieldName)));

    Map<String, Object> map = createMapWithTimestampField(TIMESTAMP);
    DataUtils.getNestedFieldValue(map, fieldName);
  }

  @Test
  public void testMissingNestedField() throws DataException {
    String topField = "nested";
    String fieldName = "foo";
    thrown.expect(DataException.class);
    thrown.expectMessage(startsWith(
          String.format("The field '%s.%s' does not exist in", topField, fieldName)));

    Map<String, Object> innerMap = createMapWithTimestampField(TIMESTAMP);
    Map<String, Object> map = new HashMap<>();
    map.put(topField, innerMap);
    DataUtils.getNestedFieldValue(map, topField + "." + fieldName);
  }

  @Test
  public void testNestedFieldSchemaWrongType() throws DataException {
    thrown.expect(DataException.class);
    thrown.expectMessage(startsWith("Unable to get field"));

    Schema schema = createSchemaWithTimestampField();
    Field f = DataUtils.getNestedField(schema, "string");
    DataUtils.getNestedField(f.schema(), "foo");

    DataUtils.getNestedField(schema, "string.foo");
  }
}
