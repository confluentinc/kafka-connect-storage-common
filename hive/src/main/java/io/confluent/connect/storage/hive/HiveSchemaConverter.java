/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.connect.storage.hive;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;

public class HiveSchemaConverter {

  private static final Map<Type, TypeInfo> TYPE_TO_TYPEINFO;

  // the name has to be consistent with io.confluent.connect.avro.AvroData, when Connect Decimal
  // schema is created, this property name is used to set precision.
  // We have to use the exact name to retrieve precision value.
  protected static final String CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision";

  // this is the maximum digits Hive allows for DECIMAL type.
  protected static final int HIVE_DECIMAL_PRECISION_MAX = 38;

  static {
    TYPE_TO_TYPEINFO = new HashMap<>();
    TYPE_TO_TYPEINFO.put(Type.BOOLEAN, TypeInfoFactory.booleanTypeInfo);
    TYPE_TO_TYPEINFO.put(Type.INT8, TypeInfoFactory.byteTypeInfo);
    TYPE_TO_TYPEINFO.put(Type.INT16, TypeInfoFactory.shortTypeInfo);
    TYPE_TO_TYPEINFO.put(Type.INT32, TypeInfoFactory.intTypeInfo);
    TYPE_TO_TYPEINFO.put(Type.INT64, TypeInfoFactory.longTypeInfo);
    TYPE_TO_TYPEINFO.put(Type.FLOAT32, TypeInfoFactory.floatTypeInfo);
    TYPE_TO_TYPEINFO.put(Type.FLOAT64, TypeInfoFactory.doubleTypeInfo);
    TYPE_TO_TYPEINFO.put(Type.BYTES, TypeInfoFactory.binaryTypeInfo);
    TYPE_TO_TYPEINFO.put(Type.STRING, TypeInfoFactory.stringTypeInfo);
  }

  public static List<FieldSchema> convertSchema(Schema schema) {
    List<FieldSchema> columns = new ArrayList<>();
    if (Schema.Type.STRUCT.equals(schema.type())) {
      for (Field field: schema.fields()) {
        columns.add(new FieldSchema(
            field.name(), convert(field.schema()).getTypeName(), field.schema().doc()));
      }
    }
    return columns;
  }

  public static List<FieldSchema> convertSchemaMaybeLogical(Schema schema) {
    List<FieldSchema> columns = new ArrayList<>();
    if (Schema.Type.STRUCT.equals(schema.type())) {
      for (Field field: schema.fields()) {
        columns.add(new FieldSchema(
            field.name(), convertMaybeLogical(field.schema()).getTypeName(), field.schema().doc()));
      }
    }
    return columns;
  }

  public static TypeInfo convert(Schema schema, boolean convertLogical) {
    // TODO: throw an error on recursive types
    switch (schema.type()) {
      case STRUCT:
        return convertStruct(schema, convertLogical);
      case ARRAY:
        return convertArray(schema, convertLogical);
      case MAP:
        return convertMap(schema, convertLogical);
      default:
        return convertLogical ? convertPrimitiveMaybeLogical(schema) : convertPrimitive(schema);
    }
  }

  public static TypeInfo convert(Schema schema) {
    return convert(schema, false);
  }

  public static TypeInfo convertMaybeLogical(Schema schema) {
    return convert(schema, true);
  }

  public static TypeInfo convertStruct(Schema schema, boolean convertLogical) {
    final List<Field> fields = schema.fields();
    final List<String> names = new ArrayList<>(fields.size());
    final List<TypeInfo> types = new ArrayList<>(fields.size());
    for (Field field : fields) {
      names.add(field.name());
      types.add(convert(field.schema(), convertLogical));
    }
    return TypeInfoFactory.getStructTypeInfo(names, types);
  }

  public static TypeInfo convertStruct(Schema schema) {
    return convertStruct(schema, false);
  }

  public static TypeInfo convertArray(Schema schema, boolean convertLogical) {
    return TypeInfoFactory.getListTypeInfo(convert(schema.valueSchema(), convertLogical));
  }

  public static TypeInfo convertArray(Schema schema) {
    return convertArray(schema, false);
  }

  public static TypeInfo convertMap(Schema schema, boolean convertLogical) {
    return TypeInfoFactory.getMapTypeInfo(
        convert(schema.keySchema(), convertLogical), convert(schema.valueSchema(), convertLogical));
  }

  public static TypeInfo convertMap(Schema schema) {
    return convertMap(schema, false);
  }

  public static TypeInfo convertPrimitive(Schema schema) {
    return TYPE_TO_TYPEINFO.get(schema.type());
  }

  public static TypeInfo convertPrimitiveMaybeLogical(Schema schema) {
    if (schema.name() == null) {
      return convertPrimitive(schema);
    }

    switch (schema.name()) {
      case Decimal.LOGICAL_NAME:
        String scale = schema.parameters().get(Decimal.SCALE_FIELD);
        String precision = schema.parameters().get(CONNECT_AVRO_DECIMAL_PRECISION_PROP);
        if (precision != null && Integer.parseInt(precision) > HIVE_DECIMAL_PRECISION_MAX) {
          throw new ConnectException(
              String.format("Illegal precision %s : Hive allows at most %d precision.",
                  precision,
                  HIVE_DECIMAL_PRECISION_MAX)
          );
        }
        // Let precision always be HIVE_DECIMAL_PRECISION_MAX. Hive serde will try the best
        // to fit decimal data into decimal schema. If the data is too long even for
        // the maximum precision, hive will throw serde exception. No data loss risk.
        return new DecimalTypeInfo(HIVE_DECIMAL_PRECISION_MAX, Integer.parseInt(scale));
      case Date.LOGICAL_NAME:
        return TypeInfoFactory.dateTypeInfo;
      case Timestamp.LOGICAL_NAME:
        return TypeInfoFactory.timestampTypeInfo;
      default:
        return convertPrimitive(schema);
    }
  }
}
