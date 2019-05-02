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
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

public class HiveSchemaConverter {

  private static final Map<Type, TypeInfo> TYPE_TO_TYPEINFO;

  // the name here is consistent with io.confluent.connect.avro.AvroData, when connect Decimal
  // schema is created, this property name is used to set precision.
  // We have to use the exact name to retrieve precision value.
  static final String CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision";

  // this is the maximum digits Hive allows for DECIMAL type.
  static final int DECIMAL_PRECISION_DEFAULT = 38;

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

  public static List<FieldSchema>  convertSchemaMaybeLogical(Schema schema) {
    List<FieldSchema> columns = new ArrayList<>();
    if (Schema.Type.STRUCT.equals(schema.type())) {
      for (Field field: schema.fields()) {
        columns.add(new FieldSchema(
            field.name(), convertMaybeLogical(field.schema()).getTypeName(), field.schema().doc()));
      }
    }
    return columns;
  }

  public static TypeInfo convert(Schema schema) {
    // TODO: throw an error on recursive types
    switch (schema.type()) {
      case STRUCT:
        return convertStruct(schema);
      case ARRAY:
        return convertArray(schema);
      case MAP:
        return convertMap(schema);
      default:
        return convertPrimitive(schema);
    }
  }

  public static TypeInfo convertMaybeLogical(Schema schema) {
    switch (schema.type()) {
      case STRUCT:
        return convertStruct(schema);
      case ARRAY:
        return convertArray(schema);
      case MAP:
        return convertMap(schema);
      default:
        return convertPrimitiveMaybeLogical(schema);
    }
  }

  public static TypeInfo convertStruct(Schema schema) {
    final List<Field> fields = schema.fields();
    final List<String> names = new ArrayList<>(fields.size());
    final List<TypeInfo> types = new ArrayList<>(fields.size());
    for (Field field : fields) {
      names.add(field.name());
      types.add(convert(field.schema()));
    }
    return TypeInfoFactory.getStructTypeInfo(names, types);
  }

  public static TypeInfo convertArray(Schema schema) {
    return TypeInfoFactory.getListTypeInfo(convert(schema.valueSchema()));
  }

  public static TypeInfo convertMap(Schema schema) {
    return TypeInfoFactory.getMapTypeInfo(
        convert(schema.keySchema()), convert(schema.valueSchema()));
  }

  public static TypeInfo convertPrimitive(Schema schema) {
    return TYPE_TO_TYPEINFO.get(schema.type());
  }

  public static TypeInfo convertPrimitiveMaybeLogical(Schema schema) {
    if (schema.name() == null) {
      return TYPE_TO_TYPEINFO.get(schema.type());
    }

    switch (schema.name()) {
      case Decimal.LOGICAL_NAME:
        String scale = schema.parameters().get(Decimal.SCALE_FIELD);
        String precision = schema.parameters().get(CONNECT_AVRO_DECIMAL_PRECISION_PROP);
        if (precision == null
            || Integer.parseInt(precision) > DECIMAL_PRECISION_DEFAULT) {
          return TYPE_TO_TYPEINFO.get(schema.type());
        }
        return new DecimalTypeInfo(Integer.parseInt(precision), Integer.parseInt(scale));

      case Date.LOGICAL_NAME:
        return TypeInfoFactory.dateTypeInfo;

      case Time.LOGICAL_NAME:
        return TypeInfoFactory.intervalDayTimeTypeInfo;

      case Timestamp.LOGICAL_NAME:
        return TypeInfoFactory.timestampTypeInfo;

      default:
        return TYPE_TO_TYPEINFO.get(schema.type());
    }
  }
}
