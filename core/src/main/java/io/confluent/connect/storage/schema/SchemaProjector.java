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

package io.confluent.connect.storage.schema;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.protobuf.ProtobufData;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


public class SchemaProjector {

  private static final Set<AbstractMap.SimpleImmutableEntry<Schema.Type, Schema.Type>> promotable =
      new HashSet<>();

  static {
    Schema.Type[] promotableTypes = {Schema.Type.INT8, Schema.Type.INT16, Schema.Type.INT32,
        Schema.Type.INT64, Schema.Type.FLOAT32, Schema.Type.FLOAT64};
    for (int i = 0; i < promotableTypes.length; ++i) {
      for (int j = i; j < promotableTypes.length; ++j) {
        promotable.add(new AbstractMap.SimpleImmutableEntry<>(
            promotableTypes[i],
            promotableTypes[j]));
      }
    }
  }

  public static Object project(Schema source, Object record, Schema target)
      throws SchemaProjectorException {
    checkMaybeCompatible(source, target);
    if (source.isOptional() && !target.isOptional()) {
      if (target.defaultValue() != null) {
        if (record != null) {
          return projectRequiredSchema(source, record, target);
        } else {
          return target.defaultValue();
        }
      } else {
        throw new SchemaProjectorException("Writer schema is optional, "
            + "however, target schema does not provide a default value.");
      }
    } else {
      if (record != null) {
        return projectRequiredSchema(source, record, target);
      } else {
        return null;
      }
    }
  }

  private static Object projectRequiredSchema(Schema source, Object record, Schema target)
      throws SchemaProjectorException {
    switch (target.type()) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT32:
      case FLOAT64:
      case BOOLEAN:
      case BYTES:
      case STRING:
        return projectPrimitive(source, record, target);
      case STRUCT:
        return projectStruct(source, (Struct) record, target);
      case ARRAY:
        return projectArray(source, record, target);
      case MAP:
        return projectMap(source, record, target);
      default:
        return null;
    }
  }

  private static Object projectStruct(Schema source, Struct sourceStruct, Schema target)
      throws SchemaProjectorException {
    Struct targetStruct = new Struct(target);
    for (Field targetField : target.fields()) {
      String fieldName = targetField.name();
      Field sourceField = source.field(fieldName);
      if (sourceField != null) {
        Object sourceFieldValue = sourceStruct.get(fieldName);
        try {
          Object targetFieldValue = project(
              sourceField.schema(),
              sourceFieldValue,
              targetField.schema());
          targetStruct.put(fieldName, targetFieldValue);
        } catch (SchemaProjectorException e) {
          throw new SchemaProjectorException("Error projecting " + sourceField.name(), e);
        }
      } else if (targetField.schema().isOptional()) {
        // Ignore missing field
      } else if (targetField.schema().defaultValue() != null) {
        targetStruct.put(fieldName, targetField.schema().defaultValue());
      } else {
        throw new SchemaProjectorException("Required field `" +  fieldName
            + "` is missing from source schema: " + source);
      }
    }
    return targetStruct;
  }


  private static void checkMaybeCompatible(Schema source, Schema target) {
    if (source.type() != target.type() && !isPromotable(source.type(), target.type())) {
      throw new SchemaProjectorException("Schema type mismatch. source type: " + source.type()
          + " and target type: " + target.type());
    } else if (!Objects.equals(source.name(), target.name())) {
      throw new SchemaProjectorException("Schema name mismatch. source name: " + source.name()
          + " and target name: " + target.name());
    } else if (source.parameters() != null && target.parameters() != null) {
      // Create copies and filter out metadata parameters that don't affect compatibility
      Map<String, String> sourceParameters = filterMetadataParameters(source.parameters());
      Map<String, String> targetParameters = filterMetadataParameters(target.parameters());
      
      if (isEnumSchema(source) && isEnumSchema(target)) {
        if (!targetParameters.entrySet().containsAll(sourceParameters.entrySet())) {
          throw new SchemaProjectorException("Schema parameters mismatch. Source parameter: "
              + source.parameters()
              + " is not a subset of target parameters: " + target.parameters());
        }
      } else if (!Objects.equals(sourceParameters, targetParameters)) {
        throw new SchemaProjectorException("Schema parameters not equal. source parameters: "
            + source.parameters() + " and target parameters: " + target.parameters());
      }
    }
  }

  /**
   * Filters out metadata/documentation parameters that don't affect schema compatibility.
   * These parameters are used for documentation purposes and should not cause schema projection to fail.
   * 
   * @param parameters the original parameters map (may be null)
   * @return a new map with metadata parameters filtered out, or empty map if input was null or empty
   */
  private static Map<String, String> filterMetadataParameters(Map<String, String> parameters) {
    if (parameters == null || parameters.isEmpty()) {
      return new HashMap<>();
    }
    
    Map<String, String> filtered = new HashMap<>(parameters);
    
    // Remove Connect metadata parameters that don't affect compatibility
    filtered.remove("connect.record.doc");
    filtered.remove("connect.record.aliases");
    filtered.remove("connect.record.namespace");
    
    // Remove all Confluent Avro field documentation parameters (io.confluent.connect.avro.field.doc.*)
    // These are created when Avro field-level "doc" fields are present and don't affect schema compatibility
    filtered.entrySet().removeIf(entry -> entry.getKey()
            .startsWith("io.confluent.connect.avro.field.doc."));
    
    return filtered;
  }

  static boolean isEnumSchema(Schema schema) {
    return schema.parameters() != null
        && (schema.parameters().containsKey(AvroData.GENERALIZED_TYPE_ENUM)
        || schema.parameters().containsKey(AvroData.AVRO_TYPE_ENUM)
        || schema.parameters().containsKey(ProtobufData.PROTOBUF_TYPE_ENUM));
  }

  private static Object projectArray(Schema source, Object record, Schema target)
      throws SchemaProjectorException {
    List<?> array = (List<?>) record;
    List<Object> retArray = new ArrayList<>();
    for (Object entry : array) {
      retArray.add(project(source.valueSchema(), entry, target.valueSchema()));
    }
    return retArray;
  }

  private static Object projectMap(Schema source, Object record, Schema target)
      throws SchemaProjectorException {
    Map<?, ?> map = (Map<?, ?>) record;
    Map<Object, Object> retMap = new HashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      Object key = entry.getKey();
      Object value = entry.getValue();
      Object retKey = project(source.keySchema(), key, target.keySchema());
      Object retValue = project(source.valueSchema(), value, target.valueSchema());
      retMap.put(retKey, retValue);
    }
    return retMap;
  }

  private static Object projectPrimitive(Schema source, Object record, Schema target)
      throws SchemaProjectorException {
    assert source.type().isPrimitive();
    assert target.type().isPrimitive();
    Object result;
    if (isPromotable(source.type(), target.type()) && record instanceof Number) {
      Number numberRecord = (Number) record;
      switch (target.type()) {
        case INT8:
          result = numberRecord.byteValue();
          break;
        case INT16:
          result = numberRecord.shortValue();
          break;
        case INT32:
          result = numberRecord.intValue();
          break;
        case INT64:
          result = numberRecord.longValue();
          break;
        case FLOAT32:
          result = numberRecord.floatValue();
          break;
        case FLOAT64:
          result = numberRecord.doubleValue();
          break;
        default:
          throw new SchemaProjectorException("Not promotable type.");
      }
    } else {
      result = record;
    }
    return result;
  }

  private static boolean isPromotable(Schema.Type sourceType, Schema.Type targetType) {
    return promotable.contains(new AbstractMap.SimpleImmutableEntry<>(sourceType, targetType));
  }
}
