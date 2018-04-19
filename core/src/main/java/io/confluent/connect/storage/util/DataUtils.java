/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.storage.util;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.util.Map;

public class DataUtils {

  public static Object getField(Object structOrMap, String fieldName) {
    if (structOrMap instanceof Struct) {
      return ((Struct) structOrMap).get(fieldName);
    } else if (structOrMap instanceof Map) {
      Object field = ((Map<?, ?>) structOrMap).get(fieldName);
      if (field == null) {
        throw new DataException(String.format("Unable to find nested field '%s'", fieldName));
      }
      return field;
    }
    throw new DataException(String.format(
          "Argument not a Struct or Map. Cannot get field '%s' from: %s",
          fieldName,
          structOrMap
    ));
  }

  public static Object getNestedFieldValue(Object structOrMap, String fieldName) {
    try {
      Object innermost = structOrMap;
      // Iterate down to final struct
      for (String name : fieldName.split("\\.")) {
        innermost = getField(innermost, name);
      }
      return innermost;
    } catch (DataException e) {
      throw new DataException(
            String.format("The nested field named '%s' does not exist.", fieldName),
            e
      );
    }
  }

  public static Field getNestedField(Schema schema, String fieldName) {
    final String[] fieldNames = fieldName.split("\\.");
    try {
      Field innermost = schema.field(fieldNames[0]);
      // Iterate down to final schema
      for (int i = 1; i < fieldNames.length; ++i) {
        innermost = innermost.schema().field(fieldNames[i]);
      }
      return innermost;
    } catch (DataException e) {
      throw new DataException(
            String.format("The nested field named '%s' does not exist.", fieldName),
            e
      );
    }
  }
}
