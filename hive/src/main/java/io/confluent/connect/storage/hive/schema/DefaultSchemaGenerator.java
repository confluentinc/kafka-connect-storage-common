/*
 * Copyright 2017 Confluent Inc.
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

package io.confluent.connect.storage.hive.schema;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.connect.storage.common.SchemaGenerator;

public class DefaultSchemaGenerator implements SchemaGenerator<FieldSchema> {

  public DefaultSchemaGenerator() {

  }

  public DefaultSchemaGenerator(Map<String, Object> config) {
    // no configs are used
  }

  @Override
  public List<FieldSchema> newPartitionFields(String partitionFields) {
    String[] fields = partitionFields.split(",");
    List<FieldSchema> result = new ArrayList<>();

    for (String field : fields) {
      result.add(
          new FieldSchema(field, TypeInfoFactory.stringTypeInfo.toString(), "")
      );
    }

    return Collections.unmodifiableList(result);
  }
}
