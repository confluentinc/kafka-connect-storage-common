/*
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file exceptin compliance with the License.
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

package io.confluent.connect.storage.format;

import org.apache.kafka.connect.data.Schema;

import java.util.Collection;

/**
 * Interface for reading a schema from the storage.
 *
 * @param <C> Storage configuration type.
 * @param <S> Type used to discover objects in storage (e.g. Path in HDFS, String in S3).
 */
public interface SchemaFileReader<C, S> {
  Schema getSchema(C conf, S path);

  Collection<Object> readData(C conf, S path);
}
