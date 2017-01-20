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

package io.confluent.connect.storage.format;

import org.apache.kafka.connect.data.Schema;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Interface for reading a schema from the storage.
 *
 * @param <C> Storage configuration type.
 * @param <T> Type used to discover objects in storage (e.g. Path in HDFS, String in S3).
 */
public interface SchemaFileReader<C, T> extends Iterator<Object>, Iterable<Object>, Closeable {
  /**
   * Get the schema for this object at the given path.
   *
   * @param conf storage configuration.
   * @param path the path to the object.
   * @return the schema.
   */
  Schema getSchema(C conf, T path);
}
