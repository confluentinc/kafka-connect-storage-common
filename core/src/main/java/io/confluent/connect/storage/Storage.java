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

package io.confluent.connect.storage;

import org.apache.avro.file.SeekableInput;

import java.io.Closeable;
import java.io.OutputStream;

/**
 * Interface to distributed storage.
 *
 * Depending on the storage implementation, an object corresponds to a file or a directory in a distributed filesystem
 * or an object in an object store. Similarly, a path corresponds to an actual path in a distributed filesystem or a
 * lookup key in an object store.
 *
 * @param <C> The configuration of this storage.
 * @param <R> Object listing that is returned when searching the storage contents.
 */
public interface Storage<C, R> extends Closeable {

  /**
   * Returns whether an object exists.
   *
   * @param path the path to the object.
   * @return true if object exists, false otherwise.
   * @throws DataException if the call to the underlying distributed storage failed.
   */
  boolean exists(String path);

  /**
   * Creates an object container (e.g. a directory or a bucket).
   *
   * @param path the path of the container
   * @return true if the container does not exist and was successfully created; false otherwise.
   */
  boolean create(String path);

  /**
   * Open for reading an object at the given path.
   *
   * @param path the path of the object to be read.
   * @param conf storage configuration.
   * @return a seek-able input stream associated with the requested object.
   */
  SeekableInput open(String path, C conf);

  /**
   * Creates a new object in the given path and with the given configuration.
   *
   * @param path the path of the object to be created.
   * @param conf storage configuration.
   * @param overwrite whether to override an existing object with the same path (optional operation).
   * @return an output stream associated with the new object.
   */
  OutputStream create(String path, C conf, boolean overwrite);

  /**
   * Append data to an existing object at the given path.
   *
   * @param path the path of the object to be appended.
   * @return an output stream associated with the existing object.
   */
  OutputStream append(String path);

  /**
   * Delete the given object or container.
   *
   * @param path the path to the object or container to delete.
   */
  void delete(String path);

  /**
   * List the contents of the storage at a given path.
   *
   * @param path the path.
   * @return the listing of the contents.
   */
  R list(String path);

  /**
   * Stop using this storage.
   */
  void close();

  /**
   * Get the storage endpoint.
   *
   * @return the storage endpoint as a string.
   */
  String url();

  /**
   * Get the storage configuration.
   *
   * @return the storage configuration.
   */
  C conf();

}
