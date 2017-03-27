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

import org.apache.kafka.connect.errors.ConnectException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * A factory of storage instances.
 */
public class StorageFactory {

  /**
   * Instantiate a specific storage type, given a configuration
   * and its class along with the storage address.
   *
   * @param storageClass the class of the storage.
   * @param confClass the class of the configuration.
   * @param conf the configuration contents.
   * @param url the address of the storage.
   * @param <S> the storage type.
   * @param <C> the configuration type.
   * @return the storage instance upon success.
   * @throws ConnectException on error.
   */
  public static <S extends Storage<C, ?>, C> S createStorage(
      Class<S> storageClass,
      Class<C> confClass,
      C conf,
      String url
  ) {
    try {
      Constructor<S> constructor =
          storageClass.getConstructor(confClass, String.class);
      return constructor.newInstance(conf, url);
    } catch (NoSuchMethodException
        | InvocationTargetException
        | InstantiationException
        | IllegalAccessException e) {
      throw new ConnectException(e);
    }
  }
}
