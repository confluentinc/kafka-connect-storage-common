/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.storage;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.velocity.exception.MethodInvocationException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class StorageFactory {
  public static <T extends Storage<?, ?, C>, C> T createStorage(Class<T> storageClass, Class<C> confClass, C conf,
                                                                String url) {
    try {
      Constructor<T> ctor =
          storageClass.getConstructor(confClass, String.class);
      return ctor.newInstance(conf, url);
    } catch (NoSuchMethodException | InvocationTargetException | MethodInvocationException | InstantiationException | IllegalAccessException e) {
      throw new ConnectException(e);
    }
  }
}
