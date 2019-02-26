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

package io.confluent.connect.storage.common.util;

/**
 * Simple string utilities, not present in java library, that are not worth importing a dependency.
 */
public class StringUtils {

  public static boolean isBlank(String string) {
    return string == null || string.isEmpty() || string.trim().isEmpty();
  }

  public static boolean isNotBlank(String string) {
    return !isBlank(string);
  }
}
