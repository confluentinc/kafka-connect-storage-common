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

package io.confluent.connect.storage.common;

import org.apache.kafka.common.config.ConfigDef;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class GenericClassRecommender implements ConfigDef.Recommender {
  private final List<Object> validValues = new ArrayList<>();

  public synchronized void addValidValues(Collection<Object> values) {
    validValues.addAll(values);
  }

  @Override
  public synchronized List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
    return validValues;
  }

  @Override
  public synchronized boolean visible(String name, Map<String, Object> connectorConfigs) {
    return true;
  }
}
