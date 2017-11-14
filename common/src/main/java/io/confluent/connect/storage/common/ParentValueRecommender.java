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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A recommender which decides visibility based on the value of a parent config.
 */
public class ParentValueRecommender implements ConfigDef.Recommender {

  protected String parentConfigName;
  protected Object parentConfigValue;

  /**
   * Construct a recommender with the name of parent config and its value for which {@link #visible}
   * returns true.
   *
   * @param parentConfigName the name of the parent config
   * @param parentConfigValue the value of the parent config for which this config
   *        will be visible (can be null).
   */
  public ParentValueRecommender(String parentConfigName, Object parentConfigValue) {
    if (parentConfigName == null) {
      throw new NullPointerException("parentConfigName cannot be null.");
    }
    this.parentConfigName = parentConfigName;
    this.parentConfigValue = parentConfigValue;
  }

  @Override
  public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
    return Collections.emptyList();
  }

  @Override
  public boolean visible(String name, Map<String, Object> connectorConfigs) {
    return Objects.equals(connectorConfigs.get(parentConfigName), parentConfigValue);
  }
}
