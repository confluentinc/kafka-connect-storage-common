/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.connect.storage;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StorageSinkConnectorConfigTest {

  /**
   * Minimum props to instantiate StorageSinkConnectorConfig. FORMAT_CLASS
   * has no default in the ConfigDef, so any test that instantiates the
   * config must supply it.
   */
  private static Map<String, String> minimalProps() {
    Map<String, String> props = new HashMap<>();
    // format.class is Type.CLASS with no default; any loadable class name
    // satisfies the parser. Behavior of the format is not exercised here.
    props.put(StorageSinkConnectorConfig.FORMAT_CLASS_CONFIG,
        "java.lang.Object");
    props.put(StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG, "1000");
    return props;
  }

  /**
   * Trivial no-op recommender to satisfy newConfigDef's required parameters.
   * Tests here do not exercise recommender behavior.
   */
  private static ConfigDef.Recommender noOpRecommender() {
    return new ConfigDef.Recommender() {
      @Override
      public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
        return Collections.emptyList();
      }

      @Override
      public boolean visible(String name, Map<String, Object> parsedConfig) {
        return true;
      }
    };
  }

  @Test
  public void newConfigDefWithoutBackupOptInOmitsBackupKeys() {
    ConfigDef configDef = StorageSinkConnectorConfig.newConfigDef(
        noOpRecommender(), noOpRecommender());

    assertFalse(configDef.configKeys().containsKey(
        StorageSinkConnectorConfig.MODE_CONFIG));
    assertFalse(configDef.configKeys().containsKey(
        StorageSinkConnectorConfig.FORMAT_JSON_SCHEMA_ENABLE_CONFIG));
  }

  @Test
  public void addBackupModeConfigsDefinesBackupKeys() {
    ConfigDef configDef = StorageSinkConnectorConfig.newConfigDef(
        noOpRecommender(), noOpRecommender());
    StorageSinkConnectorConfig.addBackupModeConfigs(configDef);

    assertTrue(configDef.configKeys().containsKey(
        StorageSinkConnectorConfig.MODE_CONFIG));
    assertTrue(configDef.configKeys().containsKey(
        StorageSinkConnectorConfig.FORMAT_JSON_SCHEMA_ENABLE_CONFIG));
    assertEquals(Boolean.FALSE, configDef.configKeys()
        .get(StorageSinkConnectorConfig.FORMAT_JSON_SCHEMA_ENABLE_CONFIG)
        .defaultValue);
  }

  @Test
  public void modeReturnsGenericWhenConfigKeyAbsent() {
    ConfigDef configDef = StorageSinkConnectorConfig.newConfigDef(
        noOpRecommender(), noOpRecommender());
    StorageSinkConnectorConfig config = new StorageSinkConnectorConfig(
        configDef, minimalProps());

    assertEquals(StorageSinkConnectorConfig.Mode.GENERIC, config.mode());
    assertFalse(config.isBackupMode());
  }

  @Test
  public void isJsonSchemaEmbeddedReturnsFalseWhenConfigKeyAbsent() {
    ConfigDef configDef = StorageSinkConnectorConfig.newConfigDef(
        noOpRecommender(), noOpRecommender());
    StorageSinkConnectorConfig config = new StorageSinkConnectorConfig(
        configDef, minimalProps());

    assertFalse(config.isJsonSchemaEmbedded());
  }

  @Test
  public void isJsonSchemaEmbeddedReturnsDefaultFalseWithOptInAndUnsetProp() {
    ConfigDef configDef = StorageSinkConnectorConfig.newConfigDef(
        noOpRecommender(), noOpRecommender());
    StorageSinkConnectorConfig.addBackupModeConfigs(configDef);
    StorageSinkConnectorConfig config = new StorageSinkConnectorConfig(
        configDef, minimalProps());

    assertFalse(config.isJsonSchemaEmbedded());
  }

  @Test
  public void isJsonSchemaEmbeddedReturnsTrueWhenSet() {
    ConfigDef configDef = StorageSinkConnectorConfig.newConfigDef(
        noOpRecommender(), noOpRecommender());
    StorageSinkConnectorConfig.addBackupModeConfigs(configDef);
    Map<String, String> props = minimalProps();
    props.put(StorageSinkConnectorConfig.FORMAT_JSON_SCHEMA_ENABLE_CONFIG, "true");
    StorageSinkConnectorConfig config = new StorageSinkConnectorConfig(
        configDef, props);

    assertTrue(config.isJsonSchemaEmbedded());
  }

  @Test
  public void modeReturnsBackupWhenOptInAndSet() {
    ConfigDef configDef = StorageSinkConnectorConfig.newConfigDef(
        noOpRecommender(), noOpRecommender());
    StorageSinkConnectorConfig.addBackupModeConfigs(configDef);
    Map<String, String> props = minimalProps();
    props.put(StorageSinkConnectorConfig.MODE_CONFIG,
        StorageSinkConnectorConfig.Mode.BACKUP_FULL_RECORD.name());
    StorageSinkConnectorConfig config = new StorageSinkConnectorConfig(
        configDef, props);

    assertEquals(StorageSinkConnectorConfig.Mode.BACKUP_FULL_RECORD, config.mode());
    assertTrue(config.isBackupMode());
  }
}
