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

package io.confluent.connect.storage.tools;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SchemaSourceConnector extends SourceConnector {
  private static final String VERSION = loadKafkaVersion();

  private Map<String, String> config;

  @Override
  public String version() {
    return VERSION;
  }

  // Minimal inline equivalent of AppInfoParser.getVersion(): read the Kafka
  // version from /kafka/kafka-version.properties (shipped in kafka-clients)
  // instead of referencing AppInfoParser, whose class moved from
  // org.apache.kafka.common.utils to ...utils.internals in CP 8.4 / Apache
  // Kafka 4.x. Referencing no Kafka class keeps a single jar loadable on both
  // 8.3 and 8.4 workers.
  private static String loadKafkaVersion() {
    Properties props = new Properties();
    try (InputStream in = SchemaSourceConnector.class
        .getResourceAsStream("/kafka/kafka-version.properties")) {
      if (in != null) {
        props.load(in);
      }
    } catch (Exception e) {
      // ignore and fall back to "unknown"
    }
    return props.getProperty("version", "unknown").trim();
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = props;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SchemaSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for (Integer i = 0; i < maxTasks; i++) {
      Map<String, String> props = new HashMap<>(config);
      props.put(SchemaSourceTask.ID_CONFIG, i.toString());
      configs.add(props);
    }
    return configs;
  }

  @Override
  public void stop() {
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }
}
