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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Map;

public class StorageCommonConfig extends AbstractConfig {

  // Common group

  public static final String STORAGE_CLASS_CONFIG = "storage.class";
  public static final String STORAGE_CLASS_DOC = "The underlying storage layer.";
  public static final String STORAGE_CLASS_DISPLAY = "Storage Class";

  public static final String STORE_URL_CONFIG = "store.url";
  public static final String STORE_URL_DOC = "Store's connection URL, if applicable.";
  public static final String STORE_URL_DEFAULT = null;
  public static final String STORE_URL_DISPLAY = "Store URL";

  public static final String TOPICS_DIR_CONFIG = "topics.dir";
  public static final String TOPICS_DIR_DOC = "Top level directory to store the data ingested from Kafka.";
  public static final String TOPICS_DIR_DEFAULT = "topics";
  public static final String TOPICS_DIR_DISPLAY = "Topics directory";

  public static final String DIRECTORY_DELIM_CONFIG = "directory.delim";
  public static final String DIRECTORY_DELIM_DOC = "Directory delimiter pattern";
  public static final String DIRECTORY_DELIM_DEFAULT = "/";
  public static final String DIRECTORY_DELIM_DISPLAY = "Directory Delimiter";

  protected static final ConfigDef CONFIG_DEF = new ConfigDef();

  static {
    {
      // Define Store's basic configuration group
      final String group = "Storage";
      int orderInGroup = 0;

      CONFIG_DEF.define(STORAGE_CLASS_CONFIG,
          Type.CLASS,
          Importance.HIGH,
          STORAGE_CLASS_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          STORAGE_CLASS_DISPLAY);

      CONFIG_DEF.define(STORE_URL_CONFIG,
          Type.STRING,
          STORE_URL_DEFAULT,
          Importance.HIGH,
          STORE_URL_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          STORE_URL_DISPLAY);

      CONFIG_DEF.define(TOPICS_DIR_CONFIG,
          Type.STRING,
          TOPICS_DIR_DEFAULT,
          Importance.HIGH,
          TOPICS_DIR_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          TOPICS_DIR_DISPLAY);

      CONFIG_DEF.define(DIRECTORY_DELIM_CONFIG,
          Type.STRING,
          DIRECTORY_DELIM_DEFAULT,
          Importance.MEDIUM,
          DIRECTORY_DELIM_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          DIRECTORY_DELIM_DISPLAY);
    }
  }

  private static boolean classNameEquals(String className, Class<?> clazz) {
    return className.equals(clazz.getSimpleName()) || className.equals(clazz.getCanonicalName());
  }

  public static ConfigDef getConfig() {
    return CONFIG_DEF;
  }

  public StorageCommonConfig(Map<String, String> props) {
    super(CONFIG_DEF, props);
  }

  public static void main(String[] args) {
    System.out.println(CONFIG_DEF.toRst());
  }
}
