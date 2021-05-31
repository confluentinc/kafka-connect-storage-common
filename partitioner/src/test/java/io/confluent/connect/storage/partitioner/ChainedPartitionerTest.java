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

package io.confluent.connect.storage.partitioner;

import io.confluent.connect.storage.Storage;
import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.common.StorageCommonConfig;
import org.apache.avro.file.SeekableInput;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

public class ChainedPartitionerTest extends StorageSinkTestBase {

  private static final String FIELD_PARTITIONER_ALIAS = "field";
  private static final String TIME_BASED_PARTITIONER_ALIAS = "timeBased";

  private static final String TIME_ZONE = "America/Los_Angeles";
  private static final DateTimeZone DATE_TIME_ZONE = DateTimeZone.forID(TIME_ZONE);
  private static final String PATH_FORMAT = "'year'=YYYY/'month'=M/'day'=d";

  private static final int YEAR = 2020;
  private static final int MONTH = DateTimeConstants.JUNE;
  private static final int DAY = 1;
  public static final DateTime DATE_TIME =
      new DateTime(YEAR, MONTH, DAY, 1, 0, DATE_TIME_ZONE);

  @Test
  public void testEncodePartition() {
    String fieldName = "string,int";
    ChainedPartitioner<String> partitioner = createChainedPartitioner(fieldName);
    String path = getEncodedPartitionerPath(partitioner);

    Map<String, Object> m = new LinkedHashMap<>();
    m.put("string", "def");
    m.put("int", 12);
    m.put("year", YEAR);
    m.put("month", MONTH);
    m.put("day", DAY);
    assertThat(path, is(generateEncodedPartitionFromMap(m)));
  }

  @Test
  public void testNotAllowedPartitionerConfig() {
    final String configKey = PartitionerConfig.PATH_FORMAT_CONFIG;
    final String path = "";

    TimeBasedConfig timeBasedConfig = new TimeBasedConfig();
    timeBasedConfig.addConfig(configKey, path);

    ConfigException e = assertThrows(ConfigException.class, () -> {
      createChainedPartitioner(timeBasedConfig);
    });
    assertThat(e.getMessage(),
        endsWith("Path format cannot be empty."));
  }

  private <T> ChainedPartitioner<T> createChainedPartitioner(String fieldName) {
    FieldConfig fieldConfig = new FieldConfig(fieldName);
    TimeBasedConfig timeBasedConfig = new TimeBasedConfig();
    return createChainedPartitioner(fieldConfig, timeBasedConfig);
  }

  private <T> ChainedPartitioner<T> createChainedPartitioner(ConfigTemplate... configTemplates) {
    HashMap<String, Object> config = new HashMap<>();
    // storage common configs
    config.put(StorageCommonConfig.STORAGE_CLASS_CONFIG, SimpleStorage.class.getName());
    config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);

    List<String> aliasList = new ArrayList<>();
    for (ConfigTemplate ct : configTemplates) {
      aliasList.add(ct.getAlias());     // get configs alias
      config.putAll(ct.getConfig());    // combine config
    }
    config.put(PartitionerConfig.PARTITIONER_CHAIN_CONFIG, aliasList);

    ChainedPartitioner<T> partitioner = new ChainedPartitioner<>();
    partitioner.configure(config);
    return partitioner;
  }

  private <T> String getEncodedPartitionerPath(ChainedPartitioner<T> partitioner) {
    SinkRecord sinkRecord = createSinkRecord(DATE_TIME.getMillis());
    return partitioner.encodePartition(sinkRecord);
  }

  private static class FieldConfig extends ConfigTemplate {
    private FieldConfig(String fieldNames) {
      super(FIELD_PARTITIONER_ALIAS, FieldPartitioner.class);
      addConfig(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, fieldNames);
    }
  }

  private static class TimeBasedConfig extends ConfigTemplate {
    private TimeBasedConfig() {
      super(TIME_BASED_PARTITIONER_ALIAS, TimeBasedPartitioner.class);
      addConfig(PartitionerConfig.TIMEZONE_CONFIG, TIME_ZONE);
      addConfig(PartitionerConfig.PATH_FORMAT_CONFIG, PATH_FORMAT);
      addConfig(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Record");
      addConfig(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, "60000");
      addConfig(PartitionerConfig.LOCALE_CONFIG, Locale.US.toString());
    }
  }

  private static class ConfigTemplate {
    private final String alias;
    private final String prefix;
    private final Map<String, String> config;

    @SuppressWarnings("rawtypes")
    private ConfigTemplate(String alias, Class<? extends Partitioner> clazz) {
      this.alias = alias;
      this.prefix = PartitionerConfig.PARTITIONER_CHAIN_CONFIG + "." + alias + ".";
      this.config = new HashMap<>();
      this.config.put(prefix + "class", clazz.getName());
    }

    public void addConfig(String key, String value) {
      config.put(prefix + key, value);
    }

    public String getAlias() {
      return alias;
    }

    public Map<String, String> getConfig() {
      return config;
    }
  }

  private static class SimpleStorage implements Storage<StorageCommonConfig, List<String>> {

    @Override
    public boolean exists(String path) {
      return false;
    }

    @Override
    public boolean create(String path) {
      return false;
    }

    @Override
    public OutputStream create(String path, StorageCommonConfig conf, boolean overwrite) {
      return null;
    }

    @Override
    public SeekableInput open(String path, StorageCommonConfig conf) {
      return null;
    }

    @Override
    public OutputStream append(String path) {
      return null;
    }

    @Override
    public void delete(String path) {
    }

    @Override
    public List<String> list(String path) {
      return null;
    }

    @Override
    public void close() {
    }

    @Override
    public String url() {
      return null;
    }

    @Override
    public StorageCommonConfig conf() {
      return null;
    }
  }
}