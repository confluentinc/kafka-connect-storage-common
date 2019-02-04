package io.confluent.connect.storage.hive;

import io.confluent.connect.storage.errors.HiveMetaStoreException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static io.confluent.connect.storage.hive.HiveConfig.HIVE_TABLE_PATTERN_CONFIG;
import static io.confluent.connect.storage.hive.HiveConfig.HIVE_TABLE_PATTERN_DEFAULT;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class HiveMetaStoreTableNameConverterFactoryTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Parameterized.Parameter(0)
  public String hivePatternConfig;

  @Parameterized.Parameter(1)
  public String sourceTableName;

  @Parameterized.Parameter(2)
  public String convertedTableName;

  @Parameterized.Parameter(3)
  public Class<Throwable> exception;

  @Parameterized.Parameters(name = "pattern='{0}' sourceTableName='{1}' convertedTableName='{2}'")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
            {HIVE_TABLE_PATTERN_DEFAULT, "topic_name", "topic_name", null},
            {HIVE_TABLE_PATTERN_DEFAULT, "topic-name", "topic_name", null},
            {HIVE_TABLE_PATTERN_DEFAULT, "topic.name", "topic_name", null},
            {"^.*\\.(.*)$", "topic.name", "name", null},
            {"^.*\\.(.*)$", "topic-name", null, HiveMetaStoreException.class}
    });
  }

  @Test
  public void test() {
    Map<String, String> properties = new HashMap<>();
    properties.put(HIVE_TABLE_PATTERN_CONFIG, hivePatternConfig);
    HiveConfig hiveConfig = new HiveConfig(properties);

    HiveMetaStore hiveMetaStore = new HiveMetaStore(hiveConfig);

    if (exception != null) {
      thrown.expect(exception);
    }

    assertEquals(hiveMetaStore.tableNameConverter(sourceTableName), convertedTableName);
  }
}
