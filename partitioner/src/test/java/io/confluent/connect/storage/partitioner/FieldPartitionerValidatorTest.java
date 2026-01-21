package io.confluent.connect.storage.partitioner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.common.GenericRecommender;
import java.util.List;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Before;
import org.junit.Test;

public class FieldPartitionerValidatorTest extends StorageSinkTestBase {

  private ConfigDef configDef;

  @Before
  public void setUp() {
    properties = super.createProps();
    configDef = PartitionerConfig.newConfigDef(new GenericRecommender());
  }

  @Test
  public void testFieldPartitionerMissingFieldNameAddsError() {
    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        FieldPartitioner.class.getName()
    );

    Config config = new Config(configDef.validate(properties));
    new FieldPartitionerValidator(properties, config).validate();

    ConfigValue fieldNameValue = getConfigValue(config,
        PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
    List<String> errors = fieldNameValue.errorMessages();
    assertEquals(1, errors.size());
    assertEquals(
        "Partition field name cannot be null or empty when using FieldPartitioner.",
        errors.get(0)
    );
  }

  @Test
  public void testFieldPartitionerEmptyFieldNameAddsError() {
    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        FieldPartitioner.class.getName()
    );
    properties.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "   ");

    Config config = new Config(configDef.validate(properties));
    new FieldPartitionerValidator(properties, config).validate();

    ConfigValue fieldNameValue = getConfigValue(config,
        PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
    assertEquals(1, fieldNameValue.errorMessages().size());
  }

  @Test
  public void testNonFieldPartitionerDoesNotAddError() {
    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        DefaultPartitioner.class.getName()
    );

    Config config = new Config(configDef.validate(properties));
    new FieldPartitionerValidator(properties, config).validate();

    ConfigValue fieldNameValue = getConfigValue(config,
        PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
    assertTrue(fieldNameValue.errorMessages().isEmpty());
  }

  @Test
  public void testValidPartitionFieldNameDoesNotAddError() {
    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        FieldPartitioner.class.getName()
    );
    properties.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "validFieldName");

    Config config = new Config(configDef.validate(properties));
    new FieldPartitionerValidator(properties, config).validate();

    ConfigValue fieldNameValue = getConfigValue(config,
        PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
    assertTrue(fieldNameValue.errorMessages().isEmpty());
  }

  private ConfigValue getConfigValue(Config config, String name) {
    for (ConfigValue configValue : config.configValues()) {
      if (name.equals(configValue.name())) {
        return configValue;
      }
    }
    throw new AssertionError("Missing ConfigValue for " + name);
  }
}
