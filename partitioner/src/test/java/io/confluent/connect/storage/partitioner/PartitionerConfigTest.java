package io.confluent.connect.storage.partitioner;

import static org.junit.Assert.assertEquals;

import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.common.GenericRecommender;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

public class PartitionerConfigTest extends StorageSinkTestBase {

  @Before
  public void setup() {
    properties = super.createProps();
  }

  @Test(expected = ConfigException.class)
  public void testInvalidTimezoneThrowsException() {
    properties.put(PartitionerConfig.TIMEZONE_CONFIG, "LLL");
    new PartitionerConfig(PartitionerConfig.newConfigDef(new GenericRecommender()), properties);
  }

  @Test
  public void testValidTimezoneAccepted() {
    properties.put(PartitionerConfig.TIMEZONE_CONFIG, "CET");
    new PartitionerConfig(PartitionerConfig.newConfigDef(new GenericRecommender()), properties);
  }

  @Test
  public void testTimezoneValidatedExceptionMessage() {
    properties.put(PartitionerConfig.TIMEZONE_CONFIG, "LLL");
    String expectedError = String.format("Invalid value LLL for configuration %s: "
        + "The datetime zone id 'LLL' is not recognised", PartitionerConfig.TIMEZONE_CONFIG);
    try {
      new PartitionerConfig(PartitionerConfig.newConfigDef(new GenericRecommender()), properties);
    } catch (ConfigException e){
      assertEquals(expectedError, e.getMessage());
    }
  }

  @Test
  public void testFieldPartitionerWithFieldsAccepted() {
    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        FieldPartitioner.class.getName()
    );
    properties.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "field");
    new PartitionerConfig(PartitionerConfig.newConfigDef(new GenericRecommender()), properties);
  }

  @Test
  public void testFieldPartitionerWithNestedFieldAccepted() {
    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        FieldPartitioner.class.getName()
    );
    properties.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "a.b.c");
    new PartitionerConfig(PartitionerConfig.newConfigDef(new GenericRecommender()), properties);
  }

  @Test
  public void testFieldPartitionerWithMultipleFieldsAccepted() {
    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        FieldPartitioner.class.getName()
    );
    properties.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "field1,nested.field2");
    new PartitionerConfig(PartitionerConfig.newConfigDef(new GenericRecommender()), properties);
  }

  @Test(expected = ConfigException.class)
  public void testFieldPartitionerWithEmptySegmentThrowsException() {
    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        FieldPartitioner.class.getName()
    );
    properties.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "a..b");
    new PartitionerConfig(PartitionerConfig.newConfigDef(new GenericRecommender()), properties);
  }

  @Test(expected = ConfigException.class)
  public void testFieldPartitionerWithLeadingDotThrowsException() {
    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        FieldPartitioner.class.getName()
    );
    properties.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, ".field");
    new PartitionerConfig(PartitionerConfig.newConfigDef(new GenericRecommender()), properties);
  }

  @Test(expected = ConfigException.class)
  public void testFieldPartitionerWithWhitespaceSegmentThrowsException() {
    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        FieldPartitioner.class.getName()
    );
    properties.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "a.   .b");
    new PartitionerConfig(PartitionerConfig.newConfigDef(new GenericRecommender()), properties);
  }

  @Test(expected = ConfigException.class)
  public void testFieldPartitionerWithMultipleDotsThrowsException() {
    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        FieldPartitioner.class.getName()
    );
    properties.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "..field");
    new PartitionerConfig(PartitionerConfig.newConfigDef(new GenericRecommender()), properties);
  }

  @Test(expected = ConfigException.class)
  public void testDefaultPartitionerWithInvalidFieldNameThrowsException() {
    properties.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "a..b");
    new PartitionerConfig(PartitionerConfig.newConfigDef(new GenericRecommender()), properties);
  }

  @Test
  public void testPartitionFieldNameEmptySegmentExceptionMessage() {
    properties.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "a..b");
    String expectedError = String.format("Invalid value a..b for configuration %s: "
        + "Field name cannot contain an empty or whitespace-only path segment",
        PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
    try {
      new PartitionerConfig(PartitionerConfig.newConfigDef(new GenericRecommender()), properties);
    } catch (ConfigException e) {
      assertEquals(expectedError, e.getMessage());
    }
  }

  @Test
  public void testEmptyPartitionFieldNameAccepted() {
    properties.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "");
    new PartitionerConfig(PartitionerConfig.newConfigDef(new GenericRecommender()), properties);
  }

}
