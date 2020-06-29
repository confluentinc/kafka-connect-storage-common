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

}
