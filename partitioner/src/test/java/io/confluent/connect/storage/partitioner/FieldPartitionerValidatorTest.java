package io.confluent.connect.storage.partitioner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.confluent.connect.storage.StorageSinkTestBase;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class FieldPartitionerValidatorTest extends StorageSinkTestBase {

  @Before
  public void setUp() {
    properties = super.createProps();
  }

  @Test
  public void testFieldPartitionerMissingFieldNameAddsError() {
    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        FieldPartitioner.class.getName()
    );

    Optional<String> error = new FieldPartitionerValidator(properties).validate();

    assertTrue(error.isPresent());
    assertEquals(
        "Partition field name cannot be null or empty when using FieldPartitioner.",
        error.get()
    );
  }

  @Test
  public void testFieldPartitionerEmptyFieldNameAddsError() {
    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        FieldPartitioner.class.getName()
    );
    properties.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "   ");

    Optional<String> error = new FieldPartitionerValidator(properties).validate();

    assertTrue(error.isPresent());
    assertEquals(
        "Partition field name cannot be null or empty when using FieldPartitioner.",
        error.get()
    );
  }

  @Test
  public void testNonFieldPartitionerDoesNotAddError() {
    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        DefaultPartitioner.class.getName()
    );

    Optional<String> error = new FieldPartitionerValidator(properties).validate();

    assertFalse(error.isPresent());
  }

  @Test
  public void testValidPartitionFieldNameDoesNotAddError() {
    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        FieldPartitioner.class.getName()
    );
    properties.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "validFieldName");

    Optional<String> error = new FieldPartitionerValidator(properties).validate();

    assertFalse(error.isPresent());
  }
}
