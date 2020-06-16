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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SchemaSourceTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(SchemaSourceTask.class);

  public static final String NAME_CONFIG = "name";
  public static final String ID_CONFIG = "id";
  public static final String TOPIC_CONFIG = "topic";
  public static final String NUM_MSGS_CONFIG = "num.messages";
  public static final String THROUGHPUT_CONFIG = "throughput";
  public static final String MULTIPLE_SCHEMA_CONFIG = "multiple.schema";
  public static final String PARTITION_COUNT_CONFIG = "partition.count";
  public static final String ENABLE_STDOUT_CONFIG = "enable.stdout";
  public static final String COMPLEX_KEYS_HEADERS = "complex.keys.headers";

  private static final String ID_FIELD = "id";
  private static final String SEQNO_FIELD = "seqno";

  private String name; // Connector name
  private int id; // Task ID
  private String topic;
  private Map<String, Integer> partition;
  private long startingSeqno;
  private long seqno;
  private long count;
  private long maxNumMsgs;
  private boolean multipleSchema;
  private int partitionCount;
  private boolean isOutputEnabled;
  private boolean complexKeysHeaders;

  // Until we can use ThroughputThrottler from Kafka, use a fixed
  // sleep interval. This isn't perfect, but close enough
  // for system testing purposes
  private long intervalMs;
  private int intervalNanos;

  private static Schema valueSchema = SchemaBuilder.struct().version(1).name("record")
      .field("boolean", Schema.BOOLEAN_SCHEMA)
      .field("int", Schema.INT32_SCHEMA)
      .field("long", Schema.INT64_SCHEMA)
      .field("float", Schema.FLOAT32_SCHEMA)
      .field("double", Schema.FLOAT64_SCHEMA)
      .field("partitioning", Schema.INT32_SCHEMA)
      .field("id", Schema.INT32_SCHEMA)
      .field("seqno", Schema.INT64_SCHEMA)
      .build();

  private static Schema valueSchema2 = SchemaBuilder.struct().version(2).name("record")
      .field("boolean", Schema.BOOLEAN_SCHEMA)
      .field("int", Schema.INT32_SCHEMA)
      .field("long", Schema.INT64_SCHEMA)
      .field("float", Schema.FLOAT32_SCHEMA)
      .field("double", Schema.FLOAT64_SCHEMA)
      .field("partitioning", Schema.INT32_SCHEMA)
      .field("string", SchemaBuilder.string().defaultValue("abc").build())
      .field("id", Schema.INT32_SCHEMA)
      .field("seqno", Schema.INT64_SCHEMA)
      .build();

  private static Schema simpleKeySchema = Schema.STRING_SCHEMA;
  private static Schema complexKeySchema = SchemaBuilder.struct().name("complexkeyschema")
      .field("boolean", Schema.BOOLEAN_SCHEMA)
      .field("long", Schema.INT64_SCHEMA)
      .field("float", Schema.FLOAT32_SCHEMA)
      .field("string", SchemaBuilder.string().defaultValue("key").build())
      .build();

  private static Struct recordValue1(int id, int partitionVal, long seqno) {
    return new Struct(valueSchema)
        .put("boolean", true)
        .put("int", 12)
        .put("long", 12L)
        .put("float", 12.2f)
        .put("double", 12.2)
        .put("partitioning", partitionVal)
        .put("id", id)
        .put("seqno", seqno);
  }

  private static Struct recordValue2(int id, int partitionVal, long seqno) {
    return new Struct(valueSchema2)
        .put("boolean", true)
        .put("int", 12)
        .put("long", 12L)
        .put("float", 12.2f)
        .put("double", 12.2)
        .put("partitioning", partitionVal)
        .put("string", "def")
        .put("id", id)
        .put("seqno", seqno);
  }

  private static Struct complexRecordKeyValue() {
    return new Struct(complexKeySchema)
        .put("boolean", false)
        .put("long", 8L)
        .put("float", 8.8f)
        .put("string", "complex-key-struct");
  }

  private static Iterable<Header> complexRecordHeaders() {
    return new ConnectHeaders()
        .addString("first-header-key", "first-header-value")
        .addLong("second-header-key", 8L)
        .addFloat("third-header-key", 6.5f);
  }

  public String version() {
    return new SchemaSourceConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    try {
      name = props.get(NAME_CONFIG);
      id = Integer.parseInt(props.get(ID_CONFIG));
      topic = props.get(TOPIC_CONFIG);
      maxNumMsgs = Long.parseLong(props.get(NUM_MSGS_CONFIG));
      multipleSchema = Boolean.parseBoolean(props.get(MULTIPLE_SCHEMA_CONFIG));
      complexKeysHeaders = Boolean.parseBoolean(props.get(COMPLEX_KEYS_HEADERS));
      partitionCount = Integer.parseInt(
          props.containsKey(PARTITION_COUNT_CONFIG)
          ? props.get(PARTITION_COUNT_CONFIG)
          : "1"
      );
      isOutputEnabled = Boolean.parseBoolean(
          props.containsKey(ENABLE_STDOUT_CONFIG)
          ? props.get(ENABLE_STDOUT_CONFIG)
          : "true"
      );
      String throughputStr = props.get(THROUGHPUT_CONFIG);
      if (throughputStr != null) {
        long throughput = Long.parseLong(throughputStr);
        long intervalTotalNanos = 1_000_000_000L / throughput;
        intervalMs = intervalTotalNanos / 1_000_000L;
        intervalNanos = (int) (intervalTotalNanos % 1_000_000L);
      } else {
        intervalMs = 0;
        intervalNanos = 0;
      }
    } catch (NumberFormatException e) {
      throw new ConnectException("Invalid SchemaSourceTask configuration", e);
    }

    partition = Collections.singletonMap(ID_FIELD, id);
    Map<String, Object> previousOffset = this.context.offsetStorageReader().offset(partition);
    if (previousOffset != null) {
      seqno = (Long) previousOffset.get(SEQNO_FIELD) + 1;
    } else {
      seqno = 0;
    }
    startingSeqno = seqno;
    count = 0;
    log.info(
        "Started SchemaSourceTask {}-{} producing to topic {} resuming from seqno {}",
        name,
        id,
        topic,
        startingSeqno
    );
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    if (count < maxNumMsgs) {
      if (intervalMs > 0 || intervalNanos > 0) {
        synchronized (this) {
          this.wait(intervalMs, intervalNanos);
        }
      }

      Map<String, Long> ccOffset = Collections.singletonMap(SEQNO_FIELD, seqno);
      int partitionVal = (int) (seqno % partitionCount);
      final SourceRecord srcRecord;

      Schema recordKeySchema = simpleKeySchema;
      Object recordKeyValue = "key";
      Iterable<Header> recordHeaders = null;

      if (complexKeysHeaders) {
        recordKeySchema = complexKeySchema;
        recordKeyValue = complexRecordKeyValue();
        recordHeaders = complexRecordHeaders();
      }

      if (!multipleSchema || count % 2 == 0) {
        srcRecord = new SourceRecord(
            partition,
            ccOffset,
            topic,
            id,
            recordKeySchema,
            recordKeyValue,
            valueSchema,
            recordValue1(id, partitionVal, seqno),
            null,
            recordHeaders
        );
      } else {
        srcRecord = new SourceRecord(
            partition,
            ccOffset,
            topic,
            id,
            recordKeySchema,
            recordKeyValue,
            valueSchema2,
            recordValue2(id, partitionVal, seqno),
            null,
            recordHeaders
        );
      }

      if (isOutputEnabled) {
        System.out.println("{\"task\": " + id + ", \"seqno\": " + seqno + "}");
      }
      List<SourceRecord> result = Arrays.asList(srcRecord);
      seqno++;
      count++;
      return result;
    } else {
      synchronized (this) {
        this.wait();
      }
      return new ArrayList<>();
    }
  }

  @Override
  public void stop() {
    synchronized (this) {
      this.notifyAll();
    }
  }
}
