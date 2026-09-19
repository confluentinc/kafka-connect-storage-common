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

import org.junit.Test;

import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class SchemaSourceConnectorTest {

  /**
   * version() must always return a non-null, non-empty string and never throw.
   * This is the property that keeps the plugin scan safe on CP 8.4 (Apache Kafka
   * 4.x), where {@code Versioned.version()} is abstract: a connector whose
   * version() throws (e.g. a NoClassDefFoundError from the relocated
   * AppInfoParser) fails the scan.
   */
  @Test
  public void versionIsNeverNullOrEmpty() {
    String version = new SchemaSourceConnector().version();
    assertNotNull("version() must not return null", version);
    assertFalse("version() must not return an empty string", version.isEmpty());
  }

  /**
   * version() reads the Kafka version from /kafka/kafka-version.properties
   * (shipped in kafka-clients), the same resource AppInfoParser reads. Read it
   * independently here and confirm version() returns exactly that value -- so
   * the method is wired to the resource, not a hardcoded or stale string. When
   * kafka-clients is on the classpath (as in this build) both are the real
   * Kafka version; if the resource were ever absent, both would be "unknown".
   */
  @Test
  public void versionMatchesKafkaVersionProperties() throws Exception {
    Properties props = new Properties();
    try (InputStream in = getClass().getResourceAsStream("/kafka/kafka-version.properties")) {
      if (in != null) {
        props.load(in);
      }
    }
    String expected = props.getProperty("version", "unknown").trim();
    assertEquals(expected, new SchemaSourceConnector().version());
  }
}
