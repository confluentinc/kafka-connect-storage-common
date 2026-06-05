/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.connect.storage.backup;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps converter class names to schema type tags used in the backup envelope.
 * Uses a registry pattern for extensibility — new converter types can be
 * registered via {@link #register(String, String)}.
 */
public final class ConverterTypeDetector {

  private static final Map<String, String> KNOWN_TYPES = new LinkedHashMap<>();

  static {
    KNOWN_TYPES.put("io.confluent.connect.avro.AvroConverter", BackupEnvelope.TYPE_AVRO);
    KNOWN_TYPES.put("io.confluent.connect.protobuf.ProtobufConverter",
        BackupEnvelope.TYPE_PROTOBUF);
    KNOWN_TYPES.put("io.confluent.connect.json.JsonSchemaConverter",
        BackupEnvelope.TYPE_JSON_SCHEMA);
    KNOWN_TYPES.put("org.apache.kafka.connect.storage.StringConverter",
        BackupEnvelope.TYPE_STRING);
    KNOWN_TYPES.put("org.apache.kafka.connect.converters.IntegerConverter",
        BackupEnvelope.TYPE_INT32);
    KNOWN_TYPES.put("org.apache.kafka.connect.converters.LongConverter",
        BackupEnvelope.TYPE_INT64);
    KNOWN_TYPES.put("org.apache.kafka.connect.converters.ShortConverter",
        BackupEnvelope.TYPE_INT16);
    KNOWN_TYPES.put("org.apache.kafka.connect.converters.FloatConverter",
        BackupEnvelope.TYPE_FLOAT32);
    KNOWN_TYPES.put("org.apache.kafka.connect.converters.DoubleConverter",
        BackupEnvelope.TYPE_FLOAT64);
    KNOWN_TYPES.put("org.apache.kafka.connect.converters.ByteArrayConverter",
        BackupEnvelope.TYPE_BYTES);
  }

  private ConverterTypeDetector() {
  }

  /**
   * Register a custom converter type mapping. Call during connector startup
   * for converters not in the built-in list.
   */
  public static void register(String converterClassName, String schemaType) {
    KNOWN_TYPES.put(converterClassName, schemaType);
  }

  /**
   * Detect the schema type tag for a given converter class name.
   *
   * @param converterClass fully qualified converter class name
   * @param config task config map (used for JsonConverter schemas.enable check)
   * @param converterPrefix converter config prefix (e.g. "key.converter.")
   * @return the schema type tag, or "UNKNOWN" if not recognized
   */
  public static String detectSchemaType(
      String converterClass, Map<String, String> config, String converterPrefix) {
    if (converterClass == null) {
      return BackupEnvelope.TYPE_NONE;
    }

    // JsonConverter needs special handling: check schemas.enable
    if ("org.apache.kafka.connect.json.JsonConverter".equals(converterClass)) {
      String schemasEnable = config.get(converterPrefix + "schemas.enable");
      return "false".equalsIgnoreCase(schemasEnable)
          ? BackupEnvelope.TYPE_JSON_SCHEMALESS
          : BackupEnvelope.TYPE_JSON_EMBEDDED_SCHEMA;
    }

    String type = KNOWN_TYPES.get(converterClass);
    return type != null ? type : BackupEnvelope.TYPE_UNKNOWN;
  }
}
