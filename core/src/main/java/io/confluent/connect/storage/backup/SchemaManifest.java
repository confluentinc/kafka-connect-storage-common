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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Data model for per-schema entry.json files written during backup and read during restore.
 *
 * <p>Uses JsonNode tree API for serialization/deserialization instead of Jackson annotations
 * to avoid classloader-based annotation introspection issues in Kafka Connect's plugin
 * isolation environment.
 */
public final class SchemaManifest {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .enable(SerializationFeature.INDENT_OUTPUT);

  private static final int FORMAT_VERSION = 1;

  private static final String JSON_FORMAT = "format";
  private static final String JSON_ID = "id";
  private static final String JSON_TYPE = "type";
  private static final String JSON_SUBJECT = "subject";
  private static final String JSON_VERSION = "version";
  private static final String JSON_FILE = "file";
  private static final String JSON_REFERENCES = "references";
  private static final String JSON_NAME = "name";
  private static final String JSON_GLOBAL_ID = "globalId";

  private SchemaManifest() {
  }

  public static class SchemaEntry {
    private final String id;
    private final String type;
    private final String subject;
    private final int version;
    private final String file;
    private final List<SchemaReferenceEntry> references;

    public SchemaEntry(
        String id, String type, String subject, int version,
        String file, List<SchemaReferenceEntry> references) {
      this.id = id;
      this.type = type;
      this.subject = subject;
      this.version = version;
      this.file = file;
      this.references = references != null
          ? Collections.unmodifiableList(new ArrayList<>(references))
          : Collections.emptyList();
    }

    public String getId() {
      return id;
    }

    public String getType() {
      return type;
    }

    public String getSubject() {
      return subject;
    }

    public int getVersion() {
      return version;
    }

    public String getFile() {
      return file;
    }

    public List<SchemaReferenceEntry> getReferences() {
      return references;
    }

    public boolean hasReferences() {
      return references != null && !references.isEmpty();
    }

    public JsonNode toJson() {
      ObjectNode node = MAPPER.createObjectNode();
      node.put(JSON_FORMAT, FORMAT_VERSION);
      if (id != null) {
        node.put(JSON_ID, id);
      }
      if (type != null) {
        node.put(JSON_TYPE, type);
      }
      if (subject != null) {
        node.put(JSON_SUBJECT, subject);
      }
      node.put(JSON_VERSION, version);
      if (file != null) {
        node.put(JSON_FILE, file);
      }
      ArrayNode arr = node.putArray(JSON_REFERENCES);
      if (references != null) {
        for (SchemaReferenceEntry ref : references) {
          arr.add(ref.toJson());
        }
      }
      return node;
    }

    public static SchemaEntry fromJson(JsonNode node) {
      int format = node.path(JSON_FORMAT).asInt(0);
      if (format > FORMAT_VERSION) {
        throw new IllegalArgumentException(
            "Unsupported entry.json format version " + format
            + " (max supported: " + FORMAT_VERSION + ")");
      }
      if (!node.has(JSON_ID)) {
        throw new IllegalArgumentException(
            "Corrupt entry.json: missing required field '" + JSON_ID + "'");
      }
      List<SchemaReferenceEntry> refs = Collections.emptyList();
      JsonNode refsNode = node.get(JSON_REFERENCES);
      if (refsNode != null && refsNode.isArray()) {
        refs = new ArrayList<>();
        for (JsonNode refNode : refsNode) {
          refs.add(SchemaReferenceEntry.fromJson(refNode));
        }
      }
      return new SchemaEntry(
          node.path(JSON_ID).asText(null),
          node.path(JSON_TYPE).asText(null),
          node.path(JSON_SUBJECT).asText(null),
          node.path(JSON_VERSION).asInt(0),
          node.path(JSON_FILE).asText(null),
          refs
      );
    }

    public String toJsonString() throws JsonProcessingException {
      return MAPPER.writeValueAsString(toJson());
    }

    public static SchemaEntry fromJsonString(String json) throws IOException {
      return fromJson(MAPPER.readTree(json));
    }
  }

  public static class SchemaReferenceEntry {
    private final String name;
    private final String subject;
    private final int version;
    private final int globalId;

    public SchemaReferenceEntry(
        String name, String subject, int version, int globalId) {
      this.name = name;
      this.subject = subject;
      this.version = version;
      this.globalId = globalId;
    }

    public String getName() {
      return name;
    }

    public String getSubject() {
      return subject;
    }

    public int getVersion() {
      return version;
    }

    public int getGlobalId() {
      return globalId;
    }

    public JsonNode toJson() {
      ObjectNode node = MAPPER.createObjectNode();
      if (name != null) {
        node.put(JSON_NAME, name);
      }
      if (subject != null) {
        node.put(JSON_SUBJECT, subject);
      }
      node.put(JSON_VERSION, version);
      node.put(JSON_GLOBAL_ID, globalId);
      return node;
    }

    public static SchemaReferenceEntry fromJson(JsonNode node) {
      return new SchemaReferenceEntry(
          node.path(JSON_NAME).asText(null),
          node.path(JSON_SUBJECT).asText(null),
          node.path(JSON_VERSION).asInt(0),
          node.path(JSON_GLOBAL_ID).asInt(0)
      );
    }
  }
}
