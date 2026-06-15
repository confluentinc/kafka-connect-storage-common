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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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

  static final ObjectMapper MAPPER = new ObjectMapper()
      .enable(SerializationFeature.INDENT_OUTPUT);

  static final int FORMAT_VERSION = 1;

  private SchemaManifest() {
  }

  public static class SchemaEntry {
    private final int id;
    private final String type;
    private final String subject;
    private final int version;
    private final String file;
    private final List<SchemaReferenceEntry> references;

    public SchemaEntry(
        int id, String type, String subject, int version,
        String file, List<SchemaReferenceEntry> references) {
      this.id = id;
      this.type = type;
      this.subject = subject;
      this.version = version;
      this.file = file;
      this.references = references != null ? references : Collections.emptyList();
    }

    public int getId() {
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
      node.put("format", FORMAT_VERSION);
      node.put("id", id);
      if (type != null) {
        node.put("type", type);
      }
      if (subject != null) {
        node.put("subject", subject);
      }
      node.put("version", version);
      if (file != null) {
        node.put("file", file);
      }
      ArrayNode arr = node.putArray("references");
      if (references != null) {
        for (SchemaReferenceEntry ref : references) {
          arr.add(ref.toJson());
        }
      }
      return node;
    }

    public static SchemaEntry fromJson(JsonNode node) {
      List<SchemaReferenceEntry> refs = Collections.emptyList();
      JsonNode refsNode = node.get("references");
      if (refsNode != null && refsNode.isArray()) {
        refs = new ArrayList<>();
        for (JsonNode refNode : refsNode) {
          refs.add(SchemaReferenceEntry.fromJson(refNode));
        }
      }
      return new SchemaEntry(
          node.path("id").asInt(0),
          node.path("type").asText(null),
          node.path("subject").asText(null),
          node.path("version").asInt(0),
          node.path("file").asText(null),
          refs
      );
    }

    public String toJsonString() throws com.fasterxml.jackson.core.JsonProcessingException {
      return MAPPER.writeValueAsString(toJson());
    }

    public static SchemaEntry fromJsonString(String json) throws java.io.IOException {
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
        node.put("name", name);
      }
      if (subject != null) {
        node.put("subject", subject);
      }
      node.put("version", version);
      node.put("globalId", globalId);
      return node;
    }

    public static SchemaReferenceEntry fromJson(JsonNode node) {
      return new SchemaReferenceEntry(
          node.path("name").asText(null),
          node.path("subject").asText(null),
          node.path("version").asInt(0),
          node.path("globalId").asInt(0)
      );
    }
  }
}
