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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaManifest {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .enable(SerializationFeature.INDENT_OUTPUT)
      .setSerializationInclusion(JsonInclude.Include.NON_NULL);

  private static final String MANIFEST_VERSION = "1.0";

  private final String version;
  private final Map<String, SchemaEntry> schemas;

  public SchemaManifest() {
    this.version = MANIFEST_VERSION;
    this.schemas = new HashMap<>();
  }

  @JsonCreator
  public SchemaManifest(
      @JsonProperty("version") String version,
      @JsonProperty("schemas") Map<String, SchemaEntry> schemas,
      @JsonProperty("entries") List<SchemaEntry> entries) {
    this.version = version != null ? version : MANIFEST_VERSION;
    if (schemas != null && !schemas.isEmpty()) {
      this.schemas = new HashMap<>(schemas);
    } else if (entries != null) {
      this.schemas = new HashMap<>();
      for (SchemaEntry e : entries) {
        this.schemas.put(String.valueOf(e.getId()), e);
      }
    } else {
      this.schemas = new HashMap<>();
    }
  }

  @JsonProperty("version")
  public String getVersion() {
    return version;
  }

  @JsonProperty("schemas")
  public Map<String, SchemaEntry> getSchemas() {
    return Collections.unmodifiableMap(schemas);
  }

  public SchemaEntry getSchema(int globalId) {
    return schemas.get(String.valueOf(globalId));
  }

  public void addEntry(SchemaEntry entry) {
    schemas.put(String.valueOf(entry.getId()), entry);
  }

  public List<SchemaEntry> getEntries() {
    return Collections.unmodifiableList(new ArrayList<>(schemas.values()));
  }

  public byte[] toJson() throws JsonProcessingException {
    return MAPPER.writeValueAsBytes(this);
  }

  public static SchemaManifest fromJson(byte[] json) throws IOException {
    return MAPPER.readValue(json, SchemaManifest.class);
  }

  public static class SchemaEntry {
    private final int id;
    private final String type;
    private final String subject;
    private final int version;
    private final String file;
    private final List<SchemaReferenceEntry> references;
    private final String compatibility;

    @JsonCreator
    public SchemaEntry(
        @JsonProperty("id") int id,
        @JsonProperty("type") String type,
        @JsonProperty("subject") String subject,
        @JsonProperty("version") int version,
        @JsonProperty("file") String file,
        @JsonProperty("references") List<SchemaReferenceEntry> references,
        @JsonProperty("compatibility") String compatibility) {
      this.id = id;
      this.type = type;
      this.subject = subject;
      this.version = version;
      this.file = file;
      this.references = references != null ? references : Collections.emptyList();
      this.compatibility = compatibility;
    }

    @JsonProperty("id")
    public int getId() {
      return id;
    }

    @JsonProperty("type")
    public String getType() {
      return type;
    }

    @JsonProperty("subject")
    public String getSubject() {
      return subject;
    }

    @JsonProperty("version")
    public int getVersion() {
      return version;
    }

    @JsonProperty("file")
    public String getFile() {
      return file;
    }

    @JsonProperty("references")
    public List<SchemaReferenceEntry> getReferences() {
      return references;
    }

    @JsonProperty("compatibility")
    public String getCompatibility() {
      return compatibility;
    }

    public boolean hasReferences() {
      return references != null && !references.isEmpty();
    }
  }

  public static class SchemaReferenceEntry {
    private final String name;
    private final String subject;
    private final int version;
    private final int globalId;

    @JsonCreator
    public SchemaReferenceEntry(
        @JsonProperty("name") String name,
        @JsonProperty("subject") String subject,
        @JsonProperty("version") int version,
        @JsonProperty("globalId") int globalId) {
      this.name = name;
      this.subject = subject;
      this.version = version;
      this.globalId = globalId;
    }

    @JsonProperty("name")
    public String getName() {
      return name;
    }

    @JsonProperty("subject")
    public String getSubject() {
      return subject;
    }

    @JsonProperty("version")
    public int getVersion() {
      return version;
    }

    @JsonProperty("globalId")
    public int getGlobalId() {
      return globalId;
    }
  }
}
