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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.errors.DataException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Parses reference tree JSON from the Wrapper into manifest-compatible
 * {@link SchemaManifest.SchemaReferenceEntry} records for storage.
 */
public final class BackupReferenceParser {

  private static final ObjectMapper JSON = new ObjectMapper();

  private BackupReferenceParser() {
  }

  /**
   * Parse the direct references JSON using globalIds from the reference tree.
   * Only direct references are returned (not transitive ones).
   *
   * @param directRefsJson the JSON from Wrapper's directRefs field
   * @param referenceTreeJson the JSON from Wrapper's referenceTree field
   * @return list of direct reference entries, or empty list if either is null
   */
  @SuppressWarnings("unchecked")
  public static List<SchemaManifest.SchemaReferenceEntry> parseDirectRefsToManifestEntries(
      String directRefsJson, String referenceTreeJson) {
    if (directRefsJson == null || directRefsJson.isEmpty()
        || referenceTreeJson == null || referenceTreeJson.isEmpty()) {
      return Collections.emptyList();
    }
    try {
      List<Map<String, Object>> directRefs = JSON.readValue(directRefsJson,
          new TypeReference<List<Map<String, Object>>>() {});
      Map<String, Map<String, Object>> tree = JSON.readValue(
          referenceTreeJson,
          new TypeReference<Map<String, Map<String, Object>>>() {});

      List<SchemaManifest.SchemaReferenceEntry> result =
          new ArrayList<>();
      for (Map<String, Object> ref : directRefs) {
        result.add(toManifestEntry(ref, tree));
      }
      return result;
    } catch (IOException e) {
      throw new DataException(
          "Failed to parse reference JSON. Cannot guarantee "
          + "pristine restore.", e);
    }
  }

  private static SchemaManifest.SchemaReferenceEntry toManifestEntry(
      Map<String, Object> ref, Map<String, Map<String, Object>> tree) {
    String name = (String) ref.get(BackupEnvelope.REF_FIELD_NAME);
    String subject = (String) ref.get(BackupEnvelope.REF_FIELD_SUBJECT);
    int version = ref.get(BackupEnvelope.REF_FIELD_VERSION) instanceof Number
        ? ((Number) ref.get(BackupEnvelope.REF_FIELD_VERSION)).intValue() : 0;
    Map<String, Object> treeEntry = tree.get(name);
    if (treeEntry == null
        || !(treeEntry.get(BackupEnvelope.REF_FIELD_GLOBAL_ID) instanceof Number)) {
      throw new DataException(
          "Direct reference '" + name + "' has no matching entry with a "
          + "globalId in the reference tree. Cannot guarantee pristine "
          + "restore; the referenced schema file would be missing.");
    }
    int globalId =
        ((Number) treeEntry.get(BackupEnvelope.REF_FIELD_GLOBAL_ID)).intValue();
    if (globalId <= 0) {
      throw new DataException(
          "Direct reference '" + name + "' has invalid globalId=" + globalId
          + " in the reference tree. Cannot guarantee pristine restore.");
    }
    return new SchemaManifest.SchemaReferenceEntry(name, subject, version, globalId);
  }

}
