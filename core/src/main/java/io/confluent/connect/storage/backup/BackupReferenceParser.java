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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger log = LoggerFactory.getLogger(BackupReferenceParser.class);
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
        String name = (String) ref.get("name");
        String subject = (String) ref.get("subject");
        int version = ref.get("version") instanceof Number
            ? ((Number) ref.get("version")).intValue() : 0;
        int globalId = 0;
        Map<String, Object> treeEntry = tree.get(name);
        if (treeEntry != null
            && treeEntry.get("globalId") instanceof Number) {
          globalId =
              ((Number) treeEntry.get("globalId")).intValue();
        }
        result.add(new SchemaManifest.SchemaReferenceEntry(
            name, subject, version, globalId));
      }
      return result;
    } catch (IOException e) {
      throw new org.apache.kafka.connect.errors.DataException(
          "Failed to parse reference JSON. Cannot guarantee "
          + "pristine restore.", e);
    }
  }

}
