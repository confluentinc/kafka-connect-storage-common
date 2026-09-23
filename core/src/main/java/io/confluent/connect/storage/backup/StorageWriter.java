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

/**
 * Abstraction for writing backup metadata (schema files, entry files)
 * to object storage. Each storage backend provides its own implementation.
 *
 * <p>All writes are idempotent — same schemaId always produces same content.
 * No CAS or versioning needed.
 */
public interface StorageWriter {

  /**
   * Write string content to the given path, overwriting if exists.
   */
  void write(String path, String content);

  /**
   * Check if a path exists in storage.
   * Used for three-level dedup: skip writing if entry file already exists.
   */
  boolean exists(String path);
}
