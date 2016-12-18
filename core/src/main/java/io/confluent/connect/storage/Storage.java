/*
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file exceptin compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.storage;

import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;

import io.confluent.connect.storage.wal.WAL;

/**
 * Interface to distributed storage.
 *
 * @param <R> File listing that is returned when searching the storage contents.
 * @param <T> A filtering argument to restrict search of files to a given path in storage.
 * @param <C> The configuration of this storage.
 */
public interface Storage<R, T, C> extends Closeable {
  boolean exists(String filename);

  boolean mkdirs(String filename);

  void append(String filename, Object object);

  void delete(String filename);

  void commit(String tempFile, String committedFile);

  void close();

  WAL wal(String topicsDir, TopicPartition topicPart);

  R listStatus(String path, T filter);

  R listStatus(String path);

  String url();

  C conf();
}
