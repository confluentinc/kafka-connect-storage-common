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

package io.confluent.connect.storage.format.backup;

import io.confluent.connect.storage.backup.SchemaBackupStore;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;

/**
 * Factory decorator that wraps a base format writer with envelope backup behavior.
 */
public class EnvelopeRecordWriterProvider<C> implements RecordWriterProvider<C> {

  private final RecordWriterProvider<C> delegate;
  private final SchemaBackupStore backupStore;
  private final String keySchemaType;
  private final String valueSchemaType;

  public EnvelopeRecordWriterProvider(
      RecordWriterProvider<C> delegate,
      SchemaBackupStore backupStore,
      String keySchemaType,
      String valueSchemaType) {
    this.delegate = delegate;
    this.backupStore = backupStore;
    this.keySchemaType = keySchemaType;
    this.valueSchemaType = valueSchemaType;
  }

  @Override
  public String getExtension() {
    return delegate.getExtension();
  }

  @Override
  public RecordWriter getRecordWriter(C conf, String fileName) {
    RecordWriter baseWriter = delegate.getRecordWriter(conf, fileName);
    return new EnvelopeRecordWriter(
        baseWriter, backupStore, keySchemaType, valueSchemaType);
  }
}
