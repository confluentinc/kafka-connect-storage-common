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

package io.confluent.connect.storage.format.parquet;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveMetaStore;
import io.confluent.connect.storage.hive.HiveUtil;
import org.apache.kafka.common.config.AbstractConfig;

public class ParquetFormat implements Format<AbstractConfig, AvroData, HiveMetaStore> {
  public RecordWriterProvider getRecordWriterProvider() {
    return new ParquetRecordWriterProvider();
  }

  public SchemaFileReader getSchemaFileReader(AvroData avroData) {
    return new ParquetFileReader(avroData);
  }

  public HiveUtil getHiveUtil(AbstractConfig config, AvroData avroData, HiveMetaStore hiveMetaStore) {
    return new ParquetHiveUtil(config, avroData, hiveMetaStore);
  }
}
