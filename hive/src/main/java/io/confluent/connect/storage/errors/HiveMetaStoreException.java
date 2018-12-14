/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.storage.errors;

import org.apache.kafka.connect.errors.ConnectException;

public class HiveMetaStoreException extends ConnectException {

  public HiveMetaStoreException(String s) {
    super(s);
  }

  public HiveMetaStoreException(String s, Throwable throwable) {
    super(s, throwable);
  }

  public HiveMetaStoreException(Throwable throwable) {
    super(throwable);
  }
}
