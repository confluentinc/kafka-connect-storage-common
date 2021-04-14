/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.connect.storage.wal;

/**
 * A class to keep track of an offset and filepath pair.
 *
 * <p>The offset may be extracted from the filepath in the same function. Extracting the offset from
 * the file path involves O(n) time complexity. This class can be used to link the extracted offset
 * to the filepath to avoid the re-computation of extracting the offset.</p>
 */
public class FilePathOffset {

  public final long extractedOffset;
  public final String filePath;

  public FilePathOffset(long extractedOffset, String filePath) {
    this.extractedOffset = extractedOffset;
    this.filePath = filePath;
  }

  public long getOffset() {
    return extractedOffset;
  }

  public String getFilePath() {
    return filePath;
  }

  @Override
  public String toString() {
    return "FilePathOffset{"
        + "extractedOffset=" + extractedOffset
        + ", filePath='" + filePath + '\''
        + '}';
  }
}
