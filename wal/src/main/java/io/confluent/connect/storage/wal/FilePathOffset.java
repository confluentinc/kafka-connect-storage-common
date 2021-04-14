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
    return "FilePathOffset{" +
        "extractedOffset=" + extractedOffset +
        ", filePath='" + filePath + '\'' +
        '}';
  }
}
