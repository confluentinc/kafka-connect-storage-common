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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Applies backup-mode-specific config defaults to a connector props map before the
 * typed {@code AbstractConfig} is constructed. CSP sink connectors call this from
 * their {@code start(Map&lt;String,String&gt;)} method BEFORE {@code super.start(props)}
 * so the overridden values are visible to the config constructor.
 *
 * <p>Currently handles: {@code filename.offset.zero.pad.width = 20} for
 * {@code BACKUP_FULL_RECORD} mode. The default of 10 only safely covers up to
 * ~10B lifetime offsets per partition before lex sort inverts (restore cursor
 * stops seeing new files). 20 digits covers the full Kafka offset range
 * ({@code Long.MAX_VALUE} = 9.22 x 10^18, 19 digits) with one digit of headroom.
 */
public final class BackupModeConfigDefaults {

  private static final Logger log = LoggerFactory.getLogger(BackupModeConfigDefaults.class);

  private static final String MODE_CONFIG = "mode";
  private static final String BACKUP_FULL_RECORD_MODE = "BACKUP_FULL_RECORD";

  private static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG =
      "filename.offset.zero.pad.width";
  private static final int BACKUP_MODE_MIN_PAD_WIDTH = 20;

  private BackupModeConfigDefaults() {
  }

  /**
   * Returns a props map with backup-mode defaults applied. The input map is never
   * mutated; a new map is returned only when an override is needed.
   */
  public static Map<String, String> applyBackupModeDefaults(Map<String, String> props) {
    if (!BACKUP_FULL_RECORD_MODE.equalsIgnoreCase(props.get(MODE_CONFIG))) {
      return props;
    }
    String current = props.get(FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG);
    if (current != null && parseIntSafe(current) >= BACKUP_MODE_MIN_PAD_WIDTH) {
      return props;
    }
    Map<String, String> adjusted = new HashMap<>(props);
    adjusted.put(FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG,
        Integer.toString(BACKUP_MODE_MIN_PAD_WIDTH));
    log.info("Auto-set {}={} for mode {} (default 10 limits lex-safe range to "
        + "~10B offsets per partition; 20 covers the full Kafka offset range).",
        FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG, BACKUP_MODE_MIN_PAD_WIDTH,
        BACKUP_FULL_RECORD_MODE);
    return adjusted;
  }

  private static int parseIntSafe(String s) {
    try {
      return Integer.parseInt(s);
    } catch (NumberFormatException e) {
      return -1;
    }
  }
}
