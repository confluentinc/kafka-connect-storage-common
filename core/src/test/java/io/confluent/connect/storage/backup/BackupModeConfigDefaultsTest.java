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

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class BackupModeConfigDefaultsTest {

  private static final String MODE_CONFIG = "mode";
  private static final String BACKUP_MODE = "BACKUP_FULL_RECORD";
  private static final String GENERIC_MODE = "GENERIC";
  private static final String PAD_WIDTH_CONFIG = "filename.offset.zero.pad.width";

  private Map<String, String> props(String mode, String padWidth) {
    Map<String, String> p = new HashMap<>();
    if (mode != null) {
      p.put(MODE_CONFIG, mode);
    }
    if (padWidth != null) {
      p.put(PAD_WIDTH_CONFIG, padWidth);
    }
    return p;
  }

  @Test
  public void genericModeIsUntouched() {
    Map<String, String> in = props(GENERIC_MODE, "10");
    Map<String, String> out = BackupModeConfigDefaults.applyBackupModeDefaults(in);
    assertSame("same map returned for non-backup mode", in, out);
    assertEquals("10", out.get(PAD_WIDTH_CONFIG));
  }

  @Test
  public void genericModeWith25IsUntouched() {
    Map<String, String> in = props(GENERIC_MODE, "25");
    Map<String, String> out = BackupModeConfigDefaults.applyBackupModeDefaults(in);
    assertSame("user retains full control of pad width outside backup mode", in, out);
    assertEquals("25", out.get(PAD_WIDTH_CONFIG));
  }

  @Test
  public void missingModeIsUntouched() {
    Map<String, String> in = props(null, "10");
    Map<String, String> out = BackupModeConfigDefaults.applyBackupModeDefaults(in);
    assertSame(in, out);
  }

  @Test
  public void backupModeWithNoPadWidthSetsTo20() {
    Map<String, String> in = props(BACKUP_MODE, null);
    Map<String, String> out = BackupModeConfigDefaults.applyBackupModeDefaults(in);
    assertNotSame("new map returned when override applied", in, out);
    assertEquals("20", out.get(PAD_WIDTH_CONFIG));
    // Input map not mutated.
    assertEquals(null, in.get(PAD_WIDTH_CONFIG));
  }

  @Test
  public void backupModeWithDefault10OverridesTo20() {
    Map<String, String> in = props(BACKUP_MODE, "10");
    Map<String, String> out = BackupModeConfigDefaults.applyBackupModeDefaults(in);
    assertEquals("20", out.get(PAD_WIDTH_CONFIG));
  }

  @Test
  public void backupModeWithPadWidth19OverridesTo20() {
    Map<String, String> in = props(BACKUP_MODE, "19");
    Map<String, String> out = BackupModeConfigDefaults.applyBackupModeDefaults(in);
    assertEquals("20", out.get(PAD_WIDTH_CONFIG));
  }

  @Test
  public void backupModeWithPadWidth20IsPreserved() {
    Map<String, String> in = props(BACKUP_MODE, "20");
    Map<String, String> out = BackupModeConfigDefaults.applyBackupModeDefaults(in);
    assertSame("no override needed when already equal to 20", in, out);
    assertEquals("20", out.get(PAD_WIDTH_CONFIG));
  }

  @Test
  public void backupModeWithPadWidth25OverridesTo20() {
    Map<String, String> in = props(BACKUP_MODE, "25");
    Map<String, String> out = BackupModeConfigDefaults.applyBackupModeDefaults(in);
    assertNotSame("values above 20 are overridden back down to 20 in backup mode", in, out);
    assertEquals("20", out.get(PAD_WIDTH_CONFIG));
  }

  @Test
  public void backupModeWithPadWidth100OverridesTo20() {
    Map<String, String> in = props(BACKUP_MODE, "100");
    Map<String, String> out = BackupModeConfigDefaults.applyBackupModeDefaults(in);
    assertEquals("20", out.get(PAD_WIDTH_CONFIG));
  }

  @Test
  public void backupModeWithNonNumericPadWidthOverridesTo20() {
    // Defensive: any non-"20" value, parseable or not, is replaced.
    Map<String, String> in = props(BACKUP_MODE, "bogus");
    Map<String, String> out = BackupModeConfigDefaults.applyBackupModeDefaults(in);
    assertEquals("20", out.get(PAD_WIDTH_CONFIG));
  }

  @Test
  public void modeCheckIsCaseInsensitive() {
    Map<String, String> in = props("backup_full_record", "10");
    Map<String, String> out = BackupModeConfigDefaults.applyBackupModeDefaults(in);
    assertEquals("20", out.get(PAD_WIDTH_CONFIG));
  }
}
