/**
 * Copyright 2022 Pinterest, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.memq.core.slot;

import static org.junit.Assert.assertEquals;

import com.pinterest.memq.core.config.SlotAccountingConfig;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@code maxSlotStep}: EMA-driven acquisition/release closes the gap
 * between current and demanded slots gradually (at most {@code maxSlotStep} per
 * tick) instead of in one large jump, which would overshoot and oscillate.
 */
public class TestSlotManagerSlotStep {

  private static final int MB = 1024 * 1024;
  private static final String TOPIC = "topicA";
  private static final String PID = "p1";

  private SlotAccountingConfig config;

  @Before
  public void setUp() {
    config = new SlotAccountingConfig();
    config.setEnabled(true);
    config.setSlotSizeMbps(10.0);
    config.setSlotOverhead(0.0);
    config.setAcquireThresholdSeconds(0.0);
    config.setReleaseThresholdSeconds(0.0);
    config.setCooldownSeconds(0.0);
    // Tiny EMA window over a 1s tick -> EMA tracks the instantaneous rate, so
    // demand is reflected immediately and the only thing pacing slot changes is
    // the per-tick step clamp under test.
    config.setEmaWindowSeconds(0.001);
    config.setTickIntervalMs(1000);
    config.setPostEvictionCooldownSeconds(0.0);
    // Disable the drain latch; with plenty of free slots it would not engage,
    // but disabling keeps this test focused on the step clamp.
    config.setDrainLatchEnabled(false);
    config.setMaxSlotStep(1);
  }

  private SlotManager create() {
    // Large broker so free-slot capacity never limits acquisition.
    return new SlotManager(config, 100);
  }

  /** Sustain {@code mbPerTick} of demand for one tick. */
  private void driveOnce(SlotManager sm, int mbPerTick) {
    sm.recordWrite(PID, TOPIC, mbPerTick * MB);
    sm.tick();
  }

  @Test
  public void testAcquisitionClampedToOneSlotPerTick() {
    SlotManager sm = create();
    // Demand 11 slots' worth (110 vs slotSize 10) but expect a single slot per
    // tick rather than the whole +11 jump.
    driveOnce(sm, 110);
    assertEquals(1, sm.getTotalProducerSlots(PID));
    driveOnce(sm, 110);
    assertEquals(2, sm.getTotalProducerSlots(PID));
    driveOnce(sm, 110);
    assertEquals(3, sm.getTotalProducerSlots(PID));
  }

  @Test
  public void testAcquisitionRespectsLargerStep() {
    config.setMaxSlotStep(3);
    SlotManager sm = create();
    driveOnce(sm, 110);
    assertEquals(3, sm.getTotalProducerSlots(PID));
    driveOnce(sm, 110);
    assertEquals(6, sm.getTotalProducerSlots(PID));
  }

  @Test
  public void testClampDisabledTakesWholeGap() {
    config.setMaxSlotStep(0);
    SlotManager sm = create();
    driveOnce(sm, 110);
    assertEquals("disabled clamp grants the whole demanded gap at once",
        11, sm.getTotalProducerSlots(PID));
  }

  @Test
  public void testReleaseClampedToOneSlotPerTick() {
    SlotManager sm = create();
    // Ramp up to 5 slots one step at a time.
    for (int i = 0; i < 5; i++) {
      driveOnce(sm, 50);
    }
    assertEquals(5, sm.getTotalProducerSlots(PID));

    // Demand collapses to a single slot's worth; slots should be shed one per
    // tick, not dropped to 1 in a single step.
    driveOnce(sm, 5);
    assertEquals(4, sm.getTotalProducerSlots(PID));
    driveOnce(sm, 5);
    assertEquals(3, sm.getTotalProducerSlots(PID));
  }
}
