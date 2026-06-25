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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.pinterest.memq.core.config.SlotAccountingConfig;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the occupancy drain latch: while the broker has recently been at
 * near-zero free slots, slot acquisition is frozen so that capacity freed by
 * eviction is not immediately reacquired (the backpressure "flap"). The latch
 * disengages once the broker has genuinely drained.
 */
public class TestSlotManagerDrainLatch {

  private static final int MB = 1024 * 1024;
  private static final String TOPIC = "topicA";

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
    config.setEmaWindowSeconds(0.001);
    config.setTickIntervalMs(1000);
    // Isolate the latch from the post-eviction cooldown so a freed slot is
    // gated only by the latch under test.
    config.setPostEvictionCooldownSeconds(0.0);
    config.setDrainLatchEnabled(true);
    // 1s window over a 1s tick -> ~0.37 decay per tick, so a few ticks move
    // the free-slots EMA appreciably.
    config.setDrainLatchEmaWindowSeconds(1.0);
    config.setDrainLatchDisengageFreeSlots(2.0);
    // These tests fill slots in a single tick; opt out of the per-tick step
    // clamp (covered by TestSlotManagerSlotStep) to keep that assumption.
    config.setMaxSlotStep(0);
  }

  private SlotManager create(int totalSlots) {
    return new SlotManager(config, totalSlots);
  }

  /** Two producers x 2 slots each fills a 4-slot broker. */
  private void fillAllSlots(SlotManager sm) {
    sm.recordWrite("p1", TOPIC, 15 * MB);
    sm.recordWrite("p2", TOPIC, 15 * MB);
    sm.tick();
  }

  private void tickAtFullOccupancy(SlotManager sm, int times) {
    for (int i = 0; i < times; i++) {
      sm.recordWrite("p1", TOPIC, 15 * MB);
      sm.recordWrite("p2", TOPIC, 15 * MB);
      sm.tick();
    }
  }

  @Test
  public void testLatchEngagesAfterSustainedFullOccupancy() {
    SlotManager sm = create(4);
    fillAllSlots(sm);
    // The drain latch tracks routing-slot occupancy; two 2-slot producers
    // fill all 4 routing slots (getFreeSlots() is now the aggregate EMA view).
    assertEquals(4, sm.getOccupiedSlots());

    tickAtFullOccupancy(sm, 4);

    assertTrue("latch should engage after sustained full occupancy",
        sm.isDrainLatched());
    assertTrue("isFrozen reflects the latch", sm.isFrozen());
  }

  @Test
  public void testLatchBlocksReacquireOfFreedSlot() {
    SlotManager sm = create(4);
    fillAllSlots(sm);
    tickAtFullOccupancy(sm, 4);
    assertTrue(sm.isDrainLatched());

    // Free one slot. Cooldown is disabled, so only the latch can block the
    // reacquire. Occupancy drops to 3 of 4 (so isFrozen is driven purely by
    // the latch, not by full occupancy or the global cooldown).
    sm.releaseProducerSlots("p1", TOPIC, 1);
    assertEquals(3, sm.getOccupiedSlots());

    // Both producers want more; the latch must keep the freed slot free.
    sm.recordWrite("p1", TOPIC, 25 * MB);
    sm.recordWrite("p2", TOPIC, 25 * MB);
    sm.tick();

    assertTrue(sm.isDrainLatched());
    assertTrue("isFrozen is driven by the latch even below full occupancy",
        sm.isFrozen());
    assertEquals("latch blocks reacquire of the freed slot", 3, sm.getOccupiedSlots());
  }

  @Test
  public void testTransientFreeSpikeDoesNotDisengage() {
    SlotManager sm = create(4);
    fillAllSlots(sm);
    tickAtFullOccupancy(sm, 4);
    assertTrue(sm.isDrainLatched());

    // A single eviction creates free=1 for one tick. The smoothed free-slots
    // EMA stays below the disengage threshold (2), so the latch holds.
    sm.releaseProducerSlots("p1", TOPIC, 1);
    sm.recordWrite("p1", TOPIC, 25 * MB);
    sm.recordWrite("p2", TOPIC, 25 * MB);
    sm.tick();

    assertTrue("a transient free=1 spike must not disengage the latch",
        sm.isDrainLatched());
  }

  @Test
  public void testLatchDisengagesAfterDraining() {
    SlotManager sm = create(4);
    fillAllSlots(sm);
    tickAtFullOccupancy(sm, 4);
    assertTrue(sm.isDrainLatched());

    // Drain the broker well above the disengage target and stop writing so
    // nothing reacquires; the free-slots EMA climbs past the threshold.
    sm.releaseProducerSlots("p1", TOPIC, 2);
    sm.releaseProducerSlots("p2", TOPIC, 2);
    assertEquals(0, sm.getOccupiedSlots());

    for (int i = 0; i < 5; i++) {
      sm.tick();
    }
    assertFalse("latch should disengage once drained above the target",
        sm.isDrainLatched());
  }

  @Test
  public void testLatchDisabledNeverEngages() {
    config.setDrainLatchEnabled(false);
    SlotManager sm = create(4);
    fillAllSlots(sm);
    tickAtFullOccupancy(sm, 6);

    assertFalse("latch must never engage when disabled", sm.isDrainLatched());

    // Without the latch (and cooldown disabled), a freed slot is reacquired
    // immediately.
    sm.releaseProducerSlots("p1", TOPIC, 1);
    assertEquals(1, sm.getFreeSlots());
    sm.recordWrite("p1", TOPIC, 25 * MB);
    sm.recordWrite("p2", TOPIC, 25 * MB);
    sm.tick();
    assertEquals("without the latch the freed slot is reacquired",
        0, sm.getFreeSlots());
  }
}
