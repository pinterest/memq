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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.junit.Test;

import com.pinterest.memq.core.config.SlotAccountingConfig;

public class TestSlotManager {

  private static final int MB = 1024 * 1024;
  private static final String TOPIC = "topicA";

  private SlotAccountingConfig fastConfig() {
    SlotAccountingConfig config = new SlotAccountingConfig();
    config.setEnabled(true);
    config.setSlotSizeMbps(10.0);
    config.setSlotOverhead(0.0);
    config.setAcquireThresholdSeconds(0.0);
    config.setReleaseThresholdSeconds(0.0);
    config.setCooldownSeconds(0.0);
    config.setEmaWindowSeconds(0.001);
    config.setTickIntervalMs(100);
    config.setIdleProducerTimeoutMs(500);
    return config;
  }

  @Test
  public void testInitialState() {
    SlotAccountingConfig config = fastConfig();
    SlotManager sm = new SlotManager(config, 32);

    assertEquals(32, sm.getTotalSlots());
    assertEquals(0, sm.getOccupiedSlots());
    assertEquals(32, sm.getFreeSlots());
    assertFalse(sm.isFrozen());
    assertEquals(0, sm.getProducerCount());
    assertEquals(0, sm.getProducerSlots("nonexistent", TOPIC));
  }

  @Test
  public void testRecordWriteCreatesProducer() {
    SlotAccountingConfig config = fastConfig();
    SlotManager sm = new SlotManager(config, 32);

    sm.recordWrite("10.0.0.1", TOPIC, 1000);
    assertEquals(1, sm.getProducerCount());
    sm.recordWrite("10.0.0.2", TOPIC, 2000);
    assertEquals(2, sm.getProducerCount());
  }

  @Test
  public void testSlotAcquisitionAfterSustainedRate() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    SlotManager sm = new SlotManager(config, 32);

    // Simulate writing 15 MB in a 1-second tick window (15 MB/s > 1 slot at 10 MB/s)
    sm.recordWrite("producer-1", TOPIC, 15 * MB);

    sm.tick();

    assertEquals(2, sm.getProducerSlots("producer-1", TOPIC));
    assertEquals(2, sm.getOccupiedSlots());
    assertEquals(30, sm.getFreeSlots());
  }

  @Test
  public void testSlotReleaseAfterRateDrops() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    SlotManager sm = new SlotManager(config, 32);

    // Acquire 2 slots first
    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1", TOPIC));

    // Now send much less traffic (5 MB/s -> expect 1 slot)
    sm.recordWrite("producer-1", TOPIC, 5 * MB);
    sm.tick();

    assertEquals(1, sm.getProducerSlots("producer-1", TOPIC));
    assertEquals(1, sm.getOccupiedSlots());
  }

  @Test
  public void testAcquireThresholdPreventsImmediateAcquisition() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    config.setAcquireThresholdSeconds(5.0);
    SlotManager sm = new SlotManager(config, 32);

    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();

    // Should NOT have acquired yet -- threshold not reached
    assertEquals(0, sm.getProducerSlots("producer-1", TOPIC));
  }

  @Test
  public void testReleaseThresholdPreventsImmediateRelease() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    config.setReleaseThresholdSeconds(5.0);
    SlotManager sm = new SlotManager(config, 32);

    // Acquire first (release threshold doesn't affect acquisition here since acquire=0)
    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1", TOPIC));

    // Drop traffic -- should NOT release immediately
    sm.recordWrite("producer-1", TOPIC, 5 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1", TOPIC));
  }

  @Test
  public void testCooldownPreventsRapidSlotChanges() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    config.setCooldownSeconds(60.0);
    SlotManager sm = new SlotManager(config, 32);

    // First acquisition should work
    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1", TOPIC));

    // Second producer tries to acquire -- should be blocked by cooldown
    sm.recordWrite("producer-2", TOPIC, 15 * MB);
    sm.tick();
    assertEquals(0, sm.getProducerSlots("producer-2", TOPIC));
  }

  @Test
  public void testFreezeWhenAtCapacity() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    SlotManager sm = new SlotManager(config, 2);

    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1", TOPIC));
    assertEquals(0, sm.getFreeSlots());

    assertTrue("Should be frozen when at capacity", sm.isFrozen());
  }

  @Test
  public void testFreezeAfterRecentSlotChange() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    config.setCooldownSeconds(60.0);
    SlotManager sm = new SlotManager(config, 32);

    assertFalse("Should not be frozen initially", sm.isFrozen());

    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();

    assertTrue("Should be frozen after slot change (cooldown active)", sm.isFrozen());
  }

  @Test
  public void testCapacityLimitPreventsOverAllocation() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    SlotManager sm = new SlotManager(config, 3);

    // Producer wants 5 slots but only 3 exist
    sm.recordWrite("producer-1", TOPIC, 45 * MB);
    sm.tick();

    assertTrue(sm.getProducerSlots("producer-1", TOPIC) <= 3);
    assertTrue(sm.getOccupiedSlots() <= 3);
    assertTrue(sm.getFreeSlots() >= 0);
  }

  @Test
  public void testMultipleProducersIndependentAccounting() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    SlotManager sm = new SlotManager(config, 32);

    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.recordWrite("producer-2", TOPIC, 25 * MB);
    sm.tick();

    assertEquals(2, sm.getProducerSlots("producer-1", TOPIC));
    assertEquals(3, sm.getProducerSlots("producer-2", TOPIC));
    assertEquals(5, sm.getOccupiedSlots());
    assertEquals(27, sm.getFreeSlots());
  }

  @Test
  public void testIdleProducerCleanup() throws InterruptedException {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    config.setIdleProducerTimeoutMs(200);
    SlotManager sm = new SlotManager(config, 32);

    sm.recordWrite("producer-1", TOPIC, 1000);
    sm.tick();
    assertEquals(1, sm.getProducerCount());

    // Wait for idle timeout
    Thread.sleep(300);
    sm.tick();

    assertEquals("Idle producer should be cleaned up", 0, sm.getProducerCount());
  }

  @Test
  public void testIdleProducerSlotsReleasedOnCleanup() throws InterruptedException {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    config.setIdleProducerTimeoutMs(200);
    SlotManager sm = new SlotManager(config, 32);

    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1", TOPIC));
    assertEquals(2, sm.getOccupiedSlots());

    // Wait for idle timeout
    Thread.sleep(300);
    sm.tick();

    // Idle producer should be cleaned up and its slots released
    assertEquals(0, sm.getProducerCount());
    assertEquals(0, sm.getOccupiedSlots());
    assertEquals(32, sm.getFreeSlots());
  }

  @Test
  public void testEmaConvergence() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    config.setEmaWindowSeconds(5.0);
    SlotManager sm = new SlotManager(config, 32);

    // Feed a steady 20 MB/s for many ticks so EMA converges
    for (int i = 0; i < 50; i++) {
      sm.recordWrite("producer-1", TOPIC, 20 * MB);
      sm.tick();
    }

    // At 20 MB/s with 10 MB/s slots, expect 2 slots
    assertEquals(2, sm.getProducerSlots("producer-1", TOPIC));
  }

  @Test
  public void testStartAndStop() throws InterruptedException {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(50);
    config.setEmaWindowSeconds(0.001);
    SlotManager sm = new SlotManager(config, 32);

    sm.start();

    // Feed writes across multiple tick windows
    for (int i = 0; i < 5; i++) {
      sm.recordWrite("producer-1", TOPIC, 15 * MB);
      Thread.sleep(60);
    }

    assertTrue("Background tick should have acquired slots",
        sm.getProducerSlots("producer-1", TOPIC) > 0);

    sm.stop();
  }

  @Test
  public void testSlotAccountingConfigDefaults() {
    SlotAccountingConfig config = new SlotAccountingConfig();
    assertFalse(config.isEnabled());
    assertEquals(10.0, config.getSlotSizeMbps(), 0.001);
    assertEquals(0.2, config.getSlotOverhead(), 0.001);
    assertEquals(30.0, config.getAcquireThresholdSeconds(), 0.001);
    assertEquals(30.0, config.getReleaseThresholdSeconds(), 0.001);
    assertEquals(10.0, config.getCooldownSeconds(), 0.001);
    assertEquals(60.0, config.getEmaWindowSeconds(), 0.001);
    assertEquals(1000, config.getTickIntervalMs());
    assertEquals(300_000, config.getIdleProducerTimeoutMs());
  }

  @Test
  public void testSlotAccountingConfigSetters() {
    SlotAccountingConfig config = new SlotAccountingConfig();
    config.setEnabled(true);
    config.setSlotSizeMbps(5.0);
    config.setSlotOverhead(0.3);
    config.setAcquireThresholdSeconds(15.0);
    config.setReleaseThresholdSeconds(20.0);
    config.setCooldownSeconds(5.0);
    config.setEmaWindowSeconds(30.0);
    config.setTickIntervalMs(500);
    config.setIdleProducerTimeoutMs(60_000);

    assertTrue(config.isEnabled());
    assertEquals(5.0, config.getSlotSizeMbps(), 0.001);
    assertEquals(0.3, config.getSlotOverhead(), 0.001);
    assertEquals(15.0, config.getAcquireThresholdSeconds(), 0.001);
    assertEquals(20.0, config.getReleaseThresholdSeconds(), 0.001);
    assertEquals(5.0, config.getCooldownSeconds(), 0.001);
    assertEquals(30.0, config.getEmaWindowSeconds(), 0.001);
    assertEquals(500, config.getTickIntervalMs());
    assertEquals(60_000, config.getIdleProducerTimeoutMs());
  }

  @Test
  public void testTotalSlotsComputation() {
    // effectiveCapacity = 400 * (1 - 0.2) = 320, totalSlots = ceil(320/10) = 32
    int totalSlots = (int) Math.ceil(400.0 * (1 - 0.2) / 10.0);
    assertEquals(32, totalSlots);

    // Non-evenly divisible: 350 * (1-0.2) = 280, ceil(280/10) = 28
    totalSlots = (int) Math.ceil(350.0 * (1 - 0.2) / 10.0);
    assertEquals(28, totalSlots);

    // Small overhead: 100 * (1-0.1) = 90, ceil(90/8) = 12
    totalSlots = (int) Math.ceil(100.0 * (1 - 0.1) / 8.0);
    assertEquals(12, totalSlots);
  }

  @Test
  public void testZeroTrafficProducerGetsZeroSlots() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    SlotManager sm = new SlotManager(config, 32);

    sm.recordWrite("producer-1", TOPIC, 0);
    sm.tick();

    assertEquals(0, sm.getProducerSlots("producer-1", TOPIC));
    assertEquals(0, sm.getOccupiedSlots());
  }

  @Test
  public void testGetFreeSlotsNeverNegative() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    SlotManager sm = new SlotManager(config, 2);

    sm.recordWrite("producer-1", TOPIC, 50 * MB);
    sm.tick();

    assertTrue("Free slots should never be negative", sm.getFreeSlots() >= 0);
  }

  @Test
  public void testProducerEmaAndSlotsGaugesUseEncodedTagNames() {
    // Verifies the (pid, topic) encoding scheme that OpenTSDBReporter parses:
    // the registry key is "<metric>|pid=<pid>|topic=<topic>", which the
    // reporter splits into metric name plus extra OpenTSDB tags. Codahale
    // itself just sees an arbitrary unique key, so this is purely a name
    // convention -- the test pins it to catch accidental drift.
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    MetricRegistry slotRegistry = new MetricRegistry();
    SlotManager sm = new SlotManager(config, 32, slotRegistry);

    sm.recordWrite("10.0.0.1", TOPIC, 15 * MB);
    sm.tick();

    String emaKey = "producer.ema|pid=10.0.0.1|topic=" + TOPIC;
    String slotsKey = "producer.slots|pid=10.0.0.1|topic=" + TOPIC;

    Gauge<?> emaGauge = slotRegistry.getGauges().get(emaKey);
    Gauge<?> slotsGauge = slotRegistry.getGauges().get(slotsKey);
    assertNotNull("ema gauge must be registered under encoded key " + emaKey, emaGauge);
    assertNotNull("slots gauge must be registered under encoded key " + slotsKey, slotsGauge);
    assertTrue("ema gauge value must reflect the producer's EMA",
        ((Number) emaGauge.getValue()).doubleValue() > 0.0);
    assertEquals("slots gauge value must reflect currentSlots",
        2, ((Number) slotsGauge.getValue()).intValue());
  }

  @Test
  public void testProducerGaugesAreIsolatedPerPidAndTopic() {
    // Two producers on two topics produces four distinct registry keys --
    // one ema + one slots gauge per (pid, topic) -- and each gauge tracks
    // only its own producer's state.
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    MetricRegistry slotRegistry = new MetricRegistry();
    SlotManager sm = new SlotManager(config, 32, slotRegistry);

    sm.recordWrite("10.0.0.1", "topicA", 15 * MB);
    sm.recordWrite("10.0.0.2", "topicB", 25 * MB);
    sm.tick();

    Gauge<?> a = slotRegistry.getGauges().get("producer.ema|pid=10.0.0.1|topic=topicA");
    Gauge<?> b = slotRegistry.getGauges().get("producer.ema|pid=10.0.0.2|topic=topicB");
    assertNotNull(a);
    assertNotNull(b);
    assertEquals(sm.getProducerEmaRate("10.0.0.1", "topicA"),
        ((Number) a.getValue()).doubleValue(), 1e-9);
    assertEquals(sm.getProducerEmaRate("10.0.0.2", "topicB"),
        ((Number) b.getValue()).doubleValue(), 1e-9);

    // No cross-topic contamination: pid 10.0.0.1 holds nothing on topicB
    // and vice-versa, so neither key should exist in the registry.
    assertFalse("must not register a gauge for an unused (pid, topic) pair",
        slotRegistry.getGauges().containsKey("producer.ema|pid=10.0.0.1|topic=topicB"));
    assertFalse("must not register a gauge for an unused (pid, topic) pair",
        slotRegistry.getGauges().containsKey("producer.ema|pid=10.0.0.2|topic=topicA"));
  }

  @Test
  public void testIdleProducerDeregistersGauges() throws InterruptedException {
    // When the idle-cleanup path drops a (pid, topic) entry it must also
    // unregister both gauges -- otherwise codahale would leak them and
    // future emission would report stale (frozen) values forever.
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    config.setIdleProducerTimeoutMs(200);
    MetricRegistry slotRegistry = new MetricRegistry();
    SlotManager sm = new SlotManager(config, 32, slotRegistry);

    sm.recordWrite("10.0.0.1", TOPIC, 15 * MB);
    sm.tick();
    String emaKey = "producer.ema|pid=10.0.0.1|topic=" + TOPIC;
    String slotsKey = "producer.slots|pid=10.0.0.1|topic=" + TOPIC;
    assertTrue(slotRegistry.getGauges().containsKey(emaKey));
    assertTrue(slotRegistry.getGauges().containsKey(slotsKey));

    Thread.sleep(300);
    sm.tick();

    assertFalse("ema gauge must be removed after idle cleanup",
        slotRegistry.getGauges().containsKey(emaKey));
    assertFalse("slots gauge must be removed after idle cleanup",
        slotRegistry.getGauges().containsKey(slotsKey));
  }

  @Test
  public void testGetProducerEmaRate() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    SlotManager sm = new SlotManager(config, 32);

    assertEquals(0.0, sm.getProducerEmaRate("nonexistent", TOPIC), 0.001);

    sm.recordWrite("producer-1", TOPIC, 20 * MB);
    sm.tick();

    assertTrue("EMA rate should be > 0 after writes", sm.getProducerEmaRate("producer-1", TOPIC) > 0.0);
  }

  @Test
  public void testNoRegistryDoesNotThrow() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    SlotManager sm = new SlotManager(config, 32);

    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();

    assertEquals(2, sm.getProducerSlots("producer-1", TOPIC));
  }

  @Test
  public void testSameProducerDifferentTopicsIndependent() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    SlotManager sm = new SlotManager(config, 32);

    sm.recordWrite("10.0.0.1", "topicA", 15 * MB);
    sm.recordWrite("10.0.0.1", "topicB", 25 * MB);
    sm.tick();

    assertEquals(2, sm.getProducerSlots("10.0.0.1", "topicA"));
    assertEquals(3, sm.getProducerSlots("10.0.0.1", "topicB"));
    assertEquals(5, sm.getOccupiedSlots());
    assertEquals(2, sm.getProducerCount());
  }

  // ---------------------------------------------------------------------
  // Hysteresis
  // ---------------------------------------------------------------------

  @Test
  public void testComputeExpectedSlotsHysteresisStayBand() {
    // With the default hysteresis=0.5 and slot=10, the stay band at
    // currentSlots=7 is [65, 75]. EMA inside the band must return 7.
    SlotAccountingConfig config = fastConfig();
    SlotManager sm = new SlotManager(config, 32);

    // Inside the band -> stay at currentSlots.
    assertEquals(7, sm.computeExpectedSlots(65.0, 7));
    assertEquals(7, sm.computeExpectedSlots(70.0, 7));
    assertEquals(7, sm.computeExpectedSlots(71.0, 7));
    assertEquals(7, sm.computeExpectedSlots(75.0, 7));

    // Above upper band edge -> acquire via ceil(EMA / slotSize).
    assertEquals(8, sm.computeExpectedSlots(75.001, 7));
    assertEquals(8, sm.computeExpectedSlots(80.0, 7));
    assertEquals(9, sm.computeExpectedSlots(85.001, 7));

    // Below lower band edge -> still ceil-based. Note that ceil() pins
    // the result back to currentSlots until EMA crosses the next integer
    // slot boundary down -- this preserves the original release semantics
    // (a slot is only relinquished when capacity is no longer needed).
    assertEquals(7, sm.computeExpectedSlots(64.999, 7));  // ceil(6.4999) = 7
    assertEquals(7, sm.computeExpectedSlots(61.0, 7));    // ceil(6.1)    = 7
    assertEquals(6, sm.computeExpectedSlots(60.0, 7));    // ceil(6.0)    = 6
    assertEquals(5, sm.computeExpectedSlots(50.0, 7));    // ceil(5.0)    = 5

    // Symmetric checks around the current=8 band [75, 85].
    assertEquals(8, sm.computeExpectedSlots(75.0, 8));
    assertEquals(8, sm.computeExpectedSlots(85.0, 8));
    assertEquals(8, sm.computeExpectedSlots(74.999, 8)); // ceil(7.4999) = 8
    assertEquals(7, sm.computeExpectedSlots(70.0, 8));   // ceil(7.0)    = 7
    assertEquals(9, sm.computeExpectedSlots(85.001, 8));
  }

  @Test
  public void testComputeExpectedSlotsHysteresisZeroIsBoundaryEager() {
    // hysteresis=0 collapses the band to a single point at
    // currentSlots*slotSize, so any EMA above current capacity triggers
    // an immediate ceil()-based re-evaluation -- the legacy behavior.
    SlotAccountingConfig config = fastConfig();
    config.setHysteresis(0.0);
    SlotManager sm = new SlotManager(config, 32);

    assertEquals(7, sm.computeExpectedSlots(70.0, 7));    // exactly at boundary
    assertEquals(8, sm.computeExpectedSlots(70.001, 7));  // any above -> acquire
    assertEquals(7, sm.computeExpectedSlots(69.999, 7));  // ceil(6.9999) = 7
    assertEquals(6, sm.computeExpectedSlots(60.0, 7));    // ceil(6.0)    = 6
  }

  @Test
  public void testEvictionDoesNotImmediatelyReacquireWithinBand() {
    // The flap scenario from the production log: producer's EMA settles
    // around 71 Mbps with 8 slots already held (because earlier sustained
    // load drove acquisition past the upper band edge of 75 Mbps). A
    // cluster eviction strips one slot to 7. With the default
    // hysteresis=0.5 the (7, ~71 Mbps) state sits inside the band
    // [65, 75], so the producer must hold at 7 even though
    // acquireThresholdSeconds=0 makes the temporal gate a no-op.
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    config.setEmaWindowSeconds(1.0);
    SlotManager sm = new SlotManager(config, 32);

    // Phase 1: drive sustained load above the 8-slot band's upper edge
    // (85 Mbps) so the producer naturally acquires 8 slots.
    for (int i = 0; i < 20; i++) {
      sm.recordWrite("producer-1", TOPIC, (int) (90.0 * MB));
      sm.tick();
    }
    assertEquals("producer should converge to 9 slots at EMA=90 Mbps",
        9, sm.getProducerSlots("producer-1", TOPIC));

    // Phase 2: relax to 71 Mbps. EMA decays toward 71; ceil(71/10)=8 so
    // we voluntarily release down to 8 (still inside the new band [75, 85]
    // when at current=8 -- wait, EMA=71 < 75 so we eventually drop further).
    // We tick enough times for EMA to converge.
    for (int i = 0; i < 30; i++) {
      sm.recordWrite("producer-1", TOPIC, (int) (71.0 * MB));
      sm.tick();
    }
    int beforeEviction = sm.getProducerSlots("producer-1", TOPIC);
    assertTrue("producer should hold at least 8 slots at EMA~71 Mbps before eviction"
            + " (was " + beforeEviction + ")",
        beforeEviction >= 8);

    // Phase 3: cluster forcibly evicts one slot.
    sm.releaseProducerSlots("producer-1", TOPIC, 1);
    int afterEviction = sm.getProducerSlots("producer-1", TOPIC);
    assertEquals(beforeEviction - 1, afterEviction);

    // Phase 4: continue the same load. Hysteresis must hold the producer
    // at the post-eviction count -- EMA~71 sits inside the band at
    // currentSlots=7 ([65, 75]) and EMA never crosses above 75.
    for (int i = 0; i < 30; i++) {
      sm.recordWrite("producer-1", TOPIC, (int) (71.0 * MB));
      sm.tick();
    }
    assertEquals("hysteresis must prevent re-acquisition after eviction"
            + " while EMA stays inside the band",
        afterEviction, sm.getProducerSlots("producer-1", TOPIC));
  }

  @Test
  public void testGrowthPastUpperBandStillAcquires() {
    // Hysteresis must not block legitimate growth: once EMA pushes past
    // the upper edge of the band the producer should pick up the next
    // slot promptly.
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    config.setEmaWindowSeconds(1.0);
    SlotManager sm = new SlotManager(config, 32);

    // Settle at currentSlots=7 by driving EMA into the [65, 75] band.
    for (int i = 0; i < 20; i++) {
      sm.recordWrite("producer-1", TOPIC, (int) (71.0 * MB));
      sm.tick();
    }
    assertEquals("EMA=71 in band [65, 75] must converge to 7 slots",
        7, sm.getProducerSlots("producer-1", TOPIC));

    // Push EMA past the upper edge (75 Mbps).
    for (int i = 0; i < 20; i++) {
      sm.recordWrite("producer-1", TOPIC, (int) (90.0 * MB));
      sm.tick();
    }
    assertEquals("EMA above the upper band edge must trigger acquisition",
        9, sm.getProducerSlots("producer-1", TOPIC));
  }

  @Test
  public void testEvictionDoesReacquireWhenHysteresisDisabled() {
    // Counter-test: with hysteresis=0.0 the legacy boundary-eager behavior
    // is restored -- the producer re-acquires on the next tick after
    // eviction whenever EMA is even slightly above the current capacity.
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    config.setEmaWindowSeconds(1.0);
    config.setHysteresis(0.0);
    SlotManager sm = new SlotManager(config, 32);

    // With hysteresis=0 the natural convergence is 8 (ceil(71/10) = 8).
    for (int i = 0; i < 10; i++) {
      sm.recordWrite("producer-1", TOPIC, (int) (71.0 * MB));
      sm.tick();
    }
    assertEquals(8, sm.getProducerSlots("producer-1", TOPIC));

    sm.releaseProducerSlots("producer-1", TOPIC, 1);
    assertEquals(7, sm.getProducerSlots("producer-1", TOPIC));

    sm.recordWrite("producer-1", TOPIC, (int) (71.0 * MB));
    sm.tick();
    assertEquals("with hysteresis=0 the producer re-acquires immediately",
        8, sm.getProducerSlots("producer-1", TOPIC));
  }
}
