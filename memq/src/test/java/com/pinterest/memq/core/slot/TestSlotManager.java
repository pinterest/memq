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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.junit.Test;

import com.pinterest.memq.core.config.SlotAccountingConfig;

public class TestSlotManager {

  private static final int MB = 1024 * 1024;

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
    assertEquals(0, sm.getProducerSlots("nonexistent"));
  }

  @Test
  public void testRecordWriteCreatesProducer() {
    SlotAccountingConfig config = fastConfig();
    SlotManager sm = new SlotManager(config, 32);

    sm.recordWrite("10.0.0.1", 1000);
    assertEquals(1, sm.getProducerCount());
    sm.recordWrite("10.0.0.2", 2000);
    assertEquals(2, sm.getProducerCount());
  }

  @Test
  public void testSlotAcquisitionAfterSustainedRate() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    SlotManager sm = new SlotManager(config, 32);

    // Simulate writing 15 MB in a 1-second tick window (15 MB/s > 1 slot at 10 MB/s)
    sm.recordWrite("producer-1", 15 * MB);

    sm.tick();

    assertEquals(2, sm.getProducerSlots("producer-1"));
    assertEquals(2, sm.getOccupiedSlots());
    assertEquals(30, sm.getFreeSlots());
  }

  @Test
  public void testSlotReleaseAfterRateDrops() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    SlotManager sm = new SlotManager(config, 32);

    // Acquire 2 slots first
    sm.recordWrite("producer-1", 15 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1"));

    // Now send much less traffic (5 MB/s -> expect 1 slot)
    sm.recordWrite("producer-1", 5 * MB);
    sm.tick();

    assertEquals(1, sm.getProducerSlots("producer-1"));
    assertEquals(1, sm.getOccupiedSlots());
  }

  @Test
  public void testAcquireThresholdPreventsImmediateAcquisition() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    config.setAcquireThresholdSeconds(5.0);
    SlotManager sm = new SlotManager(config, 32);

    sm.recordWrite("producer-1", 15 * MB);
    sm.tick();

    // Should NOT have acquired yet -- threshold not reached
    assertEquals(0, sm.getProducerSlots("producer-1"));
  }

  @Test
  public void testReleaseThresholdPreventsImmediateRelease() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    config.setReleaseThresholdSeconds(5.0);
    SlotManager sm = new SlotManager(config, 32);

    // Acquire first (release threshold doesn't affect acquisition here since acquire=0)
    sm.recordWrite("producer-1", 15 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1"));

    // Drop traffic -- should NOT release immediately
    sm.recordWrite("producer-1", 5 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1"));
  }

  @Test
  public void testCooldownPreventsRapidSlotChanges() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    config.setCooldownSeconds(60.0);
    SlotManager sm = new SlotManager(config, 32);

    // First acquisition should work
    sm.recordWrite("producer-1", 15 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1"));

    // Second producer tries to acquire -- should be blocked by cooldown
    sm.recordWrite("producer-2", 15 * MB);
    sm.tick();
    assertEquals(0, sm.getProducerSlots("producer-2"));
  }

  @Test
  public void testFreezeWhenAtCapacity() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    SlotManager sm = new SlotManager(config, 2);

    sm.recordWrite("producer-1", 15 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1"));
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

    sm.recordWrite("producer-1", 15 * MB);
    sm.tick();

    assertTrue("Should be frozen after slot change (cooldown active)", sm.isFrozen());
  }

  @Test
  public void testCapacityLimitPreventsOverAllocation() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    SlotManager sm = new SlotManager(config, 3);

    // Producer wants 5 slots but only 3 exist
    sm.recordWrite("producer-1", 45 * MB);
    sm.tick();

    assertTrue(sm.getProducerSlots("producer-1") <= 3);
    assertTrue(sm.getOccupiedSlots() <= 3);
    assertTrue(sm.getFreeSlots() >= 0);
  }

  @Test
  public void testMultipleProducersIndependentAccounting() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    SlotManager sm = new SlotManager(config, 32);

    sm.recordWrite("producer-1", 15 * MB);
    sm.recordWrite("producer-2", 25 * MB);
    sm.tick();

    assertEquals(2, sm.getProducerSlots("producer-1"));
    assertEquals(3, sm.getProducerSlots("producer-2"));
    assertEquals(5, sm.getOccupiedSlots());
    assertEquals(27, sm.getFreeSlots());
  }

  @Test
  public void testIdleProducerCleanup() throws InterruptedException {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    config.setIdleProducerTimeoutMs(200);
    SlotManager sm = new SlotManager(config, 32);

    sm.recordWrite("producer-1", 1000);
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

    sm.recordWrite("producer-1", 15 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1"));
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
      sm.recordWrite("producer-1", 20 * MB);
      sm.tick();
    }

    // At 20 MB/s with 10 MB/s slots, expect 2 slots
    assertEquals(2, sm.getProducerSlots("producer-1"));
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
      sm.recordWrite("producer-1", 15 * MB);
      Thread.sleep(60);
    }

    assertTrue("Background tick should have acquired slots",
        sm.getProducerSlots("producer-1") > 0);

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

    sm.recordWrite("producer-1", 0);
    sm.tick();

    assertEquals(0, sm.getProducerSlots("producer-1"));
    assertEquals(0, sm.getOccupiedSlots());
  }

  @Test
  public void testGetFreeSlotsNeverNegative() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    SlotManager sm = new SlotManager(config, 2);

    sm.recordWrite("producer-1", 50 * MB);
    sm.tick();

    assertTrue("Free slots should never be negative", sm.getFreeSlots() >= 0);
  }

  @Test
  public void testProducerEmaGaugesRegistered() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    MetricRegistry registry = new MetricRegistry();
    SlotManager sm = new SlotManager(config, 32, registry);

    sm.recordWrite("10.0.0.1", 15 * MB);
    sm.tick();

    Gauge<?> emaGauge = registry.getGauges().get("producer.ema.10_0_0_1");
    Gauge<?> slotsGauge = registry.getGauges().get("producer.slots.10_0_0_1");
    assertNotNull("EMA gauge should be registered for producer", emaGauge);
    assertNotNull("Slots gauge should be registered for producer", slotsGauge);

    double emaValue = ((Number) emaGauge.getValue()).doubleValue();
    assertTrue("EMA rate should be > 0 after writes", emaValue > 0.0);

    int slotsValue = ((Number) slotsGauge.getValue()).intValue();
    assertEquals(2, slotsValue);
  }

  @Test
  public void testProducerMetricsDeregisteredOnIdleCleanup() throws InterruptedException {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    config.setIdleProducerTimeoutMs(200);
    MetricRegistry registry = new MetricRegistry();
    SlotManager sm = new SlotManager(config, 32, registry);

    sm.recordWrite("10.0.0.2", 15 * MB);
    sm.tick();
    assertNotNull(registry.getGauges().get("producer.ema.10_0_0_2"));
    assertNotNull(registry.getGauges().get("producer.slots.10_0_0_2"));

    Thread.sleep(300);
    sm.tick();

    assertNull("EMA gauge should be removed after idle cleanup",
        registry.getGauges().get("producer.ema.10_0_0_2"));
    assertNull("Slots gauge should be removed after idle cleanup",
        registry.getGauges().get("producer.slots.10_0_0_2"));
    assertEquals(0, sm.getProducerCount());
  }

  @Test
  public void testMultipleProducerGauges() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    MetricRegistry registry = new MetricRegistry();
    SlotManager sm = new SlotManager(config, 32, registry);

    sm.recordWrite("10.0.0.1", 15 * MB);
    sm.recordWrite("10.0.0.2", 25 * MB);
    sm.tick();

    assertNotNull(registry.getGauges().get("producer.ema.10_0_0_1"));
    assertNotNull(registry.getGauges().get("producer.ema.10_0_0_2"));
    assertNotNull(registry.getGauges().get("producer.slots.10_0_0_1"));
    assertNotNull(registry.getGauges().get("producer.slots.10_0_0_2"));

    assertEquals(2, ((Number) registry.getGauges().get("producer.slots.10_0_0_1").getValue()).intValue());
    assertEquals(3, ((Number) registry.getGauges().get("producer.slots.10_0_0_2").getValue()).intValue());
  }

  @Test
  public void testGetProducerEmaRate() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    SlotManager sm = new SlotManager(config, 32);

    assertEquals(0.0, sm.getProducerEmaRate("nonexistent"), 0.001);

    sm.recordWrite("producer-1", 20 * MB);
    sm.tick();

    assertTrue("EMA rate should be > 0 after writes", sm.getProducerEmaRate("producer-1") > 0.0);
  }

  @Test
  public void testNoRegistryDoesNotThrow() {
    SlotAccountingConfig config = fastConfig();
    config.setTickIntervalMs(1000);
    SlotManager sm = new SlotManager(config, 32);

    sm.recordWrite("producer-1", 15 * MB);
    sm.tick();

    assertEquals(2, sm.getProducerSlots("producer-1"));
  }

  @Test
  public void testSanitizeProducerId() {
    assertEquals("10_0_0_1", SlotManager.sanitizeProducerId("10.0.0.1"));
    assertEquals("host_8080", SlotManager.sanitizeProducerId("host:8080"));
    assertEquals("simple", SlotManager.sanitizeProducerId("simple"));
  }
}
