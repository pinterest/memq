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

import com.pinterest.memq.core.config.SlotAccountingConfig;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TestSlotManagerEviction {

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
    // Default off; cooldown-specific tests opt in by overriding this.
    config.setPostEvictionCooldownSeconds(0.0);
    // These tests acquire slots in a single tick; opt out of the per-tick step
    // clamp (covered by TestSlotManagerSlotStep).
    config.setMaxSlotStep(0);
  }

  private SlotManager create(int totalSlots) {
    return new SlotManager(config, totalSlots);
  }

  @Test
  public void testReleaseProducerSlots() {
    SlotManager sm = create(32);

    // 15 MB -> 2 slots at 10 MB/s per slot
    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1", TOPIC));
    int initialOccupied = sm.getTotalSlotOwnershipAcrossProducers();

    int released = sm.releaseProducerSlots("producer-1", TOPIC, 1);

    assertEquals(1, released);
    assertEquals(1, sm.getProducerSlots("producer-1", TOPIC));
    assertEquals(initialOccupied - 1, sm.getTotalSlotOwnershipAcrossProducers());
  }

  @Test
  public void testReleaseMoreThanOwned() {
    SlotManager sm = create(32);

    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1", TOPIC));

    int released = sm.releaseProducerSlots("producer-1", TOPIC, 10);

    assertEquals(2, released);
    assertEquals(0, sm.getProducerSlots("producer-1", TOPIC));
    assertFalse("entry should be removed when slots reach 0", sm.producerHasSlots("producer-1"));
    assertEquals(0, sm.getProducerCount());
  }

  @Test
  public void testReleaseUnknownProducer() {
    SlotManager sm = create(20);

    int released = sm.releaseProducerSlots("nonexistent", TOPIC, 5);
    assertEquals(0, released);
  }

  @Test
  public void testReleaseUnknownTopic() {
    SlotManager sm = create(32);
    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();

    int released = sm.releaseProducerSlots("producer-1", "unknownTopic", 1);
    assertEquals(0, released);
  }

  @Test
  public void testGetTotalProducerSlots() {
    SlotManager sm = create(32);

    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.recordWrite("producer-2", TOPIC, 25 * MB);
    sm.tick();

    assertEquals(2, sm.getTotalProducerSlots("producer-1"));
    assertEquals(3, sm.getTotalProducerSlots("producer-2"));
    assertEquals(0, sm.getTotalProducerSlots("nonexistent"));
  }

  @Test
  public void testProducerHasSlotsAndGetTopics() {
    SlotManager sm = create(32);
    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();
    assertTrue(sm.producerHasSlots("producer-1"));
    assertEquals(1, sm.getProducerTopics("producer-1").size());
    assertTrue(sm.getProducerTopics("producer-1").contains(TOPIC));

    sm.releaseProducerSlots("producer-1", TOPIC, 2);

    assertFalse(sm.producerHasSlots("producer-1"));
    assertTrue(sm.getProducerTopics("producer-1").isEmpty());
  }

  @Test
  public void testGetProducerIdsWithSlots() {
    SlotManager sm = create(32);
    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.recordWrite("producer-2", TOPIC, 25 * MB);
    sm.tick();

    Set<String> ids = sm.getProducerIdsWithSlots();
    assertTrue(ids.contains("producer-1"));
    assertTrue(ids.contains("producer-2"));

    sm.releaseProducerSlots("producer-1", TOPIC, 10);
    ids = sm.getProducerIdsWithSlots();
    assertFalse(ids.contains("producer-1"));
    assertTrue(ids.contains("producer-2"));
  }

  @Test
  public void testRecordProducerConnections() {
    SlotManager sm = create(20);

    Set<String> connections = new HashSet<>();
    connections.add("broker-1");
    connections.add("broker-2");
    sm.recordProducerConnections("producer-1", connections);

    Map<String, Map<String, Integer>> allConnections = sm.getProducerConnections();
    assertNotNull(allConnections.get("producer-1"));
    assertEquals(2, allConnections.get("producer-1").size());
    assertTrue(allConnections.get("producer-1").containsKey("broker-1"));
    assertTrue(allConnections.get("producer-1").containsKey("broker-2"));
  }

  @Test
  public void testPostEvictionCooldownBlocksReacquire() {
    // 60s cooldown. Use long enough that the in-test tick() can't possibly
    // outrun it.
    config.setPostEvictionCooldownSeconds(60.0);
    SlotManager sm = create(32);

    // Drive producer to 2 slots.
    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1", TOPIC));

    // Forcibly evict 1 slot.
    int released = sm.releaseProducerSlots("producer-1", TOPIC, 1);
    assertEquals(1, released);
    assertEquals(1, sm.getProducerSlots("producer-1", TOPIC));

    // EMA still demands 2 slots, but the cooldown should block reacquisition.
    // We tick repeatedly to be sure the global per-tick gate is not what's
    // holding it back.
    for (int i = 0; i < 5; i++) {
      sm.recordWrite("producer-1", TOPIC, 15 * MB);
      sm.tick();
    }
    assertEquals("post-eviction cooldown should block reacquire",
        1, sm.getProducerSlots("producer-1", TOPIC));
  }

  @Test
  public void testPostEvictionCooldownExpires() throws InterruptedException {
    // Short cooldown so the test stays fast.
    config.setPostEvictionCooldownSeconds(0.1);
    SlotManager sm = create(32);

    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1", TOPIC));

    sm.releaseProducerSlots("producer-1", TOPIC, 1);
    assertEquals(1, sm.getProducerSlots("producer-1", TOPIC));

    // Inside the cooldown window: blocked.
    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();
    assertEquals(1, sm.getProducerSlots("producer-1", TOPIC));

    // After the cooldown elapses, EMA-driven acquisition resumes.
    Thread.sleep(150);
    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();
    assertEquals("cooldown should have expired",
        2, sm.getProducerSlots("producer-1", TOPIC));
  }

  @Test
  public void testPostEvictionCooldownDisabledByDefaultZero() {
    // Cooldown=0 (the legacy behavior) lets the producer reacquire on the
    // very next tick after eviction.
    config.setPostEvictionCooldownSeconds(0.0);
    SlotManager sm = create(32);

    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1", TOPIC));

    sm.releaseProducerSlots("producer-1", TOPIC, 1);
    assertEquals(1, sm.getProducerSlots("producer-1", TOPIC));

    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();
    assertEquals("with cooldown disabled, reacquire is immediate",
        2, sm.getProducerSlots("producer-1", TOPIC));
  }

  @Test
  public void testVoluntaryReleaseDoesNotArmCooldown() {
    // Voluntary EMA-driven release (the tick path) must not arm the
    // post-eviction cooldown -- it's only intended to dampen reactions to
    // forced eviction. Otherwise EMA-driven shrink/grow oscillations would
    // be needlessly delayed.
    config.setPostEvictionCooldownSeconds(60.0);
    SlotManager sm = create(32);

    // Drive producer to 2 slots.
    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1", TOPIC));

    // Drop traffic so the EMA-driven path voluntarily releases a slot.
    sm.recordWrite("producer-1", TOPIC, 5 * MB);
    sm.tick();
    assertEquals(1, sm.getProducerSlots("producer-1", TOPIC));

    // Ramp back up. Cooldown was never armed, so reacquire is allowed.
    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();
    assertEquals("voluntary release should not arm post-eviction cooldown",
        2, sm.getProducerSlots("producer-1", TOPIC));
  }

  @Test
  public void testCooldownIsScopedToPidAndTopic() {
    // Eviction on (producer-1, TOPIC) must not block other (pid, topic)
    // pairs from acquiring -- the cooldown is per-state, not global.
    config.setPostEvictionCooldownSeconds(60.0);
    SlotManager sm = create(32);

    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.recordWrite("producer-1", "topicB", 15 * MB);
    sm.recordWrite("producer-2", TOPIC, 15 * MB);
    sm.tick();
    assertEquals(2, sm.getProducerSlots("producer-1", TOPIC));
    assertEquals(2, sm.getProducerSlots("producer-1", "topicB"));
    assertEquals(2, sm.getProducerSlots("producer-2", TOPIC));

    // Evict only (producer-1, TOPIC).
    sm.releaseProducerSlots("producer-1", TOPIC, 1);

    // Drop the evicted entry to 0 reacquire (blocked) but allow the others
    // to grow if their EMA demands it. Use a higher-rate write for the
    // others to force expectedSlots > current.
    sm.recordWrite("producer-1", TOPIC, 35 * MB);    // wants 4 slots, blocked at 1
    sm.recordWrite("producer-1", "topicB", 35 * MB); // wants 4 slots, allowed
    sm.recordWrite("producer-2", TOPIC, 35 * MB);    // wants 4 slots, allowed
    sm.tick();

    assertEquals("evicted (pid, topic) is gated by cooldown",
        1, sm.getProducerSlots("producer-1", TOPIC));
    assertEquals("same pid, different topic is unaffected",
        4, sm.getProducerSlots("producer-1", "topicB"));
    assertEquals("different pid, same topic is unaffected",
        4, sm.getProducerSlots("producer-2", TOPIC));
  }

  @Test
  public void testPostEvictionCooldownSurvivesSlotRemoval() {
    // Regression: evicting a producer's only slot drops it to 0, which removes
    // the ProducerSlotState entirely. The cooldown must still block reacquire
    // even though the state (and the fresh one recreated by recordWrite) no
    // longer carries it -- this is the production "flap" where the evicted
    // producer reacquired after acquireThreshold instead of the full cooldown.
    config.setPostEvictionCooldownSeconds(60.0);
    config.setDrainLatchEnabled(false); // isolate: cooldown is the only gate
    SlotManager sm = create(32);

    // Single slot (5 MB -> ceil(5/10) = 1 slot).
    sm.recordWrite("producer-1", TOPIC, 5 * MB);
    sm.tick();
    assertEquals(1, sm.getProducerSlots("producer-1", TOPIC));

    // Evict the only slot -> 0 -> ProducerSlotState removed.
    int released = sm.releaseProducerSlots("producer-1", TOPIC, 1);
    assertEquals(1, released);
    assertFalse("state should be removed when slots reach 0",
        sm.producerHasSlots("producer-1"));

    // Producer keeps writing at full rate, recreating a fresh state. The
    // cooldown (now held at the manager level) must still block reacquire.
    for (int i = 0; i < 5; i++) {
      sm.recordWrite("producer-1", TOPIC, 15 * MB);
      sm.tick();
    }
    assertEquals("post-eviction cooldown must survive slot-state removal",
        0, sm.getProducerSlots("producer-1", TOPIC));
  }

  @Test
  public void testPostEvictionCooldownSurvivesRemovalThenExpires() throws InterruptedException {
    config.setPostEvictionCooldownSeconds(0.1);
    config.setDrainLatchEnabled(false);
    SlotManager sm = create(32);

    sm.recordWrite("producer-1", TOPIC, 5 * MB);
    sm.tick();
    assertEquals(1, sm.getProducerSlots("producer-1", TOPIC));

    sm.releaseProducerSlots("producer-1", TOPIC, 1);
    assertFalse(sm.producerHasSlots("producer-1"));

    // Within the cooldown window: blocked even though the state was removed.
    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();
    assertEquals(0, sm.getProducerSlots("producer-1", TOPIC));

    // After the cooldown elapses, EMA-driven acquisition resumes.
    Thread.sleep(150);
    sm.recordWrite("producer-1", TOPIC, 15 * MB);
    sm.tick();
    assertEquals("cooldown should expire even after slot-state removal",
        2, sm.getProducerSlots("producer-1", TOPIC));
  }

  @Test
  public void testRecordProducerConnectionsOverwrites() {
    SlotManager sm = create(20);

    sm.recordProducerConnections("producer-1",
        new HashSet<>(Collections.singletonList("broker-1")));
    sm.recordProducerConnections("producer-1",
        new HashSet<>(Collections.singletonList("broker-2")));

    Map<String, Map<String, Integer>> allConnections = sm.getProducerConnections();
    assertEquals(1, allConnections.get("producer-1").size());
    assertTrue(allConnections.get("producer-1").containsKey("broker-2"));
  }
}
