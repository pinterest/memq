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
package com.pinterest.memq.core.eviction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.core.config.EvictionConfig;
import com.pinterest.memq.core.config.SlotAccountingConfig;
import com.pinterest.memq.core.gossip.GossipState;
import com.pinterest.memq.core.slot.SlotManager;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class TestEvictionManager {

  private SlotManager createSlotManager() {
    SlotAccountingConfig config = new SlotAccountingConfig();
    config.setEnabled(true);
    config.setSlotSizeMbps(10.0);
    return new SlotManager(config, 20, new MetricRegistry());
  }

  @Test
  public void testPollEvictionReturnsAndRemoves() {
    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);

    EvictionStrategy mockStrategy = (sm, peers, conns) ->
        new EvictionResult("uuid-producer-1", "10.0.0.5", 9092, 1);

    EvictionManager manager = new EvictionManager(mockStrategy, createSlotManager(),
        Collections::emptyMap, config);

    manager.runEviction();

    EvictionResult result = manager.pollEviction("uuid-producer-1");
    assertNotNull(result);
    assertEquals("10.0.0.5", result.getTargetBrokerIp());
    assertEquals(1, result.getNumSlotsToEvict());

    // second poll should return null (already removed)
    assertNull(manager.pollEviction("uuid-producer-1"));
  }

  @Test
  public void testPollEvictionReturnsNullWhenNone() {
    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);

    EvictionStrategy mockStrategy = (sm, peers, conns) -> null;

    EvictionManager manager = new EvictionManager(mockStrategy, createSlotManager(),
        Collections::emptyMap, config);

    assertNull(manager.pollEviction("nonexistent-uuid"));
  }

  @Test
  public void testNewEvictionOverwritesOld() {
    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);

    final int[] callCount = {0};
    EvictionStrategy mockStrategy = (sm, peers, conns) -> {
      callCount[0]++;
      return new EvictionResult("uuid-producer-1", "target-" + callCount[0], 9092, 1);
    };

    EvictionManager manager = new EvictionManager(mockStrategy, createSlotManager(),
        Collections::emptyMap, config);

    manager.runEviction();
    assertEquals(1, manager.getPendingCount());

    manager.runEviction();
    assertEquals(1, manager.getPendingCount());

    EvictionResult result = manager.pollEviction("uuid-producer-1");
    assertNotNull(result);
    assertEquals("target-2", result.getTargetBrokerIp());
  }

  @Test
  public void testPeekDoesNotRemove() {
    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);

    EvictionStrategy mockStrategy = (sm, peers, conns) ->
        new EvictionResult("uuid-producer-1", "10.0.0.5", 9092, 1);

    EvictionManager manager = new EvictionManager(mockStrategy, createSlotManager(),
        Collections::emptyMap, config);

    manager.runEviction();

    EvictionResult peek1 = manager.peekEviction("uuid-producer-1");
    assertNotNull(peek1);
    EvictionResult peek2 = manager.peekEviction("uuid-producer-1");
    assertNotNull(peek2);
    assertEquals(1, manager.getPendingCount());
  }

  @Test
  public void testStartStop() throws InterruptedException {
    EvictionConfig config = new EvictionConfig();
    config.setEnabled(true);
    config.setIntervalSeconds(0.1);

    final int[] callCount = {0};
    EvictionStrategy mockStrategy = (sm, peers, conns) -> {
      callCount[0]++;
      return null;
    };

    EvictionManager manager = new EvictionManager(mockStrategy, createSlotManager(),
        Collections::emptyMap, config);
    manager.start();
    Thread.sleep(500);
    manager.stop();

    assertTrue("Expected at least 2 eviction runs, got " + callCount[0], callCount[0] >= 2);
  }

  private static void assertTrue(String msg, boolean condition) {
    if (!condition) throw new AssertionError(msg);
  }
}
