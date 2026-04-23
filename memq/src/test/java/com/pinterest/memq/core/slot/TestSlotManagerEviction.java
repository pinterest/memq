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
    int initialOccupied = sm.getOccupiedSlots();

    int released = sm.releaseProducerSlots("producer-1", TOPIC, 1);

    assertEquals(1, released);
    assertEquals(1, sm.getProducerSlots("producer-1", TOPIC));
    assertEquals(initialOccupied - 1, sm.getOccupiedSlots());
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

    Map<String, Set<String>> allConnections = sm.getProducerConnections();
    assertNotNull(allConnections.get("producer-1"));
    assertEquals(2, allConnections.get("producer-1").size());
    assertTrue(allConnections.get("producer-1").contains("broker-1"));
    assertTrue(allConnections.get("producer-1").contains("broker-2"));
  }

  @Test
  public void testRecordProducerConnectionsOverwrites() {
    SlotManager sm = create(20);

    sm.recordProducerConnections("producer-1",
        new HashSet<>(Collections.singletonList("broker-1")));
    sm.recordProducerConnections("producer-1",
        new HashSet<>(Collections.singletonList("broker-2")));

    Map<String, Set<String>> allConnections = sm.getProducerConnections();
    assertEquals(1, allConnections.get("producer-1").size());
    assertTrue(allConnections.get("producer-1").contains("broker-2"));
  }
}
