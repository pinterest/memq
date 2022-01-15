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
package com.pinterest.memq.core.clustering;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.protocol.Broker.BrokerType;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TestExpirationPartitionBalanceStrategy {

  @Test
  public void testExpirationPartitionBalanceStrategy() {
    short port = 9092;
    BalanceStrategy strategy = new ExpirationPartitionBalanceStrategy();
    Set<Broker> brokers = new HashSet<>(
        Arrays.asList(new Broker("1.1.1.9", port, "c5.2xlarge", "us-east-1c", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.8", port, "c5.2xlarge", "us-east-1b", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.7", port, "c5.2xlarge", "us-east-1a", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.6", port, "c5.2xlarge", "us-east-1c", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.5", port, "c5.2xlarge", "us-east-1b", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.4", port, "c5.2xlarge", "us-east-1a", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.3", port, "c5.2xlarge", "us-east-1c", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.2", port, "c5.2xlarge", "us-east-1b", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.1", port, "c5.2xlarge", "us-east-1a", BrokerType.WRITE, new HashSet<>())));
    Set<TopicConfig> topics = new HashSet<>(
        Arrays.asList(
            new TopicConfig(1, 1024 * 1024 * 2, 256, "test1", 15, 100, 2),
            new TopicConfig(0, 1024 * 1024 * 2, 256, "test2", 15, 200, 2),
            new TopicConfig(2, 1024 * 1024 * 2, 256, "test3", 50, 500, 2)));
    Set<Broker> newBrokers = strategy.balance(topics, brokers);

    for (Broker broker : newBrokers) {
      assertTrue(broker.getAssignedTopics().size() > 0);
    }
    
    Map<String, Set<String>> one = buildAllocationMap(newBrokers);
    
    System.out.println("Rebalancing again");
    topics = new HashSet<>(Arrays.asList(
        new TopicConfig(1, 1024 * 1024 * 2, 256, "test1", 15, 100, 2),
        new TopicConfig(0, 1024 * 1024 * 2, 256, "test2", 15, 200, 2),
        new TopicConfig(2, 1024 * 1024 * 2, 256, "test3", 50, 500, 2),
        new TopicConfig(3, 1024 * 1024 * 2, 256, "test4", 5, 10, 2)));
    newBrokers = strategy.balance(topics, newBrokers);

    Map<String, Set<String>> two = buildAllocationMap(newBrokers);
    for (Entry<String, Set<String>> entry : one.entrySet()) {
      Set<String> oneSet = entry.getValue();
      Set<String> twoSet = two.get(entry.getKey());
      assertTrue(
          "Broker " + entry.getKey() + " rebalancing should be stable for new topics being added, i.e. adding of new topics shouldn't cause existing topic allocations to be changed",
          Sets.difference(oneSet, twoSet).isEmpty());
    }
  }

  @Test
  public void testUpdateConfigs() {
    short port = 9092;
    BalanceStrategy strategy = new ExpirationPartitionBalanceStrategy();
    Set<Broker> brokers = new HashSet<>(
        Arrays.asList(new Broker("1.1.1.9", port, "c5.2xlarge", "us-east-1c", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.8", port, "c5.2xlarge", "us-east-1b", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.7", port, "c5.2xlarge", "us-east-1a", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.6", port, "c5.2xlarge", "us-east-1c", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.5", port, "c5.2xlarge", "us-east-1b", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.4", port, "c5.2xlarge", "us-east-1a", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.3", port, "c5.2xlarge", "us-east-1c", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.2", port, "c5.2xlarge", "us-east-1b", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.1", port, "c5.2xlarge", "us-east-1a", BrokerType.WRITE, new HashSet<>())));

    TopicConfig baseConfig = new TopicConfig(0, 1024 * 1024 * 2, 256, "", 15, 100, 2);


    Set<TopicConfig> topics = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      TopicConfig conf = new TopicConfig(baseConfig);
      conf.setTopicOrder(i);
      conf.setTopic("test" + i);
      conf.setInputTrafficMB(i * 100);
      conf.setStorageHandlerName("delayeddevnull");
      topics.add(conf);
    }
    Set<Broker> newBrokers = strategy.balance(topics, brokers);

    Map<Broker, Set<TopicConfig>> firstBrokerConfig = new HashMap<>();
    for (Broker broker : newBrokers) {
      assertTrue(broker.getAssignedTopics().size() > 0);
      assertTrue(broker.getAssignedTopics().stream().allMatch(tc -> tc.getStorageHandlerName().equals("delayeddevnull")));
      firstBrokerConfig.put(broker, new HashSet<>(broker.getAssignedTopics()));
    }

    System.out.println("Rebalancing again");
    topics.clear();

    for (int i = 0; i < 3; i++) {
      TopicConfig conf = new TopicConfig(baseConfig);
      conf.setTopicOrder(i);
      conf.setTopic("test" + i);
      conf.setInputTrafficMB(i * 100);
      conf.setStorageHandlerName("customs3aync2");
      conf.setClusteringMultiplier(2);
      topics.add(conf);
    }
    newBrokers = strategy.balance(topics, brokers);

    for (Broker broker : newBrokers) {
      assertEquals(firstBrokerConfig.get(broker), broker.getAssignedTopics());
      assertTrue(broker.getAssignedTopics().stream().allMatch(tc -> tc.getStorageHandlerName().equals("customs3aync2")));
    }
  }

  @Test
  public void testPartitionBalanceStrategyShrink() {
    short port = 9092;
    BalanceStrategy strategy = new ExpirationPartitionBalanceStrategy();
    Set<Broker> brokers = new HashSet<>(
        Arrays.asList(new Broker("1.1.1.9", port, "c5.2xlarge", "us-east-1c", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.8", port, "c5.2xlarge", "us-east-1b", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.7", port, "c5.2xlarge", "us-east-1a", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.6", port, "c5.2xlarge", "us-east-1c", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.5", port, "c5.2xlarge", "us-east-1b", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.4", port, "c5.2xlarge", "us-east-1a", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.3", port, "c5.2xlarge", "us-east-1c", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.2", port, "c5.2xlarge", "us-east-1b", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.1", port, "c5.2xlarge", "us-east-1a", BrokerType.WRITE, new HashSet<>())));
    Set<TopicConfig> topics = new HashSet<>(
        Arrays.asList(
            new TopicConfig(1, 1024 * 1024 * 2, 256, "test1", 15, 150, 2),
            new TopicConfig(0, 1024 * 1024 * 2, 256, "test2", 15, 200, 2),
            new TopicConfig(2, 1024 * 1024 * 2, 256, "test3", 50, 500, 2)));
    Set<Broker> newBrokers = strategy.balance(topics, brokers);

    for (Broker broker : newBrokers) {
      assertTrue(broker.getAssignedTopics().size() > 0);
    }

    Map<String, Set<String>> one = buildAllocationMap(newBrokers);
    int min1 = newBrokers.stream().mapToInt(Broker::getAvailableCapacity).min().getAsInt();

    System.out.println("Rebalancing again");
    topics = new HashSet<>(Arrays.asList(
        new TopicConfig(1, 1024 * 1024 * 2, 256, "test1", 15, 150, 2),
        new TopicConfig(0, 1024 * 1024 * 2, 256, "test2", 15, 200, 2),
        new TopicConfig(2, 1024 * 1024 * 2, 256, "test3", 50, 100, 2)));
    newBrokers = strategy.balance(topics, newBrokers);

    Map<String, Set<String>> two = buildAllocationMap(newBrokers);
    int min2 = newBrokers.stream().mapToInt(Broker::getAvailableCapacity).min().getAsInt();
    Map<String, Integer> oneTopicCount = new HashMap<>();
    for(Set<String> assignments : one.values()) {
      assignments.forEach(t -> oneTopicCount.compute(t, (k, v) -> v == null ? 1 : (v + 1)));
    }
    Map<String, Integer> twoTopicCount = new HashMap<>();
    for(Set<String> assignments : two.values()) {
      assignments.forEach(t -> twoTopicCount.compute(t, (k, v) -> v == null ? 1 : (v + 1)));
    }

    assertTrue(min2 > min1);

    assertEquals(0, oneTopicCount.get("test1") - twoTopicCount.get("test1"));
    assertEquals(0, oneTopicCount.get("test2") - twoTopicCount.get("test2"));
    assertEquals(3, oneTopicCount.get("test3") - twoTopicCount.get("test3"));
  }

  @Test
  public void testExpiringAssignments() throws Exception {
    short port = 9092;
    ExpirationPartitionBalanceStrategy strategy = new ExpirationPartitionBalanceStrategy();
    strategy.setDefaultExpirationTime(1000L);
    Set<Broker> brokers = new HashSet<>(
        Arrays.asList(new Broker("1.1.1.9", port, "c5.2xlarge", "us-east-1c", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.8", port, "c5.2xlarge", "us-east-1b", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.7", port, "c5.2xlarge", "us-east-1a", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.6", port, "c5.2xlarge", "us-east-1c", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.5", port, "c5.2xlarge", "us-east-1b", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.4", port, "c5.2xlarge", "us-east-1a", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.3", port, "c5.2xlarge", "us-east-1c", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.2", port, "c5.2xlarge", "us-east-1b", BrokerType.WRITE, new HashSet<>()),
            new Broker("1.1.1.1", port, "c5.2xlarge", "us-east-1a", BrokerType.WRITE, new HashSet<>())));

    TopicConfig baseConfig = new TopicConfig(0, 1024 * 1024 * 2, 256, "", 15, 100, 2);


    Set<TopicConfig> topics = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      TopicConfig conf = new TopicConfig(baseConfig);
      conf.setTopicOrder(i);
      conf.setTopic("test" + i);
      conf.setInputTrafficMB((i + 1) * 100);
      conf.setStorageHandlerName("delayeddevnull");
      conf.setClusteringMultiplier(2);
      topics.add(conf);
    }
    Set<Broker> newBrokers = strategy.balance(topics, brokers);
    System.out.println(buildAllocationMap(newBrokers));

    Map<Broker, Set<TopicConfig>> firstBrokerConfig = new HashMap<>();
    for (Broker broker : newBrokers) {
      assertTrue(broker.getAssignedTopics().size() > 0);
      assertTrue(broker.getAssignedTopics().stream().allMatch(tc -> tc.getStorageHandlerName().equals("delayeddevnull")));
      firstBrokerConfig.put(broker, new HashSet<>(broker.getAssignedTopics()));
    }

    System.out.println("Rebalancing again");
    topics.clear();

    for (int i = 0; i < 3; i++) {
      TopicConfig conf = new TopicConfig(baseConfig);
      conf.setTopicOrder(i);
      conf.setTopic("test" + i);
      conf.setInputTrafficMB((i + 1) * 100);
      conf.setStorageHandlerName("customs3aync2");
      conf.setClusteringMultiplier(2);
      topics.add(conf);
    }

    Set<Broker> additionalBrokers = new HashSet<>(Arrays.asList(
        new Broker("1.1.1.12", port, "c5.2xlarge", "us-east-1c", BrokerType.WRITE, new HashSet<>()),
        new Broker("1.1.1.11", port, "c5.2xlarge", "us-east-1b", BrokerType.WRITE, new HashSet<>()),
        new Broker("1.1.1.10", port, "c5.2xlarge", "us-east-1a", BrokerType.WRITE, new HashSet<>())
    ));

    newBrokers.addAll(additionalBrokers);
    newBrokers = strategy.balance(topics, newBrokers);

    System.out.println(buildAllocationMap(newBrokers));

    for (Broker broker : newBrokers) {
      if (additionalBrokers.contains(broker)) {
        assertTrue(broker.getAssignedTopics().isEmpty());
      } else {
        assertEquals(firstBrokerConfig.get(broker), broker.getAssignedTopics());
        assertTrue(broker.getAssignedTopics().stream().allMatch(tc -> tc.getStorageHandlerName().equals("customs3aync2")));
      }
    }

    Thread.sleep(1500);

    newBrokers.addAll(additionalBrokers);
    newBrokers = strategy.balance(topics, newBrokers);

    System.out.println(buildAllocationMap(newBrokers));

    for (Broker broker : newBrokers) {
      assertFalse(broker.getAssignedTopics().isEmpty());
      assertTrue(broker.getAssignedTopics().stream().allMatch(tc -> tc.getStorageHandlerName().equals("customs3aync2")));
    }
  }

  private Map<String, Set<String>> buildAllocationMap(Set<Broker> brokers) {
    Map<String, Set<String>> brokerAllocationMap = new HashMap<>();
    for (Broker broker : brokers) {
      Set<String> set = new HashSet<>();
      brokerAllocationMap.put(broker.getBrokerIP(), set);
      for (TopicConfig topicConfig : broker.getAssignedTopics()) {
        set.add(topicConfig.getTopic());
      }
    }
    return brokerAllocationMap;
  }

}