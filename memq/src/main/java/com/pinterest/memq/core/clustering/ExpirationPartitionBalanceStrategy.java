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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.TopicAssignment;
import com.pinterest.memq.commons.protocol.TopicConfig;

public class ExpirationPartitionBalanceStrategy extends BalanceStrategy {

  private long defaultExpirationTime = 300_000;
  private static final int DEFAULT_CAPACITY = 200;
  private static final Logger logger = Logger.getLogger(ExpirationPartitionBalanceStrategy.class.getName());
  private Map<String, Integer> instanceTypeThroughputMap = new HashMap<>();

  @Override
  public Set<Broker> balance(Set<TopicConfig> topics, Set<Broker> brokers) {
    List<Broker> oldBrokerList = new ArrayList<>(brokers);
    Collections.sort(oldBrokerList);

    List<Broker> newBrokerList = new ArrayList<>();

    List<TopicConfig> topicsList = new ArrayList<>(topics);
    Collections.sort(topicsList);

    long now = System.currentTimeMillis();

    // establish base assignment from sticky (non-expired) assignments
    for (Broker b : oldBrokerList) {
      Set<TopicAssignment> brokerStickyAssignments = b.getAssignedTopics()
          .stream()
          .filter(topicAssignment -> topicAssignment.getAssignmentTimestamp() + defaultExpirationTime
              > now)
          .collect(Collectors.toSet());
      Broker newBroker = new Broker(b);
      newBroker.setAssignedTopics(brokerStickyAssignments);
      newBrokerList.add(newBroker);
    }

    Map<String, PriorityQueue<Broker>> rackBrokerCapacityMap = new HashMap<>();
    for (Broker broker : newBrokerList) {
      Integer capacity = instanceTypeThroughputMap.getOrDefault(broker.getInstanceType(),
          DEFAULT_CAPACITY);
      broker.setTotalNetworkCapacity(capacity);
      PriorityQueue<Broker> priorityQueue = rackBrokerCapacityMap.computeIfAbsent(
          broker.getLocality(),
          k -> new PriorityQueue<>(Comparator.comparingInt(Broker::getAvailableCapacity).reversed())
      );
      priorityQueue.add(broker);
    }

    int racks = rackBrokerCapacityMap.size();

    for (TopicConfig topicConfig : topicsList) {
      String topic = topicConfig.getTopic();
      double inputTrafficMB = topicConfig.getInputTrafficMB();
      double ceil = (Math.floor((inputTrafficMB * 1024 * 1024 / (topicConfig.getBatchSizeBytes() * topicConfig.getClusteringMultiplier()))));
      logger.fine("(" + topic + ")" + " ceil:" + ceil);
      int partitions = (int) (Math.round(ceil / racks)) * racks;
      if (partitions < racks) {
        partitions = racks;
      }

      int trafficPerPartition = (int) (inputTrafficMB / partitions);
      logger.info("(" + topic + ")" + " partitions:" + partitions + " traffic:" + inputTrafficMB
          + " partitions:" + partitions + " trafficPerPartition:" + trafficPerPartition);

      for (Map.Entry<String, PriorityQueue<Broker>> entry : rackBrokerCapacityMap.entrySet()) {
        int partitionsPerRack = partitions / racks;
        List<Broker> dequeuedBrokers = new ArrayList<>();
        List<Broker> ineligibleBrokers = new ArrayList<>();
        PriorityQueue<Broker> queue = entry.getValue();

        TopicAssignment assignment = new TopicAssignment(topicConfig, trafficPerPartition);
        while (!queue.isEmpty()) {
          Broker broker = queue.poll();
          if (broker.getAssignedTopics().contains(assignment)) {
            ineligibleBrokers.add(broker);
            broker.getAssignedTopics().remove(assignment);
            broker.getAssignedTopics().add(assignment); // update configs/traffic/timestamp
            partitionsPerRack--;
          } else {
            dequeuedBrokers.add(broker);
          }
        }
        if (partitionsPerRack < 0) {
          PriorityQueue<Broker> utilizationSortedBrokers = new PriorityQueue<>(
              Comparator.comparingInt(Broker::getAvailableCapacity).reversed()
          );
          utilizationSortedBrokers.addAll(ineligibleBrokers);
          for (int i = partitionsPerRack; i < 0; i++) {
            Broker b = utilizationSortedBrokers.poll();
            if (b != null) {
              b.getAssignedTopics().remove(assignment);
            }
          }
        }
        queue.addAll(dequeuedBrokers);
        dequeuedBrokers.clear();
        if (partitionsPerRack > queue.size()) {
          logger.severe("Insufficient number of nodes to host this topic:" + topic + " partitions:"
              + partitionsPerRack + " nodes:" + queue.size());
          break;
        } else if (partitionsPerRack > 0) {
          for (int i = 0; i < partitionsPerRack; i++) {
            Broker broker = queue.poll();
            dequeuedBrokers.add(broker);
            if (broker == null || broker.getAssignedTopics() == null) {
              logger.info("Failed to initialize broker assigned topic set, skipping broker");
              continue;
            }
            if (broker.getAssignedTopics().contains(assignment)) {
              broker.getAssignedTopics().remove(assignment);
              broker.getAssignedTopics().add(assignment);
              logger.info(
                  i + " Topic(" + topic + ") already assigned to node " + broker.getBrokerIP() + ", updating configs");
            } else if (broker.getAvailableCapacity() - trafficPerPartition > 0) {
              broker.getAssignedTopics().add(assignment);
              logger.info("(" + topic + ") assigned to broker:" + broker.getBrokerIP());
            } else {
              logger.severe(i + " (" + topic + ") Insufficient capacity left on nodes:" + broker
                  + " needed:" + trafficPerPartition + "\n" + queue);
            }
          }
        }
        queue.addAll(ineligibleBrokers);
        queue.addAll(dequeuedBrokers);
      }
    }

    return new HashSet<>(newBrokerList);
  }

  public void setDefaultExpirationTime(long defaultExpirationTime) {
    this.defaultExpirationTime = defaultExpirationTime;
  }

  private void printRackStatus(Map<String, PriorityQueue<Broker>> rackBrokerCapacityMap) {
    logger.info("Rack Status:");
    for (Map.Entry<String, PriorityQueue<Broker>> entry2 : rackBrokerCapacityMap.entrySet()) {
      logger.info("Rack:" + entry2.getKey() + "\n\t" + entry2.getValue());
    }
  }
}
