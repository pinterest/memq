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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.logging.Logger;

import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.TopicAssignment;
import com.pinterest.memq.commons.protocol.TopicConfig;

/**
 * This balancer uses a stable balancing strategy. Topics are allocated to nodes
 * based on the concept of partitions.
 * 
 * A partition is defined as batchsize*2, therefore total number of partitions used by a
 * topic are calculated based on traffic/partitionsize.
 * 
 * Topics are allocated in sequence of the topic ordering (essentially an index
 * that shows when was this topic provisioned on this cluster)
 * 
 * e.g. Topic A, with 100MB/s and batch size of 15MB = 100/(15*2) = 3.3 partitions
 * This will result is 3 partitions in total.
 * 
 * This balancer honors the following constraints: 1. A broker can have at most
 * 1 partition for a topic 2. Allocation is performed on a node with most capacity
 * available 3. Minimum no.of partitions = no.of racks in the cluster
 */
public class PartitionBalanceStrategy extends BalanceStrategy {

  private static final int DEFAULT_CAPACITY = 150;
  static final Logger logger = Logger.getLogger(PartitionBalanceStrategy.class.getCanonicalName());
  private Map<String, Integer> instanceTypeThroughputMap = new HashMap<>();

  @Override
  public Set<Broker> balance(Set<TopicConfig> topics, Set<Broker> brokers) {
    List<Broker> brokerList = new ArrayList<>(brokers);
    Map<Broker, Set<TopicAssignment>> newAssignments = new HashMap<>();
    Collections.sort(brokerList);

    List<TopicConfig> topicsList = new ArrayList<>(topics);
    Collections.sort(topicsList);
    System.out.println(topicsList);

    Map<String, PriorityQueue<Broker>> rackBrokerCapacityMap = new HashMap<>();
    for (Broker broker : brokerList) {
      Integer capacity = instanceTypeThroughputMap.getOrDefault(broker.getInstanceType(),
          DEFAULT_CAPACITY);
      if (broker.getTotalNetworkCapacity() == 0) {
        broker.setTotalNetworkCapacity(capacity);
      }
      PriorityQueue<Broker> priorityQueue = rackBrokerCapacityMap.computeIfAbsent(
          broker.getLocality(),
          k -> new PriorityQueue<>(
              (o1, o2) -> Integer.compare(o2.getAvailableCapacity(), o1.getAvailableCapacity())
          )
      );
      priorityQueue.add(broker);
    }

    printRackStatus(rackBrokerCapacityMap);

    int racks = rackBrokerCapacityMap.size();
    logger.info("Racks:" + racks + "\n\n");
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

      for (Entry<String, PriorityQueue<Broker>> entry : rackBrokerCapacityMap.entrySet()) {
        int partitionsPerRack = partitions / racks;
        List<Broker> dequeuedBrokers = new ArrayList<>();
        List<Broker> ineligibleBrokers = new ArrayList<>();
        PriorityQueue<Broker> queue = entry.getValue();
        TopicAssignment conf = new TopicAssignment(topicConfig, trafficPerPartition);
        while (!queue.isEmpty()) {
          Broker broker = queue.poll();
          if (broker.getAssignedTopics().contains(conf)) {
            Set<TopicAssignment> newBrokerAssignment = newAssignments.computeIfAbsent(broker, k -> new HashSet<>());
            newBrokerAssignment.add(conf);
            ineligibleBrokers.add(broker);
            partitionsPerRack--;
          } else {
            dequeuedBrokers.add(broker);
          }
        }
        if (partitionsPerRack < 0) {
          PriorityQueue<Broker> utilizationSortedBrokers = new PriorityQueue<>(
              (b1, b2) -> {
                double broker1Available = b1.getTotalNetworkCapacity() - newAssignments.get(b1).stream().mapToDouble(TopicConfig::getInputTrafficMB).sum();
                double broker2Available = b2.getTotalNetworkCapacity() - newAssignments.get(b2).stream().mapToDouble(TopicConfig::getInputTrafficMB).sum();
                return Double.compare(broker2Available, broker1Available);
              }
          );
          utilizationSortedBrokers.addAll(ineligibleBrokers);
          for (int i = partitionsPerRack; i < 0; i++) {
            Broker b = utilizationSortedBrokers.poll();
            newAssignments.get(b).remove(conf);
          }
        }
        queue.addAll(dequeuedBrokers);
        dequeuedBrokers.clear();
        if (partitionsPerRack > queue.size()) {
          logger.severe("Insufficient number of nodes to host this topic:" + topic + " partitions:"
              + partitionsPerRack + " nodes:" + queue.size());
          break;
        } else {
          for (int i = 0; i < partitionsPerRack; i++) {
            Broker broker = queue.poll();
            dequeuedBrokers.add(broker);
            if (broker == null || broker.getAssignedTopics() == null) {
              logger.info("Failed to initialize broker assigned topic set, skipping broker");
              continue;
            }
            Set<TopicAssignment> newBrokerAssignment = newAssignments.computeIfAbsent(broker, k -> new HashSet<>());
            if (broker.getAssignedTopics().contains(conf)) {
              newBrokerAssignment.add(conf);
              logger.info(
                  i + " Topic(" + topic + ") already assigned to node " + broker.getBrokerIP() + ", updating configs");
            } else if (broker.getAvailableCapacity() - trafficPerPartition > 0) {
              newBrokerAssignment.add(conf);
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

    // update all brokers with new assignments
    for (Broker broker : brokers) {
      broker.setAssignedTopics(newAssignments.getOrDefault(broker, Collections.emptySet()));
    }

    return brokers;
  }

  private void printRackStatus(Map<String, PriorityQueue<Broker>> rackBrokerCapacityMap) {
    logger.info("Rack Status:");
    for (Entry<String, PriorityQueue<Broker>> entry2 : rackBrokerCapacityMap.entrySet()) {
      logger.info("Rack:" + entry2.getKey() + "\n\t" + entry2.getValue());
    }
  }

}