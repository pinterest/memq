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

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;

import com.google.gson.Gson;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.Broker.BrokerType;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.core.config.MemqConfig;

public class Balancer implements Runnable {

  static final Logger logger = Logger.getLogger(Balancer.class.getCanonicalName());
  private static final Gson GSON = new Gson();
  private CuratorFramework client;
  private LeaderSelector leaderSelector;
  private BalanceStrategy writeBalanceStrategy;
  private BalanceStrategy readBalanceStrategy;
  private MemqGovernor governor;
  private MemqConfig config;

  public Balancer(MemqConfig config,
                  MemqGovernor governor,
                  CuratorFramework client,
                  LeaderSelector leaderSelector) {
    this.config = config;
    this.governor = governor;
    this.client = client;
    this.leaderSelector = leaderSelector;
    this.writeBalanceStrategy = config.getClusteringConfig().isEnableExpiration()
        ? new ExpirationPartitionBalanceStrategy()
        : new PartitionBalanceStrategy();
    this.readBalanceStrategy = config.getClusteringConfig().isEnableExpiration()
        ? new ExpirationPartitionBalanceStrategy()
        : new PartitionBalanceStrategy();
  }

  @Override
  public void run() {
    boolean firstRun = true;
    while (true) {
      if (leaderSelector.hasLeadership()) {
        if (firstRun) {
          if (config.getTopicConfig() != null) {
            for (TopicConfig topipConfig : config.getTopicConfig()) {
              try {
                client.delete().forPath("/topics/" + topipConfig.getTopic());
                governor.createTopic(topipConfig);
              } catch (Exception e) {
              }
            }
          }
          firstRun = false;
        }

        // run topic provisioning and balancing
        logger.info("Running topic balancer");
        try {
          // get current cluster capacity
          Set<Broker> brokers = new HashSet<>();
          for (String id : client.getChildren().forPath("/brokers")) {
            byte[] brokerInfoBytes;
            try {
              brokerInfoBytes = client.getData().forPath("/brokers/" + id);
              String brokerInfo = new String(brokerInfoBytes);
              Broker broker = GSON.fromJson(brokerInfo, Broker.class);
              brokers.add(broker);
            } catch (Exception e) {
              logger.log(Level.SEVERE, "Unable to get broker information for:" + id, e);
            }
          }
          logger.info("Current brokers:" + brokers);

          Set<TopicConfig> topics = new HashSet<>();
          for (String topicName : client.getChildren().forPath("/topics")) {
            byte[] topicConfigBytes = client.getData().forPath("/topics/" + topicName);
            String topicConfig = new String(topicConfigBytes);
            TopicConfig topic = GSON.fromJson(topicConfig, TopicConfig.class);
            topics.add(topic);
          }
          balanceAndUpdateWriteBrokers(brokers, topics);
//          balanceAndUpdateReadBrokers(brokers, topics);
          logger.info("Updated brokers with topic assignments:" + brokers);
        } catch (Exception e) {
          logger.log(Level.SEVERE, "Exception during balancing", e);
        }
      }
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        logger.log(Level.SEVERE, "Balancer interrupted, exiting", e);
        break;
      }
    }
  }

  private void balanceAndUpdateWriteBrokers(Set<Broker> brokers,
                                            Set<TopicConfig> topics) throws Exception {
    Set<Broker> writeBrokers = brokers.stream().filter(
        v -> v.getBrokerType() == BrokerType.WRITE || v.getBrokerType() == BrokerType.READ_WRITE)
        .collect(Collectors.toSet());
    Set<Broker> newBrokers = writeBalanceStrategy.balance(topics, writeBrokers);
    // update brokers
    for (Broker broker : newBrokers) {
      client.setData().forPath("/brokers/" + broker.getBrokerIP(), GSON.toJson(broker).getBytes());
    }
  }

  private void balanceAndUpdateReadBrokers(Set<Broker> brokers,
                                           Set<TopicConfig> topics) throws Exception {
    Set<Broker> writeBrokers = brokers.stream()
        .filter(
            v -> v.getBrokerType() == BrokerType.READ || v.getBrokerType() == BrokerType.READ_WRITE)
        .collect(Collectors.toSet());
    Set<Broker> newBrokers = readBalanceStrategy.balance(topics, writeBrokers);
    // update brokers
    for (Broker broker : newBrokers) {
      client.setData().forPath("/brokers/" + broker.getBrokerIP(), GSON.toJson(broker).getBytes());
    }
  }

}
