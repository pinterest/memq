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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.zookeeper.KeeperException;

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
        ? new ExpirationPartitionBalanceStrategyWithAssignmentFreeze(config)
        : new PartitionBalanceStrategy();
    this.readBalanceStrategy = config.getClusteringConfig().isEnableExpiration()
        ? new ExpirationPartitionBalanceStrategyWithAssignmentFreeze(config)
        : new PartitionBalanceStrategy();
  }

  @Override
  public void run() {
    boolean firstRun = true;
    while (true) {
      if (leaderSelector.hasLeadership()) {
        if (firstRun) {
          if (config.getTopicConfig() != null) {
            for (TopicConfig topicConfig : config.getTopicConfig()) {
              try {
                client.delete().forPath(MemqGovernor.ZNODE_TOPICS_BASE + topicConfig.getTopic());
                governor.createTopic(topicConfig);
              } catch (Exception e) {
              }
            }
          }
          firstRun = false;
        }

        // run topic provisioning and balancing
        logger.info("Running topic balancer");
        try {
          // Capture the leadership epoch before computing anything. This is the /governor
          // version THIS broker wrote when it took leadership (pinned to our own term for
          // the life of the term, never a successor's version). Committing the plan against
          // this token means a plan computed under a stale term is rejected, even if our
          // local hasLeadership() briefly lags after we lose the session. Reading the live
          // /governor version here instead would be unsafe: if a successor had already taken
          // over and bumped it, we would capture the successor's epoch and defeat the fence.
          int fencingVersion = governor.getLeadershipEpoch();

          // get current cluster capacity
          Set<Broker> brokers = new HashSet<>();
          for (String id : client.getChildren().forPath(MemqGovernor.ZNODE_BROKERS)) {
            byte[] brokerInfoBytes;
            try {
              brokerInfoBytes = client.getData().forPath(MemqGovernor.ZNODE_BROKERS_BASE + id);
              String brokerInfo = new String(brokerInfoBytes);
              Broker broker = GSON.fromJson(brokerInfo, Broker.class);
              brokers.add(broker);
            } catch (Exception e) {
              logger.log(Level.SEVERE, "Unable to get broker information for:" + id, e);
            }
          }
          logger.info("Current brokers:" + brokers);

          Set<TopicConfig> topics = new HashSet<>();
          for (String topicName : client.getChildren().forPath(MemqGovernor.ZNODE_TOPICS)) {
            byte[] topicConfigBytes = client.getData().forPath(MemqGovernor.ZNODE_TOPICS_BASE + topicName);
            String topicConfig = new String(topicConfigBytes);
            TopicConfig topic = GSON.fromJson(topicConfig, TopicConfig.class);
            topics.add(topic);
          }
          balanceAndUpdateWriteBrokers(brokers, topics, fencingVersion);
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
                                            Set<TopicConfig> topics,
                                            int fencingVersion) throws Exception {
    Set<Broker> writeBrokers = brokers.stream().filter(
        v -> v.getBrokerType() == BrokerType.WRITE || v.getBrokerType() == BrokerType.READ_WRITE)
        .collect(Collectors.toSet());
    publishAssignments(writeBalanceStrategy.balance(topics, writeBrokers), fencingVersion);
  }

  private void balanceAndUpdateReadBrokers(Set<Broker> brokers,
                                           Set<TopicConfig> topics,
                                           int fencingVersion) throws Exception {
    Set<Broker> writeBrokers = brokers.stream()
        .filter(
            v -> v.getBrokerType() == BrokerType.READ || v.getBrokerType() == BrokerType.READ_WRITE)
        .collect(Collectors.toSet());
    publishAssignments(readBalanceStrategy.balance(topics, writeBrokers), fencingVersion);
  }

  /**
   * Atomically writes computed assignments back to the broker znodes, fenced to the
   * leadership term the plan was computed under.
   *
   * <p>
   * A plain {@code hasLeadership()} recheck is only a point-in-time guard: it cannot ensure
   * leadership was held continuously from plan computation through every write. With
   * {@code autoRequeue()} a governor can lose its session, have a successor take over, then
   * reacquire leadership and publish a plan built under the now-defunct term, fighting the
   * new governor over assignments.
   * </p>
   *
   * <p>
   * To fence this, the writes are issued as a single ZooKeeper transaction guarded by a
   * version check on {@link MemqGovernor#ZNODE_GOVERNOR} ({@code fencingVersion}). That
   * token is {@link MemqGovernor#getLeadershipEpoch() the version this broker wrote when it
   * took leadership}, captured before computing the plan. Because {@code /governor} is
   * rewritten on every leadership term, any intervening term change bumps its version and
   * ZooKeeper rejects the whole transaction atomically, so a stale plan is never partially
   * applied.
   * </p>
   */
  void publishAssignments(Set<Broker> newBrokers, int fencingVersion) throws Exception {
    if (!leaderSelector.hasLeadership()) {
      logger.warning("Lost cluster leadership while balancing, discarding computed assignments");
      return;
    }
    if (fencingVersion < 0) {
      logger.warning("Governor epoch znode missing, discarding computed assignments");
      return;
    }
    if (newBrokers.isEmpty()) {
      return;
    }
    List<CuratorOp> ops = new ArrayList<>();
    ops.add(client.transactionOp().check().withVersion(fencingVersion)
        .forPath(MemqGovernor.ZNODE_GOVERNOR));
    for (Broker broker : newBrokers) {
      ops.add(client.transactionOp().setData().forPath(
          MemqGovernor.ZNODE_BROKERS_BASE + broker.getBrokerIP(), GSON.toJson(broker).getBytes()));
    }
    try {
      client.transaction().forOperations(ops);
    } catch (KeeperException.BadVersionException e) {
      logger.warning("Governor epoch changed (leadership term ended) during balancing; "
          + "discarding stale assignments computed under the previous term");
    }
  }

}
