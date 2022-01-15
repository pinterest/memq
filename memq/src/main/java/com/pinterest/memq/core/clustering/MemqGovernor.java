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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import com.google.gson.Gson;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.TopicAssignment;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.protocol.TopicMetadata;
import com.pinterest.memq.core.MemqManager;
import com.pinterest.memq.core.config.ClusteringConfig;
import com.pinterest.memq.core.config.EnvironmentProvider;
import com.pinterest.memq.core.config.MemqConfig;

public class MemqGovernor {

  private static final String ZNODE_BROKERS_BASE = "/brokers/";
  private static final String ZNODE_GOVERNOR = "/governor";
  private static final String ZNODE_BROKERS = "/brokers";
  private static final String ZNODE_TOPICS_BASE = "/topics";
  private static final String ZNODE_TOPICS = "/topics/";
  private static final Gson GSON = new Gson();
  private static final Logger logger = Logger.getLogger(MemqGovernor.class.getCanonicalName());
  private MemqConfig config;
  private MemqManager mgr;
  private EnvironmentProvider provider;
  private Map<String, TopicMetadata> topicMetadataMap = new ConcurrentHashMap<>();
  private LeaderSelector leaderSelector;
  private CuratorFramework client;
  private ClusteringConfig clusteringConfig;
  private String brokerZnodePath;

  public MemqGovernor(MemqManager mgr, MemqConfig config, EnvironmentProvider provider) {
    this.mgr = mgr;
    this.config = config;
    if (config.getTopicConfig() != null) {
      Broker broker = new Broker(provider.getIP(), config.getNettyServerConfig().getPort(),
          provider.getInstanceType(), provider.getRack(), config.getBrokerType(),
          mgr.getTopicAssignment());
      for (TopicConfig tc : config.getTopicConfig()) {
        switch (config.getBrokerType()) {
        case READ:
          topicMetadataMap.put(tc.getTopic(),
              new TopicMetadata(tc.getTopic(), Collections.emptySet(),
                  Collections.singleton(broker), tc.getStorageHandlerName(),
                  tc.getStorageHandlerConfig()));
          break;
        case WRITE:
          topicMetadataMap.put(tc.getTopic(),
              new TopicMetadata(tc.getTopic(), Collections.singleton(broker),
                  Collections.emptySet(), tc.getStorageHandlerName(),
                  tc.getStorageHandlerConfig()));
        case READ_WRITE:
          topicMetadataMap.put(tc.getTopic(),
              new TopicMetadata(tc.getTopic(), Collections.singleton(broker),
                  Collections.singleton(broker), tc.getStorageHandlerName(),
                  tc.getStorageHandlerConfig()));
        }

      }
    }
    this.clusteringConfig = config.getClusteringConfig();
    this.provider = provider;
  }

  public void init() throws Exception {
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    String zookeeperConnectionString = clusteringConfig.getZookeeperConnectionString();
    client = CuratorFrameworkFactory.builder().connectString(zookeeperConnectionString)
        .retryPolicy(retryPolicy).sessionTimeoutMs(30_000).build();
    client.start();
    client.blockUntilConnected(100, TimeUnit.SECONDS);
    logger.info("Connected to zookeeper:" + zookeeperConnectionString);

    initializeZNodesAndWatchers(client);

    leaderSelector = new LeaderSelector(client, ZNODE_GOVERNOR, new LeaderSelectorListener() {

      @Override
      public void stateChanged(CuratorFramework client, ConnectionState newState) {
        logger.info("Connection state changed:" + newState);
      }

      @Override
      public void takeLeadership(CuratorFramework client) throws Exception {
        logger.info("Elected leader for cluster");
        // start leader process
        client.setData().forPath(ZNODE_GOVERNOR, provider.getIP().getBytes());
        while (true) {
          Thread.sleep(5000);
        }
      }
    });

    if (clusteringConfig.isEnableBalancer()) {
      Balancer balancer = new Balancer(config, this, client, leaderSelector);
      Thread thBalancer = new Thread(balancer);
      thBalancer.setName("BalancerThread");
      thBalancer.setDaemon(true);
      thBalancer.start();
    }

    Thread th = new Thread(new MetadataPoller(client, topicMetadataMap));
    th.setName("MetadataPollerThread");
    th.setDaemon(true);
    th.start();

    if (clusteringConfig.isEnableLeaderSelector()) {
      leaderSelector.autoRequeue();
      leaderSelector.start();
    }
  }

  public static void createTopic(CuratorFramework client, TopicConfig config) throws Exception {
    if (client.checkExists().forPath(ZNODE_TOPICS + config.getTopic()) != null) {
      throw new Exception("Topic exists");
    } else {
      client.create().forPath(ZNODE_TOPICS + config.getTopic(), GSON.toJson(config).getBytes());
    }
  }

  private void initializeZNodesAndWatchers(CuratorFramework client) throws Exception {
    if (client.checkExists().forPath(ZNODE_TOPICS_BASE) == null) {
      client.create().withMode(CreateMode.PERSISTENT).forPath(ZNODE_TOPICS_BASE);
    }
    if (client.checkExists().forPath(ZNODE_BROKERS) == null) {
      client.create().withMode(CreateMode.PERSISTENT).forPath(ZNODE_BROKERS);
    }

    Broker broker = new Broker(provider.getIP(), config.getNettyServerConfig().getPort(),
        provider.getInstanceType(), provider.getRack(), config.getBrokerType(),
        mgr.getTopicAssignment());

    brokerZnodePath = ZNODE_BROKERS_BASE + broker.getBrokerIP();
    if (client.checkExists().forPath(brokerZnodePath) != null) {
      client.delete().forPath(brokerZnodePath);
    }
    client.create().withMode(CreateMode.EPHEMERAL).forPath(brokerZnodePath,
        GSON.toJson(broker).getBytes());

    if (clusteringConfig.isEnableLocalAssigner()) {
      Thread th = new Thread(new TopicAssignmentWatcher(mgr, brokerZnodePath, broker, client));
      th.setDaemon(true);
      th.setName("TopicAssignmentWatcher");
      th.start();
    }
  }

  public Map<String, TopicMetadata> getTopicMetadataMap() {
    return topicMetadataMap;
  }

  public static List<String> convertTopicAssignmentsSetToList(Set<TopicAssignment> topicAssignments) {
    return topicAssignments.stream().map(TopicConfig::getTopic).collect(Collectors.toList());
  }

  public void stop() {
    if (brokerZnodePath != null && client != null) {
      try {
        // best effort remove broker znode during shutdown
        client.delete().forPath(brokerZnodePath);
      } catch (Exception e) {
        logger.log(Level.WARNING,
            "Failed to deregister broker znode, will be removed after session timeout", e);
      }
    }
    leaderSelector.close();
    client.close();
  }

  public void createTopic(TopicConfig topipConfig) throws Exception {
    createTopic(client, topipConfig);
  }

}
