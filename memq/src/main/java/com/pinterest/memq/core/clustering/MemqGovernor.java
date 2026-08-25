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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.IntConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import com.codahale.metrics.MetricRegistry;
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

  public static final String ZNODE_GOVERNOR = "/governor";
  public static final String ZNODE_BROKERS_BASE = "/brokers/";
  public static final String ZNODE_BROKERS = "/brokers";
  public static final String ZNODE_TOPICS_BASE = "/topics/";
  public static final String ZNODE_TOPICS = "/topics";
  public static final String METRIC_BROKER_ZNODE_MISSING = "broker.znode.missing";
  public static final String METRIC_BROKER_REREGISTERED = "broker.znode.reregistered";
  public static final String METRIC_LEADERSHIP_ACQUIRED = "governor.leadership.acquired";
  public static final String METRIC_LEADERSHIP_SET = "governor.leadership.set";
  public static final String METRIC_LEADERSHIP_RELINQUISHED = "governor.leadership.relinquished";
  private static final long LEADERSHIP_PARK_INTERVAL_MS = 5000;
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
  private volatile boolean closed = false;
  // Set on LOST so we only re-register on the following RECONNECTED. A plain
  // SUSPENDED -> RECONNECTED keeps the same session, so the ephemeral broker
  // znode survives and must not be rewritten.
  private volatile boolean sessionLost = false;
  // The /governor znode version written when THIS broker most recently took leadership.
  // It is pinned to our own term and never reflects a successor's write, so the balancer
  // can fence a computed plan to the exact term it was built under even if our local
  // hasLeadership() lags reality. -1 means we do not currently hold a leadership term.
  private volatile int leadershipEpoch = -1;
  private MetricRegistry metricRegistry;
  private final ExecutorService reRegistrationExecutor = Executors.newSingleThreadExecutor(r -> {
    Thread t = new Thread(r, "BrokerReRegistration");
    t.setDaemon(true);
    return t;
  });

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

    // Reuse the shared registry map so the clustering counters are exported alongside
    // the other "_"-prefixed (non-topic) metrics.
    Map<String, MetricRegistry> registryMap = mgr.getRegistry();
    metricRegistry = registryMap != null
        ? registryMap.computeIfAbsent("_cluster", k -> new MetricRegistry())
        : new MetricRegistry();

    // The broker registers itself as an EPHEMERAL znode that is tied to the ZK
    // session. Only a session expiry (LOST) deletes the node server-side and forces
    // Curator onto a brand new session; a plain SUSPENDED -> RECONNECTED keeps the
    // same session and the znode intact. So re-announce the broker only on the
    // RECONNECTED that follows a LOST, otherwise we needlessly rewrite a live znode.
    client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
      @Override
      public void stateChanged(CuratorFramework cf, ConnectionState newState) {
        logger.info("Curator connection state changed:" + newState);
        if (closed) {
          return;
        }
        if (newState == ConnectionState.LOST) {
          // Session expired: ZooKeeper has dropped our ephemeral broker znode.
          sessionLost = true;
          metricRegistry.counter(METRIC_BROKER_ZNODE_MISSING).inc();
          logger.warning("ZK session lost, broker znode is now missing:" + brokerZnodePath);
        } else if (newState == ConnectionState.RECONNECTED) {
          if (sessionLost) {
            sessionLost = false;
            // Run off the Curator event thread to avoid blocking it on ZK operations.
            reRegisterBroker();
          } else {
            logger.info(
                "Reconnected without session loss, broker znode intact, skipping re-registration:"
                    + brokerZnodePath);
          }
        }
      }
    });

    initializeZNodesAndWatchers(client);

    leaderSelector = new LeaderSelector(client, ZNODE_GOVERNOR, new GovernorLeadershipListener(
        provider.getIP(), () -> closed, metricRegistry, this::setLeadershipEpoch));

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
    if (client.checkExists().forPath(ZNODE_TOPICS_BASE + config.getTopic()) != null) {
      throw new Exception("Topic exists");
    } else {
      client.create().forPath(ZNODE_TOPICS_BASE + config.getTopic(), GSON.toJson(config).getBytes());
    }
  }

  private void initializeZNodesAndWatchers(CuratorFramework client) throws Exception {
    if (client.checkExists().forPath(ZNODE_TOPICS) == null) {
      client.create().withMode(CreateMode.PERSISTENT).forPath(ZNODE_TOPICS);
    }
    if (client.checkExists().forPath(ZNODE_BROKERS) == null) {
      client.create().withMode(CreateMode.PERSISTENT).forPath(ZNODE_BROKERS);
    }

    Broker broker = registerBroker(false);

    if (clusteringConfig.isEnableLocalAssigner()) {
      Thread th = new Thread(
          new TopicAssignmentWatcher(mgr, brokerZnodePath, broker, client, this::reRegisterBroker));
      th.setDaemon(true);
      th.setName("TopicAssignmentWatcher");
      th.start();
    }
  }

  /**
   * Idempotently (re)creates this broker's EPHEMERAL znode under {@code /brokers/<ip>}.
   *
   * <p>
   * A fresh {@link Broker} snapshot is built on every call so the published znode always
   * reflects the broker's current topic assignment (the assignment can change between the
   * initial registration and a later re-registration triggered by a ZK session expiry).
   * The method tolerates the races inherent to recreating an ephemeral node whose previous
   * incarnation may still be lingering on the just-expired session.
   * </p>
   */
  synchronized Broker registerBroker(boolean reRegistration) throws Exception {
    Broker broker = new Broker(provider.getIP(), config.getNettyServerConfig().getPort(),
        provider.getInstanceType(), provider.getRack(), config.getBrokerType(),
        mgr.getTopicAssignment());
    brokerZnodePath = ZNODE_BROKERS_BASE + broker.getBrokerIP();
    if (closed) {
      return broker;
    }
    byte[] brokerData = GSON.toJson(broker).getBytes();
    // Parent could be missing if it never existed yet; ensure it before the child create.
    if (client.checkExists().forPath(ZNODE_BROKERS) == null) {
      try {
        client.create().withMode(CreateMode.PERSISTENT).forPath(ZNODE_BROKERS);
      } catch (KeeperException.NodeExistsException ignore) {
        // created concurrently, fine
      }
    }
    try {
      if (client.checkExists().forPath(brokerZnodePath) != null) {
        client.delete().forPath(brokerZnodePath);
      }
      client.create().withMode(CreateMode.EPHEMERAL).forPath(brokerZnodePath, brokerData);
      logger.info("Registered broker ephemeral znode:" + brokerZnodePath);
    } catch (KeeperException.NodeExistsException e) {
      // A stale ephemeral from the prior (expired) session may briefly linger, or a
      // concurrent re-registration won the race. Refresh the payload and move on.
      client.setData().forPath(brokerZnodePath, brokerData);
      logger.info("Broker znode already existed, refreshed data:" + brokerZnodePath);
    } catch (KeeperException.NoNodeException e) {
      // delete() raced with another deletion; the node is gone so just (re)create it.
      client.create().withMode(CreateMode.EPHEMERAL).forPath(brokerZnodePath, brokerData);
      logger.info("Re-created broker ephemeral znode after NoNode race:" + brokerZnodePath);
    }
    if (reRegistration) {
      metricRegistry.counter(METRIC_BROKER_REREGISTERED).inc();
    }
    return broker;
  }

  /**
   * Best-effort, non-throwing re-registration used as a recovery callback (e.g. when the
   * {@link TopicAssignmentWatcher} notices the broker znode has vanished). The primary
   * recovery path remains the {@link ConnectionStateListener}; this is a safety net.
   */
  void reRegisterBroker() {
    if (closed) {
      return;
    }
    reRegistrationExecutor.submit(() -> {
      try {
        registerBroker(true);
      } catch (Exception e) {
        logger.log(Level.SEVERE, "Failed to re-register broker znode", e);
      }
    });
  }

  public Map<String, TopicMetadata> getTopicMetadataMap() {
    return topicMetadataMap;
  }

  public static List<String> convertTopicAssignmentsSetToList(Set<TopicAssignment> topicAssignments) {
    return topicAssignments.stream().map(TopicConfig::getTopic).collect(Collectors.toList());
  }

  public Set<Broker> getAllBrokersInRack(String rack) {
    if (client == null) {
      return Collections.emptySet();
    }
    try {
      List<String> children = client.getChildren().forPath(ZNODE_BROKERS);
      Set<Broker> brokers = new HashSet<>();
      for (String child : children) {
        byte[] data = client.getData().forPath(ZNODE_BROKERS_BASE + child);
        Broker broker = GSON.fromJson(new String(data), Broker.class);
        if (rack == null || rack.equals(broker.getLocality())) {
          brokers.add(broker);
        }
      }
      return brokers;
    } catch (Exception e) {
      logger.log(Level.WARNING, "Failed to get brokers in rack " + rack + " from ZK", e);
      return Collections.emptySet();
    }
  }

  public void stop() {
    // Flip closed first so any in-flight RECONNECTED callback short-circuits instead of
    // racing the shutdown by recreating the broker znode we are about to delete.
    closed = true;
    reRegistrationExecutor.shutdownNow();
    if (brokerZnodePath != null && client != null) {
      try {
        // best effort remove broker znode during shutdown
        client.delete().forPath(brokerZnodePath);
      } catch (Exception e) {
        logger.log(Level.WARNING,
            "Failed to deregister broker znode, will be removed after session timeout", e);
      }
    }
    if (leaderSelector != null) {
      try {
        leaderSelector.close();
      } catch (IllegalStateException e) {
        // leader selector may not have been started (e.g. disabled via config); ignore
        logger.log(Level.FINE, "Leader selector was not started, skipping close", e);
      }
    }
    if (client != null) {
      client.close();
    }
  }

  public void createTopic(TopicConfig topipConfig) throws Exception {
    createTopic(client, topipConfig);
  }

  public boolean hasLeadership() {
    return leaderSelector.hasLeadership();
  }

  private void setLeadershipEpoch(int epoch) {
    this.leadershipEpoch = epoch;
  }

  /**
   * Returns the fencing token for this broker's current leadership term: the version of the
   * {@link #ZNODE_GOVERNOR} znode that this broker wrote when it took leadership, or
   * {@code -1} if it does not currently hold a term. The value is pinned to our own term, so
   * it never reflects a write from a successor governor.
   */
  int getLeadershipEpoch() {
    return leadershipEpoch;
  }

  public CuratorFramework getCuratorFramework() {
    return client;
  }

  /**
   * Holds cluster leadership for as long as this broker is both running and able to prove
   * to ZooKeeper that it still owns the {@code /governor} lock.
   *
   * <p>
   * Extending {@link LeaderSelectorListenerAdapter} is load bearing, not stylistic. Curator
   * cannot take leadership away on its own: {@code LeaderSelector.hasLeadership()} stays
   * true until {@link #takeLeadership(CuratorFramework)} returns, and the underlying
   * {@code InterProcessMutex} has no idea that ZooKeeper deleted its ephemeral lock znode
   * when the session expired. The only path back out is {@code stateChanged} throwing
   * {@link org.apache.curator.framework.recipes.leader.CancelLeadershipException}, which
   * Curator catches and turns into an interrupt of the leadership thread. The adapter
   * supplies exactly that behavior for every state the connection state error policy
   * considers an error. A listener that merely logs the state change leaves a deposed
   * governor believing it is still the leader for the rest of the process's life, which
   * produces two live governors while ZooKeeper only ever reflects the newest one.
   * </p>
   *
   * <p>
   * Under the default {@code StandardConnectionStateErrorPolicy} this steps down on
   * SUSPENDED as well as LOST. That is deliberate: stepping down as soon as the ensemble
   * becomes unreachable is strictly earlier than waiting to be told the session expired, so
   * it shrinks the window in which a successor could already be balancing. Leadership is
   * cheap to reacquire because the selector is started with {@code autoRequeue()}.
   * </p>
   */
  static final class GovernorLeadershipListener extends LeaderSelectorListenerAdapter {

    private final String governorIp;
    private final BooleanSupplier stopped;
    private final MetricRegistry metricRegistry;
    private final IntConsumer epochSink;

    GovernorLeadershipListener(String governorIp,
                               BooleanSupplier stopped,
                               MetricRegistry metricRegistry,
                               IntConsumer epochSink) {
      this.governorIp = governorIp;
      this.stopped = stopped;
      this.metricRegistry = metricRegistry;
      this.epochSink = epochSink;
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
      logger.info("Elected leader for cluster:" + governorIp);
      metricRegistry.counter(METRIC_LEADERSHIP_ACQUIRED).inc();
      try {
        // start leader process
        Stat stat = client.setData().forPath(ZNODE_GOVERNOR, governorIp.getBytes());
        // Pin the fencing epoch to the version WE just wrote. This is our term's token; it
        // never reflects a successor's write, so a plan the balancer computes under this
        // term can be fenced precisely even if our local hasLeadership() lags a step behind.
        epochSink.accept(stat.getVersion());
        // Distinct from acquiring leadership: this confirms we published our identity as
        // the governor, which is what the rest of the cluster reads.
        metricRegistry.counter(METRIC_LEADERSHIP_SET).inc();
        // Returning from here relinquishes leadership, so park until the broker shuts down
        // or Curator interrupts us because the connection to ZooKeeper degraded.
        while (!stopped.getAsBoolean() && !Thread.currentThread().isInterrupted()) {
          Thread.sleep(LEADERSHIP_PARK_INTERVAL_MS);
        }
      } catch (InterruptedException e) {
        // Expected step-down path: Curator interrupts the leadership thread (via the
        // adapter's CancelLeadershipException) when the connection degrades. Restore the
        // flag and fall through to relinquish.
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        // setData (or another operation) failed for a reason we can't assume; log the
        // actual cause instead of attributing it to a degraded connection.
        logger.log(Level.WARNING, "Relinquished cluster leadership due to error:" + governorIp, e);
        throw e;
      } finally {
        // Invalidate our term epoch first, so the balancer stops publishing the instant we
        // step down no matter how leadership ended.
        epochSink.accept(-1);
        metricRegistry.counter(METRIC_LEADERSHIP_RELINQUISHED).inc();
        if (stopped.getAsBoolean()) {
          logger.info("Relinquished cluster leadership during shutdown:" + governorIp);
        } else if (Thread.currentThread().isInterrupted()) {
          logger.warning("Relinquished cluster leadership, Curator revoked it because the "
              + "ZooKeeper connection degraded:" + governorIp);
        } else {
          logger.warning("Relinquished cluster leadership:" + governorIp);
        }
      }
    }
  }

}
