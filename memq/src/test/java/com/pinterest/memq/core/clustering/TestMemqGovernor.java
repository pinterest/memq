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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.StandardConnectionStateErrorPolicy;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.pinterest.memq.commons.protocol.Broker.BrokerType;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.core.MemqManager;
import com.pinterest.memq.core.clustering.MemqGovernor.GovernorLeadershipListener;
import com.pinterest.memq.core.config.ClusteringConfig;
import com.pinterest.memq.core.config.EnvironmentProvider;
import com.pinterest.memq.core.config.MemqConfig;
import com.pinterest.memq.core.config.NettyServerConfig;

public class TestMemqGovernor {

  @Test
  public void testBackwardsCompatibility() throws JsonSyntaxException, JsonIOException,
                                           FileNotFoundException {
    Gson gson = new Gson();
    TopicConfig oldConf = gson.fromJson(new FileReader("src/test/resources/old.test_topic.json"),
        TopicConfig.class);
    TopicConfig newConf = gson.fromJson(new FileReader("src/test/resources/new.test_topic.json"),
        TopicConfig.class);
    assertEquals("customs3aync2", oldConf.getStorageHandlerName());
    assertEquals("customs3aync2", newConf.getStorageHandlerName());
  }

  /**
   * Simulates a ZooKeeper session expiry and verifies that the broker re-creates its
   * ephemeral {@code /brokers/<ip>} znode automatically, without a process restart.
   */
  @Test
  public void testBrokerReRegistersAfterSessionExpiry() throws Exception {
    String brokerIp = "10.0.0.42";
    String expectedPath = MemqGovernor.ZNODE_BROKERS_BASE + brokerIp;

    try (TestingServer testingServer = new TestingServer()) {
      Map<String, MetricRegistry> registryMap = new HashMap<>();
      // Keep the test focused on registration; disable the extra background machinery.
      MemqGovernor governor = newGovernor(testingServer.getConnectString(), brokerIp, registryMap,
          false);
      try {
        governor.init();
        CuratorFramework client = governor.getCuratorFramework();

        assertNotNull("broker znode should exist after init",
            client.checkExists().forPath(expectedPath));

        // Force the current ZK session to expire. ZooKeeper deletes the ephemeral
        // broker znode server-side as a result.
        ZooKeeper zk = client.getZookeeperClient().getZooKeeper();
        KillSession.kill(zk);

        // After Curator establishes a new session and fires RECONNECTED, the governor's
        // ConnectionStateListener must recreate the ephemeral broker znode.
        assertTrue("broker znode should be re-created automatically after session expiry",
            waitForPath(client, expectedPath, 60_000));

        MetricRegistry clusterRegistry = registryMap.get("_cluster");
        assertNotNull("cluster metric registry should be created", clusterRegistry);
        assertTrue("missing znode metric should be emitted",
            clusterRegistry.counter(MemqGovernor.METRIC_BROKER_ZNODE_MISSING).getCount() >= 1);
        // Re-registration is performed asynchronously off the Curator event thread.
        assertTrue("re-registration metric should be emitted",
            waitForCounter(clusterRegistry, MemqGovernor.METRIC_BROKER_REREGISTERED, 30_000));
      } finally {
        governor.stop();
      }
    }
  }

  /**
   * End-to-end guard against the governor split brain: a governor whose ZK session expires
   * must give up leadership instead of holding onto a lock that ZooKeeper already handed to
   * someone else.
   *
   * <p>
   * With a listener that only logs connection state changes, the deposed governor stays
   * parked inside {@code takeLeadership} forever, so {@code hasLeadership()} keeps returning
   * true and its balancer keeps writing topic assignments while the successor does the same.
   * </p>
   */
  @Test
  public void testGovernorRelinquishesLeadershipOnSessionExpiry() throws Exception {
    try (TestingServer testingServer = new TestingServer()) {
      Map<String, MetricRegistry> registryA = new HashMap<>();
      Map<String, MetricRegistry> registryB = new HashMap<>();
      MemqGovernor governorA = newGovernor(testingServer.getConnectString(), "10.0.0.1", registryA,
          true);
      MemqGovernor governorB = newGovernor(testingServer.getConnectString(), "10.0.0.2", registryB,
          true);
      try {
        governorA.init();
        governorB.init();

        assertTrue("exactly one governor should win the initial election",
            waitFor(() -> governorA.hasLeadership() ^ governorB.hasLeadership(), 30_000));

        boolean governorAWasLeader = governorA.hasLeadership();
        MemqGovernor leader = governorAWasLeader ? governorA : governorB;
        MetricRegistry leaderRegistry = (governorAWasLeader ? registryA : registryB).get("_cluster");

        KillSession.kill(leader.getCuratorFramework().getZookeeperClient().getZooKeeper());

        // Asserting on the counter rather than on hasLeadership() because the deposed
        // governor is started with autoRequeue and may win the next election immediately,
        // which would hide the step down from a poll of the flag.
        assertTrue("deposed governor must relinquish leadership once its ZK session expires",
            waitForCounter(leaderRegistry, MemqGovernor.METRIC_LEADERSHIP_RELINQUISHED, 30_000));

        assertTrue("cluster must settle back on exactly one governor",
            waitFor(() -> governorA.hasLeadership() ^ governorB.hasLeadership(), 30_000));
      } finally {
        governorB.stop();
        governorA.stop();
      }
    }
  }

  /**
   * The connection states Curator's error policy flags must abort leadership. This is the
   * contract that {@code LeaderSelector} relies on to interrupt the leadership thread, and
   * it is the piece that a hand written {@code LeaderSelectorListener} silently drops.
   */
  @Test
  public void testLeadershipIsCancelledOnConnectionErrorStates() {
    CuratorFramework client = mock(CuratorFramework.class);
    when(client.getConnectionStateErrorPolicy())
        .thenReturn(new StandardConnectionStateErrorPolicy());
    GovernorLeadershipListener listener = new GovernorLeadershipListener("10.0.0.1", () -> false,
        new MetricRegistry());

    for (ConnectionState state : new ConnectionState[] { ConnectionState.SUSPENDED,
        ConnectionState.LOST }) {
      try {
        listener.stateChanged(client, state);
        fail("leadership should be cancelled on connection state " + state);
      } catch (CancelLeadershipException expected) {
        // Curator translates this into an interrupt of the leadership thread.
      }
    }

    for (ConnectionState state : new ConnectionState[] { ConnectionState.CONNECTED,
        ConnectionState.RECONNECTED, ConnectionState.READ_ONLY }) {
      listener.stateChanged(client, state);
    }
  }

  /**
   * Curator cancels leadership by interrupting the thread inside {@code takeLeadership}, so
   * the park loop has to actually unwind on interrupt rather than swallow it and keep going.
   */
  @Test
  public void testTakeLeadershipReturnsWhenInterrupted() throws Exception {
    CuratorFramework client = mock(CuratorFramework.class);
    SetDataBuilder setDataBuilder = mock(SetDataBuilder.class);
    when(client.setData()).thenReturn(setDataBuilder);
    when(setDataBuilder.forPath(anyString(), any(byte[].class))).thenReturn(null);

    MetricRegistry registry = new MetricRegistry();
    GovernorLeadershipListener listener = new GovernorLeadershipListener("10.0.0.1", () -> false,
        registry);

    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread leadershipThread = new Thread(() -> {
      try {
        listener.takeLeadership(client);
      } catch (InterruptedException expected) {
        Thread.currentThread().interrupt();
      } catch (Throwable t) {
        failure.set(t);
      }
    }, "TestLeadership");
    leadershipThread.setDaemon(true);
    leadershipThread.start();

    assertTrue("leadership should have been acquired",
        waitForCounter(registry, MemqGovernor.METRIC_LEADERSHIP_ACQUIRED, 10_000));

    leadershipThread.interrupt();
    leadershipThread.join(10_000);

    assertFalse("takeLeadership must return once interrupted", leadershipThread.isAlive());
    if (failure.get() != null) {
      throw new AssertionError("leadership thread failed unexpectedly", failure.get());
    }
    assertEquals("relinquishing leadership should be recorded", 1,
        registry.counter(MemqGovernor.METRIC_LEADERSHIP_RELINQUISHED).getCount());
  }

  /**
   * Builds a governor wired to the given ZK ensemble. The balancer and local assigner stay
   * off so tests only exercise registration and leadership.
   */
  private static MemqGovernor newGovernor(String zkConnectionString,
                                          String brokerIp,
                                          Map<String, MetricRegistry> registryMap,
                                          boolean enableLeaderSelector) {
    ClusteringConfig clusteringConfig = new ClusteringConfig();
    clusteringConfig.setZookeeperConnectionString(zkConnectionString);
    clusteringConfig.setEnableBalancer(false);
    clusteringConfig.setEnableLeaderSelector(enableLeaderSelector);
    clusteringConfig.setEnableLocalAssigner(false);

    NettyServerConfig nettyServerConfig = mock(NettyServerConfig.class);
    when(nettyServerConfig.getPort()).thenReturn((short) 9092);

    MemqConfig config = mock(MemqConfig.class);
    when(config.getTopicConfig()).thenReturn(null);
    when(config.getClusteringConfig()).thenReturn(clusteringConfig);
    when(config.getNettyServerConfig()).thenReturn(nettyServerConfig);
    when(config.getBrokerType()).thenReturn(BrokerType.WRITE);

    EnvironmentProvider provider = mock(EnvironmentProvider.class);
    when(provider.getIP()).thenReturn(brokerIp);
    when(provider.getInstanceType()).thenReturn("test-instance");
    when(provider.getRack()).thenReturn("test-rack");

    MemqManager mgr = mock(MemqManager.class);
    when(mgr.getTopicAssignment()).thenReturn(Collections.emptySet());
    when(mgr.getRegistry()).thenReturn(registryMap);

    return new MemqGovernor(mgr, config, provider);
  }

  private static boolean waitFor(BooleanSupplier condition,
                                 long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean()) {
        return true;
      }
      Thread.sleep(200);
    }
    return condition.getAsBoolean();
  }

  private static boolean waitForCounter(MetricRegistry registry,
                                        String name,
                                        long timeoutMs) throws InterruptedException {
    return waitFor(() -> registry.counter(name).getCount() >= 1, timeoutMs);
  }

  private static boolean waitForPath(CuratorFramework client,
                                     String path,
                                     long timeoutMs) throws Exception {
    return waitFor(() -> {
      try {
        return client.checkExists().forPath(path) != null;
      } catch (Exception ignore) {
        // connection may be momentarily unavailable during the new-session handshake
        return false;
      }
    }, timeoutMs);
  }

}
