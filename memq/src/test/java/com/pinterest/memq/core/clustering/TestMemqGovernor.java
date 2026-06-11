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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
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
      ClusteringConfig clusteringConfig = new ClusteringConfig();
      clusteringConfig.setZookeeperConnectionString(testingServer.getConnectString());
      // Keep the test focused on registration; disable the extra background machinery.
      clusteringConfig.setEnableBalancer(false);
      clusteringConfig.setEnableLeaderSelector(false);
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

      Map<String, MetricRegistry> registryMap = new HashMap<>();
      MemqManager mgr = mock(MemqManager.class);
      when(mgr.getTopicAssignment()).thenReturn(Collections.emptySet());
      when(mgr.getRegistry()).thenReturn(registryMap);

      MemqGovernor governor = new MemqGovernor(mgr, config, provider);
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

  private static boolean waitForCounter(MetricRegistry registry,
                                        String name,
                                        long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (registry.counter(name).getCount() >= 1) {
        return true;
      }
      Thread.sleep(200);
    }
    return false;
  }

  private static boolean waitForPath(CuratorFramework client,
                                     String path,
                                     long timeoutMs) throws Exception {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      try {
        if (client.checkExists().forPath(path) != null) {
          return true;
        }
      } catch (Exception ignore) {
        // connection may be momentarily unavailable during the new-session handshake
      }
      Thread.sleep(200);
    }
    return false;
  }

}
