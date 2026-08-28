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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.Broker.BrokerType;
import com.pinterest.memq.commons.protocol.TopicAssignment;
import com.pinterest.memq.core.config.ClusteringConfig;
import com.pinterest.memq.core.config.MemqConfig;

public class TestBalancer {

  private static final Gson GSON = new Gson();
  private static final String BROKER_IP = "10.0.0.7";
  private static final String BROKER_PATH = MemqGovernor.ZNODE_BROKERS_BASE + BROKER_IP;

  private TestingServer testingServer;
  private CuratorFramework client;
  private LeaderSelector leaderSelector;
  private MemqGovernor governor;
  private Balancer balancer;

  @Before
  public void setup() throws Exception {
    testingServer = new TestingServer();
    client = CuratorFrameworkFactory.newClient(testingServer.getConnectString(),
        new ExponentialBackoffRetry(500, 3));
    client.start();
    client.blockUntilConnected();

    // /governor is the fencing anchor; its version is the leadership epoch.
    client.create().withMode(CreateMode.PERSISTENT).forPath(MemqGovernor.ZNODE_GOVERNOR,
        "governorA".getBytes());
    client.create().withMode(CreateMode.PERSISTENT).forPath(MemqGovernor.ZNODE_BROKERS);
    client.create().withMode(CreateMode.PERSISTENT).forPath(BROKER_PATH,
        GSON.toJson(broker("rack-old")).getBytes());

    ClusteringConfig clusteringConfig = new ClusteringConfig();
    clusteringConfig.setEnableExpiration(false);
    MemqConfig config = mock(MemqConfig.class);
    when(config.getClusteringConfig()).thenReturn(clusteringConfig);

    leaderSelector = mock(LeaderSelector.class);
    when(leaderSelector.hasLeadership()).thenReturn(true);

    governor = mock(MemqGovernor.class);
    balancer = new Balancer(config, governor, client, leaderSelector);
  }

  @After
  public void tearDown() throws Exception {
    if (client != null) {
      client.close();
    }
    if (testingServer != null) {
      testingServer.close();
    }
  }

  private static Broker broker(String locality) {
    return new Broker(BROKER_IP, (short) 9092, "test-instance", locality, BrokerType.WRITE,
        new HashSet<TopicAssignment>());
  }

  private String currentBrokerLocality() throws Exception {
    return GSON.fromJson(new String(client.getData().forPath(BROKER_PATH)), Broker.class)
        .getLocality();
  }

  private int governorEpoch() throws Exception {
    return client.checkExists().forPath(MemqGovernor.ZNODE_GOVERNOR).getVersion();
  }

  /**
   * Happy path: the epoch is unchanged between plan computation and publish, so the
   * assignments are written.
   */
  @Test
  public void testPublishSucceedsWhenEpochUnchanged() throws Exception {
    int fencingVersion = governorEpoch();
    Set<Broker> plan = Collections.singleton(broker("rack-new"));

    balancer.publishAssignments(plan, fencingVersion);

    assertEquals("assignments should be published when still in the same leadership term",
        "rack-new", currentBrokerLocality());
  }

  /**
   * Stale-term path: a new leadership term rewrites /governor (bumping its version) after
   * the plan was computed. The fenced transaction must be rejected atomically so the stale
   * plan is never applied, even though hasLeadership() still reports true (mimicking a
   * governor that lost and, via autoRequeue, regained leadership).
   */
  @Test
  public void testPublishRejectedWhenEpochAdvanced() throws Exception {
    int staleVersion = governorEpoch();

    // A successor governor (or ourselves after re-acquiring) starts a new term.
    client.setData().forPath(MemqGovernor.ZNODE_GOVERNOR, "governorB".getBytes());

    Set<Broker> stalePlan = Collections.singleton(broker("rack-new"));
    balancer.publishAssignments(stalePlan, staleVersion);

    assertEquals("stale-term assignments must be discarded, leaving the znode untouched",
        "rack-old", currentBrokerLocality());
  }

  /**
   * Reproduces the residual race the pinned epoch closes: a successor has already advanced
   * /governor before this balancer publishes, so a live read would capture the successor's
   * (higher) version and the fence would trivially pass. Because the balancer instead uses
   * the governor's pinned own-term epoch (which stays behind the successor's write), the
   * fenced transaction is rejected and the successor's assignments are left untouched.
   */
  @Test
  public void testBalancerUsesGovernorPinnedEpochNotLiveVersion() throws Exception {
    // Our own leadership term wrote /governor at this version.
    int ourTermEpoch = governorEpoch();
    when(governor.getLeadershipEpoch()).thenReturn(ourTermEpoch);

    // A successor takes over and advances /governor beyond our pinned epoch.
    client.setData().forPath(MemqGovernor.ZNODE_GOVERNOR, "governorB".getBytes());

    // The balancer fences with the governor's pinned epoch, not the live (advanced) version.
    Set<Broker> stalePlan = Collections.singleton(broker("rack-new"));
    balancer.publishAssignments(stalePlan, governor.getLeadershipEpoch());

    assertEquals("using the pinned own-term epoch must reject the stale publish",
        "rack-old", currentBrokerLocality());
  }

  /**
   * Reproduces the "balancer captures -1" scenario: Curator has flipped hasLeadership() to
   * true, but takeLeadership() has not yet written /governor, so the governor's pinned epoch
   * is still its initial/cleared value of -1. If a -1 fencing token were submitted to
   * ZooKeeper it would mean "match any version" and defeat the fence entirely. The guard must
   * instead discard the publish outright so no assignments are written, even though
   * hasLeadership() reports true.
   */
  @Test
  public void testPublishRejectedWhenEpochNotYetPinned() throws Exception {
    when(governor.getLeadershipEpoch()).thenReturn(-1);
    int liveVersionBefore = governorEpoch();

    Set<Broker> plan = Collections.singleton(broker("rack-new"));
    balancer.publishAssignments(plan, governor.getLeadershipEpoch());

    assertEquals("an unpinned (-1) epoch must discard the publish, leaving the znode untouched",
        "rack-old", currentBrokerLocality());
    assertEquals("no fenced transaction should run, so /governor must be unchanged",
        liveVersionBefore, governorEpoch());
  }
}
