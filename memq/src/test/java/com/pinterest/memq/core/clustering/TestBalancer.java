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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.junit.Test;

import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.Broker.BrokerType;
import com.pinterest.memq.core.config.ClusteringConfig;
import com.pinterest.memq.core.config.MemqConfig;

public class TestBalancer {

  /**
   * A governor that lost its ZK session mid-balance must not publish the plan it computed
   * while it still believed it was the leader, otherwise it fights the new governor over
   * topic assignments.
   */
  @Test
  public void testAssignmentsAreNotPublishedAfterLosingLeadership() throws Exception {
    CuratorFramework client = mock(CuratorFramework.class);
    LeaderSelector leaderSelector = mock(LeaderSelector.class);
    when(leaderSelector.hasLeadership()).thenReturn(false);

    Balancer balancer = new Balancer(newConfig(), mock(MemqGovernor.class), client, leaderSelector);
    balancer.publishAssignments(brokers("10.0.0.1", "10.0.0.2"));

    verify(client, never()).setData();
  }

  @Test
  public void testAssignmentsArePublishedWhileLeader() throws Exception {
    CuratorFramework client = mock(CuratorFramework.class);
    SetDataBuilder setDataBuilder = mock(SetDataBuilder.class);
    when(client.setData()).thenReturn(setDataBuilder);
    when(setDataBuilder.forPath(anyString(), any(byte[].class))).thenReturn(null);
    LeaderSelector leaderSelector = mock(LeaderSelector.class);
    when(leaderSelector.hasLeadership()).thenReturn(true);

    Balancer balancer = new Balancer(newConfig(), mock(MemqGovernor.class), client, leaderSelector);
    balancer.publishAssignments(brokers("10.0.0.1", "10.0.0.2"));

    verify(setDataBuilder).forPath(eq(MemqGovernor.ZNODE_BROKERS_BASE + "10.0.0.1"),
        any(byte[].class));
    verify(setDataBuilder).forPath(eq(MemqGovernor.ZNODE_BROKERS_BASE + "10.0.0.2"),
        any(byte[].class));
    verify(setDataBuilder, times(2)).forPath(anyString(), any(byte[].class));
  }

  private static MemqConfig newConfig() {
    MemqConfig config = new MemqConfig();
    ClusteringConfig clusteringConfig = new ClusteringConfig();
    clusteringConfig.setEnableExpiration(false);
    config.setClusteringConfig(clusteringConfig);
    return config;
  }

  private static Set<Broker> brokers(String... ips) {
    Set<Broker> brokers = new HashSet<>();
    for (String ip : ips) {
      brokers.add(new Broker(ip, (short) 9092, "test-instance", "test-rack", BrokerType.WRITE,
          Collections.emptySet()));
    }
    return brokers;
  }
}
