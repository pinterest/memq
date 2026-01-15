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

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Set;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.core.MemqManager;
import com.pinterest.memq.core.config.ClusteringConfig;
import com.pinterest.memq.core.config.LocalEnvironmentProvider;
import com.pinterest.memq.core.config.MemqConfig;

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

  @Test
  public void testGetAllBrokersWithNoZkClient() throws Exception {
    MemqConfig config = new MemqConfig();
    ClusteringConfig clusteringConfig = new ClusteringConfig();
    clusteringConfig.setEnableAssignments(false);
    config.setClusteringConfig(clusteringConfig);
    MemqManager mgr = new MemqManager(null, config, new HashMap<>());
    MemqGovernor governor = new MemqGovernor(mgr, config, new LocalEnvironmentProvider());

    Set<Broker> brokers = governor.getAllBrokers();
    assertEquals(0, brokers.size());
  }

}
