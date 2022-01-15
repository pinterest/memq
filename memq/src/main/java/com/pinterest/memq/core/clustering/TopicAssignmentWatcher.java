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

import java.util.Set;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.TopicAssignment;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.core.MemqManager;

public final class TopicAssignmentWatcher implements Runnable {
  /**
   * 
   */
  private static final Logger logger = Logger
      .getLogger(TopicAssignmentWatcher.class.getCanonicalName());
  private final Gson GSON = new Gson();
  private final String path;
  private final CuratorFramework client;
  private MemqManager mgr;

  public TopicAssignmentWatcher(MemqManager mgr,
                                String path,
                                Broker broker,
                                CuratorFramework client) {
    this.mgr = mgr;
    this.path = path;
    this.client = client;
  }

  @Override
  public void run() {
    while (true) {
      try {
        String brokerDataStr = new String(client.getData().forPath(path));
        logger.fine("Broker assignment:" + brokerDataStr);
        Broker tmpBroker = GSON.fromJson(brokerDataStr, Broker.class);
        Set<TopicAssignment> newTopics = null;
        boolean updated = false;
        if (tmpBroker.getAssignedTopics().size() != mgr.getProcessorMap().size()
            || !tmpBroker.getAssignedTopics().equals(mgr.getTopicAssignment())) {
          updated = true; // update topic cache file
          // topic assignment changed update topic manager
          // delete existing topics
          Set<TopicAssignment> decommissionedTopics = Sets.difference(mgr.getTopicAssignment(),
              tmpBroker.getAssignedTopics());
          logger.info("Received signal to delete the following Topics from this node:"
              + MemqGovernor.convertTopicAssignmentsSetToList(decommissionedTopics));
          for (TopicConfig topicConfig : decommissionedTopics) {
            mgr.deleteTopicProcessor(topicConfig.getTopic());
          }

          // create new topics
          newTopics = Sets.difference(tmpBroker.getAssignedTopics(),
              mgr.getTopicAssignment());
          logger.info("Received signal to create the following Topics on this node:"
              + MemqGovernor.convertTopicAssignmentsSetToList(newTopics));
          for (TopicAssignment topicConfig : newTopics) {
            mgr.createTopicProcessor(topicConfig);
          }
        }
        // updated existing topics
        for (TopicAssignment topic : tmpBroker.getAssignedTopics()) {
          if (newTopics == null || !newTopics.contains(topic)) {
            updated |= mgr.updateTopic(topic);
          }
        }
        if (updated) {
          mgr.updateTopicCache();
        }
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
}