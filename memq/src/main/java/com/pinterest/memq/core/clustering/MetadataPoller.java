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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;

import com.google.gson.Gson;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.protocol.TopicMetadata;

public final class MetadataPoller implements Runnable {

  private static final Logger logger = Logger.getLogger(MetadataPoller.class.getCanonicalName());
  private static final Gson GSON = new Gson();
  private final CuratorFramework client;
  private Map<String, TopicMetadata> topicBrokerMap;

  public MetadataPoller(CuratorFramework client, Map<String, TopicMetadata> topicBrokerMap) {
    this.client = client;
    this.topicBrokerMap = topicBrokerMap;
  }

  @Override
  public void run() {
    while (true) {
      try {
        Map<String, TopicMetadata> topicBrokerMap = new HashMap<>();
        // poll topic assignment data
        List<String> brokers = client.getChildren().forPath("/brokers");
        for (String brokerId : brokers) {
          String brokerDataStr = new String(client.getData().forPath("/brokers/" + brokerId));
          Broker broker = GSON.fromJson(brokerDataStr, Broker.class);
          for (TopicConfig topicConfig : broker.getAssignedTopics()) {
            TopicMetadata md = topicBrokerMap.get(topicConfig.getTopic());
            if (md == null) {
              md = new TopicMetadata(topicConfig.getTopic(), topicConfig.getStorageHandlerName(),
                  topicConfig.getStorageHandlerConfig());
              topicBrokerMap.put(topicConfig.getTopic(), md);
            }
            // based on type of broker add the broker to the appropriate set
            switch (broker.getBrokerType()) {
            case READ:
              md.getReadBrokers().add(broker);
              break;
            case WRITE:
              md.getWriteBrokers().add(broker);
              break;
            default:
              md.getReadBrokers().add(broker);
              md.getWriteBrokers().add(broker);
            }
          }
        }
        this.topicBrokerMap.putAll(topicBrokerMap);
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        logger.log(Level.SEVERE, "Topic metadata poller thread interrupted, exiting", e);
        break;
      } catch (Exception e) {
        logger.log(Level.SEVERE, "Topic metadata poller thread experienced exception", e);
      }
    }
  }
}