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
package com.pinterest.memq.core;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.file.Paths;
import java.util.HashMap;

import org.junit.Test;

import com.pinterest.memq.commons.protocol.TopicAssignment;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.core.config.MemqConfig;

public class TestMemqManager {

  @Test
  public void testTopicCache() throws Exception {
    new File("target/testmgrcache").delete();
    MemqConfig configuration = new MemqConfig();
    configuration.setTopicCacheFile("target/testmgrcache");
    MemqManager mgr = new MemqManager(null, configuration, new HashMap<>());
    mgr.init();
    TopicConfig topicConfig = new TopicConfig(0, 1024, 100, "test", 10, 10, 2);
    TopicAssignment topicAssignment = new TopicAssignment(topicConfig, 10);
    topicAssignment.setStorageHandlerName("delayeddevnull");
    mgr.createTopicProcessor(topicAssignment);
    mgr.updateTopicCache();

    mgr = new MemqManager(null, configuration, new HashMap<>());
    mgr.init();
    assertEquals(1, mgr.getProcessorMap().size());
    assertEquals(1, mgr.getTopicAssignment().size());
  }

  @Test
  public void testTopicConfig() throws Exception {
    MemqConfig config = new MemqConfig();
    File tmpFile = File.createTempFile("test", "", Paths.get("/tmp").toFile());
    tmpFile.deleteOnExit();
    config.setTopicCacheFile(tmpFile.toString());
    MemqManager mgr = new MemqManager(null, new MemqConfig(), new HashMap<>());
    TopicConfig topicConfig = new TopicConfig("test", "delayeddevnull");
    TopicAssignment topicAssignment = new TopicAssignment(topicConfig, -1);
    mgr.createTopicProcessor(topicAssignment);

    long size = mgr.getTopicAssignment().iterator().next().getBatchSizeBytes();
    topicAssignment = new TopicAssignment(new TopicConfig("test", "delayeddevnull"), -1);
    topicAssignment.setBatchSizeBytes(size + 100);
    mgr.updateTopic(topicAssignment);
    assertEquals(size + 100, mgr.getTopicAssignment().iterator().next().getBatchSizeBytes());

  }

}
