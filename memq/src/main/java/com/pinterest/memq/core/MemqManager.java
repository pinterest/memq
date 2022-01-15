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

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.google.gson.Gson;
import com.pinterest.memq.commons.mon.OpenTSDBClient;
import com.pinterest.memq.commons.mon.OpenTSDBReporter;
import com.pinterest.memq.commons.protocol.TopicAssignment;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.commons.storage.StorageHandlerTable;
import com.pinterest.memq.core.config.MemqConfig;
import com.pinterest.memq.core.processing.TopicProcessor;
import com.pinterest.memq.core.processing.TopicProcessorState;
import com.pinterest.memq.core.processing.bucketing.BucketingTopicProcessor;
import com.pinterest.memq.core.utils.DaemonThreadFactory;
import com.pinterest.memq.core.utils.MiscUtils;

import io.dropwizard.lifecycle.Managed;

public class MemqManager implements Managed {

  private static final Logger logger = Logger.getLogger(MemqManager.class.getName());
  private static final Gson gson = new Gson();
  private Map<String, TopicProcessor> processorMap = new ConcurrentHashMap<>();
  private Map<String, TopicAssignment> topicMap = new ConcurrentHashMap<>();
  private MemqConfig configuration;
  private ScheduledExecutorService timerService;
  private ScheduledExecutorService cleanupService;
  private Map<String, MetricRegistry> metricsRegistryMap;
  private AtomicBoolean disabled;
  private String topicCacheFile;
  private OpenTSDBClient client;

  public MemqManager(OpenTSDBClient client,
                     MemqConfig configuration,
                     Map<String, MetricRegistry> metricsRegistryMap) throws UnknownHostException {
    this.configuration = configuration;
    this.topicCacheFile = configuration.getTopicCacheFile();
    this.metricsRegistryMap = metricsRegistryMap;
    timerService = Executors.newScheduledThreadPool(1, DaemonThreadFactory.INSTANCE);
    cleanupService = Executors.newScheduledThreadPool(configuration.getCleanerThreadCount(),
        DaemonThreadFactory.INSTANCE);
    this.disabled = new AtomicBoolean();
    this.client = client;
  }

  public void init() throws Exception {
    loadStorageHandlers(configuration);
    File file = new File(topicCacheFile);
    if (file.exists()) {
      byte[] bytes = Files.readAllBytes(file.toPath());
      TopicAssignment[] topics = gson.fromJson(new String(bytes), TopicAssignment[].class);
      topicMap = new ConcurrentHashMap<>();
      for (TopicAssignment topicConfig : topics) {
        topicMap.put(topicConfig.getTopic(), topicConfig);
      }
    }
    if (configuration.getTopicConfig() != null) {
      for (TopicConfig topicConfig : configuration.getTopicConfig()) {
        topicMap.put(topicConfig.getTopic(), new TopicAssignment(topicConfig, -1));
      }
    }
    for (Entry<String, TopicAssignment> entry : topicMap.entrySet()) {
      createTopicProcessor(entry.getValue());
    }
  }

  public void createTopicProcessor(TopicAssignment topicConfig) throws BadRequestException,
                                                            UnknownHostException {
    if (processorMap.containsKey(topicConfig.getTopic())) {
      return;
    }
    if (topicConfig.getStorageHandlerName() == null) {
      throw new BadRequestException("Missing handler " + topicConfig.toString());
    }
    MetricRegistry registry = new MetricRegistry();
    metricsRegistryMap.put(topicConfig.getTopic(), registry);

    ScheduledReporter reporter = null;
    if (client != null) {
      String localHostname = MiscUtils.getHostname();
      reporter = OpenTSDBReporter.createReporterWithTags("", registry, topicConfig.getTopic(),
          (String name, com.codahale.metrics.Metric metric) -> true, TimeUnit.SECONDS,
          TimeUnit.SECONDS, client, localHostname,
          Collections.singletonMap("topic", topicConfig.getTopic()));
      reporter.start(configuration.getOpenTsdbConfig().getFrequencyInSeconds(), TimeUnit.SECONDS);
    }

    StorageHandler storageHandler;
    try {
      storageHandler = StorageHandlerTable.getClass(topicConfig.getStorageHandlerName()).newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new BadRequestException("Invalid output class", e);
    }
    try {
      storageHandler.initWriter(topicConfig.getStorageHandlerConfig(), topicConfig.getTopic(), registry);
    } catch (Exception e) {
      throw new InternalServerErrorException(e);
    }

    if (topicConfig.getBufferSize() == 0) {
      topicConfig.setBufferSize(configuration.getDefaultBufferSize());
    }

    if (topicConfig.getRingBufferSize() == 0) {
      topicConfig.setRingBufferSize(configuration.getDefaultRingBufferSize());
    }
    TopicProcessor tp = new BucketingTopicProcessor(registry, topicConfig, storageHandler, timerService, reporter);

    processorMap.put(topicConfig.getTopic(), tp);
    topicMap.put(topicConfig.getTopic(), topicConfig);
    logger.info("Configured and started TopicProcessor for:" + topicConfig.getTopic());
  }

  public void updateTopicCache() {
    String json = gson.toJson(topicMap.values());
    try {
      Files.write(new File(topicCacheFile).toPath(), json.getBytes());
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Failed to update topic cache", e);
    }
  }

  public TopicProcessorState getProcessorState(String topic) {
    TopicProcessor topicProcessor = checkAndGetTopicProcessor(topic);
    return topicProcessor.getState();
  }

  /**
   * 
   * @param topic
   * @return future task to track deletion
   */
  public Future<?> deleteTopicProcessor(String topic) {
    final TopicProcessor topicProcessor = checkAndGetTopicProcessor(topic);
    return cleanupService.submit(() -> {
      try {
        topicProcessor.stopAndAwait();
      } catch (InterruptedException e) {
        logger.severe("Termination failed");
      }
      if (topicProcessor.getState() != TopicProcessorState.STOPPED) {
        logger.severe("Topic processor not stopped for topic:" + topic);
      }
      processorMap.remove(topic);
      topicMap.remove(topic);
    });
  }

  private TopicProcessor checkAndGetTopicProcessor(String topic) {
    final TopicProcessor topicProcessor = processorMap.get(topic);
    if (topicProcessor == null) {
      throw new NotFoundException(
          "Topic processor for:" + topic + " doesn't exist on this instance");
    }
    return topicProcessor;
  }
  
  public MemqConfig getConfiguration() {
    return configuration;
  }

  public Map<String, TopicProcessor> getProcessorMap() {
    return processorMap;
  }

  public Set<TopicAssignment> getTopicAssignment() {
    return new HashSet<>(topicMap.values());
  }

  public Map<String, MetricRegistry> getRegistry() {
    return metricsRegistryMap;
  }

  @Override
  public void start() throws Exception {
    logger.info("Memq manager started");
  }

  @Override
  public void stop() throws Exception {
    try {
      for (Iterator<Entry<String, TopicProcessor>> iterator = processorMap.entrySet()
          .iterator(); iterator.hasNext();) {
        Entry<String, TopicProcessor> entry = iterator.next();
        iterator.remove();
        entry.getValue().stopNow();
      }
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Processor stop failed", e);
    }
    timerService.shutdownNow();
    cleanupService.shutdownNow();
    logger.info("Memq Manager stopped");
  }

  public boolean isRunning() {
    return !timerService.isShutdown() && !cleanupService.isShutdown();
  }

  public boolean isDisabled() {
    return disabled.get();
  }

  public void setDisabled(boolean disabled) {
    this.disabled.set(disabled);
  }

  public void loadStorageHandlers(MemqConfig configuration) {
    String[] storageHandlerPackageNames = configuration.getStorageHandlerPackageNames();
    if (storageHandlerPackageNames != null && storageHandlerPackageNames.length > 0) {
      for (String packageName : storageHandlerPackageNames) {
        StorageHandlerTable.findAndRegisterOutputHandlers(packageName);
      }
    }
  }

  public boolean updateTopic(TopicAssignment topicConfig) {
    String topic = topicConfig.getTopic();
    TopicConfig existingConfig = topicMap.get(topic);
    // return since topic doesn't exist
    if (existingConfig == null) {
      return false;
    }

    // return since topic isn't updated
    if (!existingConfig.isDifferentFrom(topicConfig)) {
      return false;
    }
    topicMap.put(topic, topicConfig);
    TopicProcessor processor = processorMap.get(topic);
    if (processor == null) {
      // TODO: throw exception since processor should exist
      return false;
    }
    logger.info("Topic config of topic " + topic + " changed, original: " + existingConfig + ", new: " + topicConfig);
    processor.reconfigure(topicConfig);
    return true;
  }
}