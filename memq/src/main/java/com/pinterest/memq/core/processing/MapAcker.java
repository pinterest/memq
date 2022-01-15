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
package com.pinterest.memq.core.processing;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class MapAcker implements Ackable {

  private static final Logger logger = Logger.getLogger(MapAcker.class.getName());
  private static final int DEFAULT_ACK_MAP_TIMEOUT_MINUTES = 10;
  private static final int DEFAULT_ACKER_SHUTDOWN_TIMEOUT = DEFAULT_ACK_MAP_TIMEOUT_MINUTES
      * 3600_000;
  private LoadingCache<Long, ProcessingStatus> ackMap;
  private String topicName;
  private MetricRegistry registry;
  private Counter missingAckEntryCounter;

  public MapAcker(String topicName, MetricRegistry registry) {
    this.topicName = topicName;
    this.registry = registry;
    initializeAckerMap();
  }

  public void initializeAckerMap() {
    missingAckEntryCounter = registry.counter("acker.missingAckEntryCounter");
    registry.register("acker.ackMapSize", new Gauge<Integer>() {

      @Override
      public Integer getValue() {
        return (int) ackMap.size();
      }
    });
    ackMap = CacheBuilder.newBuilder().maximumSize(10000)
        .expireAfterAccess(DEFAULT_ACK_MAP_TIMEOUT_MINUTES, TimeUnit.MINUTES)
        .build(new CacheLoader<Long, ProcessingStatus>() {

          @Override
          public ProcessingStatus load(Long key) throws Exception {
            missingAckEntryCounter.inc();
            return ProcessingStatus.FAILED;
          }
        });
    logger.info("Initialized acker map");
  }

  public LoadingCache<Long, ProcessingStatus> getAckMap() {
    return ackMap;
  }

  public void setAckMap(LoadingCache<Long, ProcessingStatus> ackMap) {
    this.ackMap = ackMap;
  }

  public void stop() {
    int timer = 0;
    while (ackMap.size() > 0) {
      try {
        ackMap.cleanUp();
        if (timer > DEFAULT_ACKER_SHUTDOWN_TIMEOUT) {
          break;
        }
        Thread.sleep(1000);
        timer += 1000;
      } catch (InterruptedException e) {
        logger.log(Level.SEVERE, "Stop thread interrupted for:" + topicName, e);
      }
    }
    logger.fine("Successfully cleaned up ack map");
  }

  public void markProcessed(long ackKey) {
    ackMap.put(ackKey, ProcessingStatus.PROCESSED);
  }

  public void markFailed(long ackKey) {
    ackMap.put(ackKey, ProcessingStatus.FAILED);
  }

  public void markPending(long ackKey) {
    ackMap.put(ackKey, ProcessingStatus.PENDING);
  }

  public ProcessingStatus getProcessingStatus(long ackKey) throws ExecutionException {
    return ackMap.get(ackKey);
  }

  @Override
  public String toString() {
    return "Acker [ackMap=" + ackMap.asMap() + "]";
  }

}
