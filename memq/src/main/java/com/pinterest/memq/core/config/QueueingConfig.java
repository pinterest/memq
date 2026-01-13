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
package com.pinterest.memq.core.config;

import com.pinterest.memq.core.rpc.queue.DeficitRoundRobinStrategy;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for fair-queueing mechanism in packet processing.
 */
public class QueueingConfig {

  private boolean enabled = false;
  
  /**
   * The fully qualified class name of the QueueingStrategy implementation to use.
   * Default is DeficitRoundRobinStrategy.
   */
  private String strategyClass = DeficitRoundRobinStrategy.class.getName();
  
  /**
   * Number of threads in the dequeue thread pool.
   * Each thread processes a subset of the topic queues.
   */
  private int dequeueThreadPoolSize = 4;
  
  /**
   * Maximum bytes of pending requests per topic queue.
   * If a queue exceeds this limit, requests will be rejected.
   * Default is 100MB per topic.
   */
  private long maxQueueBytesPerTopic = 100 * 1024 * 1024; // 100MB default
  
  /**
   * Strategy-specific configuration options.
   * Keys and values depend on the strategy implementation.
   * For example, DeficitRoundRobinStrategy uses "quantum" key.
   */
  private Map<String, Object> strategyConfig = new HashMap<>();

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getStrategyClass() {
    return strategyClass;
  }

  public void setStrategyClass(String strategyClass) {
    this.strategyClass = strategyClass;
  }

  public int getDequeueThreadPoolSize() {
    return dequeueThreadPoolSize;
  }

  public void setDequeueThreadPoolSize(int dequeueThreadPoolSize) {
    this.dequeueThreadPoolSize = dequeueThreadPoolSize;
  }

  public long getMaxQueueBytesPerTopic() {
    return maxQueueBytesPerTopic;
  }

  public void setMaxQueueBytesPerTopic(long maxQueueBytesPerTopic) {
    this.maxQueueBytesPerTopic = maxQueueBytesPerTopic;
  }

  public Map<String, Object> getStrategyConfig() {
    return strategyConfig;
  }

  public void setStrategyConfig(Map<String, Object> strategyConfig) {
    this.strategyConfig = strategyConfig;
  }
}
