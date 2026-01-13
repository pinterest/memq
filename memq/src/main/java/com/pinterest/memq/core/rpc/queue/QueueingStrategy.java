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
package com.pinterest.memq.core.rpc.queue;

import com.pinterest.memq.core.config.QueueingConfig;

import java.util.Set;

/**
 * Abstract base class for fair queueing strategies.
 * 
 * Implementations of this class provide different algorithms for
 * scheduling requests across multiple topic queues in a fair manner.
 */
public abstract class QueueingStrategy {

  protected QueueingConfig config;

  /**
   * Initialize the strategy with configuration.
   * Called once after instantiation.
   * 
   * @param config the queueing configuration
   */
  public void init(QueueingConfig config) {
    this.config = config;
  }

  /**
   * Enqueue a request into the appropriate topic queue.
   * 
   * @param request the queued request to enqueue
   * @return true if the request was successfully enqueued, false if rejected
   *         (e.g., due to queue being full)
   */
  public abstract boolean enqueue(QueuedRequest request);

  /**
   * Dequeue the next request to be processed according to the strategy's
   * fair scheduling algorithm.
   * 
   * This method should only process queues that are assigned to the calling thread.
   * 
   * @param assignedTopics the set of topic names that this thread is responsible for
   * @return the next request to process, or null if no requests are available
   */
  public abstract QueuedRequest dequeue(Set<String> assignedTopics);

  /**
   * Get the current pending bytes across all queues.
   * 
   * @return total pending bytes
   */
  public abstract long getPendingBytes();

  /**
   * Get the current pending bytes for a specific topic.
   * 
   * @param topicName the topic name
   * @return pending bytes for the topic
   */
  public abstract long getPendingBytes(String topicName);

  /**
   * Get all topic names that currently have queues (even if empty).
   * 
   * @return set of topic names
   */
  public abstract Set<String> getActiveTopics();

  /**
   * Called when the strategy is being shut down.
   * Implementations should clean up any resources.
   */
  public void shutdown() {
    // Default no-op implementation
  }
}
