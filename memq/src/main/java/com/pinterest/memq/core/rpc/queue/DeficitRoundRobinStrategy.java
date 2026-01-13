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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Deficit Round Robin (DRR) implementation of QueueingStrategy.
 * 
 * DRR is a fair scheduling algorithm that provides weighted fair queuing.
 * Each queue maintains a deficit counter. When a queue is visited:
 * - Its deficit counter is incremented by a quantum value
 * - Requests are dequeued as long as their size is less than or equal to the deficit counter
 * - The request's size is subtracted from the deficit counter
 * 
 * This ensures fair bandwidth allocation across all topics while being
 * work-conserving (idle queues don't waste capacity).
 * 
 * Strategy-specific configuration (in strategyConfig map):
 * - "quantum": int - bytes per round per queue (default: 64KB)
 */
public class DeficitRoundRobinStrategy extends QueueingStrategy {

  private static final Logger logger = Logger.getLogger(DeficitRoundRobinStrategy.class.getName());

  /**
   * Configuration key for the quantum value in strategyConfig.
   */
  public static final String CONFIG_KEY_QUANTUM = "quantum";

  /**
   * Default quantum size in bytes (64KB).
   * This determines how many bytes worth of requests can be processed
   * per round for each queue.
   */
  public static final int DEFAULT_QUANTUM = 65536;

  /**
   * Per-topic queue holding pending requests.
   */
  private final ConcurrentHashMap<String, ConcurrentLinkedQueue<QueuedRequest>> queues = new ConcurrentHashMap<>();

  /**
   * Current byte count for each topic queue.
   */
  private final ConcurrentHashMap<String, AtomicLong> queueBytes = new ConcurrentHashMap<>();

  /**
   * Deficit counter for each topic (used by DRR algorithm).
   */
  private final ConcurrentHashMap<String, AtomicInteger> deficitCounters = new ConcurrentHashMap<>();

  /**
   * Round-robin state for each dequeue thread - tracks current position in topic list.
   */
  private final ConcurrentHashMap<Long, Iterator<String>> threadIterators = new ConcurrentHashMap<>();

  /**
   * Cached list of topics for round-robin iteration.
   */
  private final ConcurrentHashMap<Long, List<String>> threadTopicLists = new ConcurrentHashMap<>();

  private int quantum;
  private long maxQueueBytes;

  @Override
  public void init(QueueingConfig config) {
    super.init(config);
    this.quantum = getQuantumFromConfig(config);
    this.maxQueueBytes = config.getMaxQueueBytesPerTopic();
  }

  /**
   * Extract the quantum value from strategy config, or use default.
   */
  private int getQuantumFromConfig(QueueingConfig config) {
    Object quantumObj = config.getStrategyConfig().get(CONFIG_KEY_QUANTUM);
    if (quantumObj != null) {
      if (quantumObj instanceof Number) {
        return ((Number) quantumObj).intValue();
      }
      if (quantumObj instanceof String) {
        try {
          return Integer.parseInt((String) quantumObj);
        } catch (NumberFormatException e) {
          logger.warning("Invalid quantum value in config: " + quantumObj + ", using default");
        }
      }
    }
    return DEFAULT_QUANTUM;
  }

  /**
   * Get the current quantum value being used.
   * 
   * @return the quantum in bytes
   */
  public int getQuantum() {
    return quantum;
  }

  @Override
  public boolean enqueue(QueuedRequest request) {
    String topicName = request.getTopicName();
    int requestSize = Math.max(1, request.getSize());
    
    // Get or create queue and byte counter
    ConcurrentLinkedQueue<QueuedRequest> queue = queues.computeIfAbsent(topicName,
        k -> new ConcurrentLinkedQueue<>());
    AtomicLong bytes = queueBytes.computeIfAbsent(topicName, k -> new AtomicLong(0));
    deficitCounters.computeIfAbsent(topicName, k -> new AtomicInteger(0));
    
    // Check if adding this request would exceed the byte limit
    // Use CAS loop for thread safety
    while (true) {
      long currentBytes = bytes.get();
      if (currentBytes + requestSize > maxQueueBytes) {
        logger.warning("Queue full for topic: " + topicName + 
            ", current bytes: " + currentBytes + ", request size: " + requestSize + 
            ", max: " + maxQueueBytes + ", rejecting request");
        return false;
      }
      if (bytes.compareAndSet(currentBytes, currentBytes + requestSize)) {
        queue.offer(request);
        return true;
      }
      // CAS failed, retry
    }
  }

  @Override
  public QueuedRequest dequeue(Set<String> assignedTopics) {
    if (assignedTopics.isEmpty()) {
      return null;
    }

    long threadId = Thread.currentThread().getId();
    
    // Get or create the topic list for this thread
    List<String> topicList = threadTopicLists.computeIfAbsent(threadId, k -> new ArrayList<>());
    
    // Refresh the topic list if needed (topics may have been added/removed)
    refreshTopicListIfNeeded(topicList, assignedTopics);
    
    if (topicList.isEmpty()) {
      return null;
    }

    // Get or create iterator for this thread
    Iterator<String> iterator = threadIterators.get(threadId);
    if (iterator == null || !iterator.hasNext()) {
      iterator = topicList.iterator();
      threadIterators.put(threadId, iterator);
    }

    // Try each topic in round-robin fashion
    int topicsChecked = 0;
    int totalTopics = topicList.size();
    
    while (topicsChecked < totalTopics) {
      if (!iterator.hasNext()) {
        iterator = topicList.iterator();
        threadIterators.put(threadId, iterator);
      }
      
      String topicName = iterator.next();
      topicsChecked++;
      
      ConcurrentLinkedQueue<QueuedRequest> queue = queues.get(topicName);
      if (queue == null || queue.isEmpty()) {
        // Reset deficit when queue is empty (work-conserving)
        AtomicInteger deficit = deficitCounters.get(topicName);
        if (deficit != null) {
          deficit.set(0);
        }
        continue;
      }

      AtomicInteger deficit = deficitCounters.get(topicName);
      if (deficit == null) {
        continue;
      }

      // Peek at the next request
      QueuedRequest request = queue.peek();
      if (request == null) {
        continue;
      }

      int requestSize = Math.max(1, request.getSize()); // Minimum size of 1 to prevent starvation
      int currentDeficit = deficit.get();

      // Add quantum to deficit
      if (currentDeficit < requestSize) {
        currentDeficit = deficit.addAndGet(quantum);
      }

      // Check if we can service this request
      if (currentDeficit >= requestSize) {
        // Try to poll the request
        request = queue.poll();
        if (request != null) {
          // Subtract request size from deficit and update byte counter
          deficit.addAndGet(-requestSize);
          AtomicLong bytes = queueBytes.get(topicName);
          if (bytes != null) {
            bytes.addAndGet(-requestSize);
          }
          return request;
        }
      }
    }

    return null;
  }

  /**
   * Refresh the topic list if the assigned topics have changed.
   */
  private void refreshTopicListIfNeeded(List<String> topicList, Set<String> assignedTopics) {
    // Check if we need to refresh
    Set<String> currentTopics = new HashSet<>(topicList);
    Set<String> activeAssigned = new HashSet<>();
    
    for (String topic : assignedTopics) {
      if (queues.containsKey(topic)) {
        activeAssigned.add(topic);
      }
    }
    
    if (!currentTopics.equals(activeAssigned)) {
      topicList.clear();
      topicList.addAll(activeAssigned);
      // Reset iterator since list changed
      threadIterators.remove(Thread.currentThread().getId());
    }
  }

  @Override
  public long getPendingBytes() {
    long total = 0;
    for (AtomicLong bytes : queueBytes.values()) {
      total += bytes.get();
    }
    return total;
  }

  @Override
  public long getPendingBytes(String topicName) {
    AtomicLong bytes = queueBytes.get(topicName);
    return bytes != null ? bytes.get() : 0;
  }

  /**
   * Get the current number of pending requests across all queues.
   * 
   * @return total number of pending requests
   */
  public int getPendingCount() {
    int total = 0;
    for (ConcurrentLinkedQueue<QueuedRequest> queue : queues.values()) {
      total += queue.size();
    }
    return total;
  }

  /**
   * Get the current number of pending requests for a specific topic.
   * 
   * @param topicName the topic name
   * @return number of pending requests for the topic
   */
  public int getPendingCount(String topicName) {
    ConcurrentLinkedQueue<QueuedRequest> queue = queues.get(topicName);
    return queue != null ? queue.size() : 0;
  }

  @Override
  public Set<String> getActiveTopics() {
    return new HashSet<>(queues.keySet());
  }

  @Override
  public void shutdown() {
    queues.clear();
    queueBytes.clear();
    deficitCounters.clear();
    threadIterators.clear();
    threadTopicLists.clear();
  }
}
