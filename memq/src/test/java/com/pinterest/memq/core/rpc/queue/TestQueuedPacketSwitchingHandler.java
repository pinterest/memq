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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.WriteRequestPacket;
import com.pinterest.memq.core.config.QueueingConfig;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * Tests for QueuedPacketSwitchingHandler and related queueing components.
 * 
 * These tests focus on the core queueing functionality:
 * - Enqueueing and dequeueing
 * - Fairness across topic queues
 * - Queue full scenarios
 */
public class TestQueuedPacketSwitchingHandler {

  private QueueingConfig config;
  private DeficitRoundRobinStrategy strategy;

  @Before
  public void setUp() {
    config = new QueueingConfig();
    config.setEnabled(true);
    config.setDequeueThreadPoolSize(4);
    config.setMaxQueueBytesPerTopic(1024 * 1024); // 1MB per topic
    config.getStrategyConfig().put(DeficitRoundRobinStrategy.CONFIG_KEY_QUANTUM, 1024); // 1KB quantum
    
    strategy = new DeficitRoundRobinStrategy();
    strategy.init(config);
  }

  @After
  public void tearDown() {
    if (strategy != null) {
      strategy.shutdown();
    }
  }

  /**
   * Helper to create a QueuedRequest with a given topic name and data size.
   */
  private QueuedRequest createRequest(String topicName, int dataSize) {
    WriteRequestPacket writePacket = new WriteRequestPacket();
    writePacket.setTopicName(topicName);
    writePacket.setData(new byte[dataSize]);
    
    RequestPacket requestPacket = new RequestPacket();
    // ChannelHandlerContext is null for these unit tests - we only test queueing logic
    return new QueuedRequest(null, requestPacket, writePacket);
  }

  // ===========================================
  // Basic Enqueue/Dequeue Tests
  // ===========================================

  @Test
  public void testBasicEnqueueDequeue() {
    String topic = "test-topic";
    QueuedRequest request = createRequest(topic, 100);
    
    // Enqueue should succeed
    assertTrue(strategy.enqueue(request));
    assertEquals(1, strategy.getPendingCount());
    assertEquals(1, strategy.getPendingCount(topic));
    
    // Dequeue should return the request
    Set<String> topics = new HashSet<>();
    topics.add(topic);
    QueuedRequest dequeued = strategy.dequeue(topics);
    
    assertNotNull(dequeued);
    assertEquals(topic, dequeued.getTopicName());
    assertEquals(0, strategy.getPendingCount());
  }

  @Test
  public void testEnqueueMultipleTopics() {
    String topic1 = "topic-1";
    String topic2 = "topic-2";
    String topic3 = "topic-3";
    
    // Enqueue requests for different topics
    assertTrue(strategy.enqueue(createRequest(topic1, 100)));
    assertTrue(strategy.enqueue(createRequest(topic2, 100)));
    assertTrue(strategy.enqueue(createRequest(topic3, 100)));
    
    assertEquals(3, strategy.getPendingCount());
    assertEquals(1, strategy.getPendingCount(topic1));
    assertEquals(1, strategy.getPendingCount(topic2));
    assertEquals(1, strategy.getPendingCount(topic3));
    
    // Active topics should contain all three
    Set<String> activeTopics = strategy.getActiveTopics();
    assertEquals(3, activeTopics.size());
    assertTrue(activeTopics.contains(topic1));
    assertTrue(activeTopics.contains(topic2));
    assertTrue(activeTopics.contains(topic3));
  }

  @Test
  public void testDequeueEmptyQueue() {
    Set<String> topics = new HashSet<>();
    topics.add("nonexistent-topic");
    
    QueuedRequest dequeued = strategy.dequeue(topics);
    assertNull(dequeued);
  }

  @Test
  public void testDequeueWithNoAssignedTopics() {
    String topic = "test-topic";
    assertTrue(strategy.enqueue(createRequest(topic, 100)));
    
    // Dequeue with empty assigned topics should return null
    Set<String> emptyTopics = new HashSet<>();
    QueuedRequest dequeued = strategy.dequeue(emptyTopics);
    assertNull(dequeued);
    
    // Request should still be in queue
    assertEquals(1, strategy.getPendingCount());
  }

  @Test
  public void testDequeueOnlyAssignedTopics() {
    String topic1 = "topic-1";
    String topic2 = "topic-2";
    
    assertTrue(strategy.enqueue(createRequest(topic1, 100)));
    assertTrue(strategy.enqueue(createRequest(topic2, 100)));
    
    // Only assign topic1
    Set<String> assignedTopics = new HashSet<>();
    assignedTopics.add(topic1);
    
    QueuedRequest dequeued = strategy.dequeue(assignedTopics);
    assertNotNull(dequeued);
    assertEquals(topic1, dequeued.getTopicName());
    
    // topic2 should still be pending
    assertEquals(1, strategy.getPendingCount());
    assertEquals(1, strategy.getPendingCount(topic2));
  }

  // ===========================================
  // Queue Full Scenario Tests
  // ===========================================

  @Test
  public void testQueueFull() {
    // Set a very small queue size in bytes (300 bytes = 3 requests of 100 bytes)
    QueueingConfig smallConfig = new QueueingConfig();
    smallConfig.setMaxQueueBytesPerTopic(300);
    smallConfig.getStrategyConfig().put(DeficitRoundRobinStrategy.CONFIG_KEY_QUANTUM, 1024);
    
    DeficitRoundRobinStrategy smallStrategy = new DeficitRoundRobinStrategy();
    smallStrategy.init(smallConfig);
    
    try {
      String topic = "test-topic";
      
      // Fill the queue (3 x 100 bytes = 300 bytes)
      assertTrue(smallStrategy.enqueue(createRequest(topic, 100)));
      assertTrue(smallStrategy.enqueue(createRequest(topic, 100)));
      assertTrue(smallStrategy.enqueue(createRequest(topic, 100)));
      
      // Fourth request should be rejected (would exceed 300 bytes)
      assertFalse(smallStrategy.enqueue(createRequest(topic, 100)));
      
      assertEquals(3, smallStrategy.getPendingCount());
      assertEquals(300, smallStrategy.getPendingBytes(topic));
    } finally {
      smallStrategy.shutdown();
    }
  }

  @Test
  public void testQueueFullPerTopic() {
    // Set a very small queue size in bytes (200 bytes per topic = 2 requests of 100 bytes)
    QueueingConfig smallConfig = new QueueingConfig();
    smallConfig.setMaxQueueBytesPerTopic(200);
    smallConfig.getStrategyConfig().put(DeficitRoundRobinStrategy.CONFIG_KEY_QUANTUM, 1024);
    
    DeficitRoundRobinStrategy smallStrategy = new DeficitRoundRobinStrategy();
    smallStrategy.init(smallConfig);
    
    try {
      String topic1 = "topic-1";
      String topic2 = "topic-2";
      
      // Fill topic1 queue (2 x 100 bytes = 200 bytes)
      assertTrue(smallStrategy.enqueue(createRequest(topic1, 100)));
      assertTrue(smallStrategy.enqueue(createRequest(topic1, 100)));
      assertFalse(smallStrategy.enqueue(createRequest(topic1, 100))); // rejected
      
      // topic2 should still accept (independent byte limit)
      assertTrue(smallStrategy.enqueue(createRequest(topic2, 100)));
      assertTrue(smallStrategy.enqueue(createRequest(topic2, 100)));
      assertFalse(smallStrategy.enqueue(createRequest(topic2, 100))); // rejected
      
      assertEquals(4, smallStrategy.getPendingCount());
      assertEquals(2, smallStrategy.getPendingCount(topic1));
      assertEquals(2, smallStrategy.getPendingCount(topic2));
      assertEquals(200, smallStrategy.getPendingBytes(topic1));
      assertEquals(200, smallStrategy.getPendingBytes(topic2));
    } finally {
      smallStrategy.shutdown();
    }
  }

  @Test
  public void testQueueFullBytesBased() {
    // Test that queue limit is enforced by bytes, not count
    QueueingConfig byteConfig = new QueueingConfig();
    byteConfig.setMaxQueueBytesPerTopic(500);
    byteConfig.getStrategyConfig().put(DeficitRoundRobinStrategy.CONFIG_KEY_QUANTUM, 1024);
    
    DeficitRoundRobinStrategy byteStrategy = new DeficitRoundRobinStrategy();
    byteStrategy.init(byteConfig);
    
    try {
      String topic = "test-topic";
      
      // Enqueue a large request (400 bytes)
      assertTrue(byteStrategy.enqueue(createRequest(topic, 400)));
      assertEquals(400, byteStrategy.getPendingBytes(topic));
      
      // Enqueue a small request (50 bytes) - should succeed
      assertTrue(byteStrategy.enqueue(createRequest(topic, 50)));
      assertEquals(450, byteStrategy.getPendingBytes(topic));
      
      // Another 50 bytes should succeed (total 500)
      assertTrue(byteStrategy.enqueue(createRequest(topic, 50)));
      assertEquals(500, byteStrategy.getPendingBytes(topic));
      
      // Any additional bytes should be rejected
      assertFalse(byteStrategy.enqueue(createRequest(topic, 1)));
      assertFalse(byteStrategy.enqueue(createRequest(topic, 100)));
      
      assertEquals(3, byteStrategy.getPendingCount());
      assertEquals(500, byteStrategy.getPendingBytes(topic));
    } finally {
      byteStrategy.shutdown();
    }
  }

  // ===========================================
  // Fairness Tests
  // ===========================================

  @Test
  public void testRoundRobinFairness() {
    String topic1 = "topic-a";
    String topic2 = "topic-b";
    String topic3 = "topic-c";
    
    // Enqueue multiple requests per topic (same size)
    for (int i = 0; i < 10; i++) {
      strategy.enqueue(createRequest(topic1, 100));
      strategy.enqueue(createRequest(topic2, 100));
      strategy.enqueue(createRequest(topic3, 100));
    }
    
    assertEquals(30, strategy.getPendingCount());
    
    // Dequeue all and count per topic
    Set<String> allTopics = new HashSet<>();
    allTopics.add(topic1);
    allTopics.add(topic2);
    allTopics.add(topic3);
    
    Map<String, AtomicInteger> dequeueCounts = new HashMap<>();
    dequeueCounts.put(topic1, new AtomicInteger(0));
    dequeueCounts.put(topic2, new AtomicInteger(0));
    dequeueCounts.put(topic3, new AtomicInteger(0));
    
    // Dequeue in batches and check fairness
    List<String> dequeueOrder = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      QueuedRequest req = strategy.dequeue(allTopics);
      assertNotNull("Should have request at iteration " + i, req);
      dequeueCounts.get(req.getTopicName()).incrementAndGet();
      dequeueOrder.add(req.getTopicName());
    }
    
    // All topics should have been dequeued completely
    assertEquals(10, dequeueCounts.get(topic1).get());
    assertEquals(10, dequeueCounts.get(topic2).get());
    assertEquals(10, dequeueCounts.get(topic3).get());
    
    // Verify round-robin behavior - check that we alternate between topics
    // in the first few dequeues (before any topic runs out of quantum)
    // With DRR and same-sized requests, we should see interleaving
    verifyInterleaving(dequeueOrder.subList(0, Math.min(15, dequeueOrder.size())));
  }

  /**
   * Verify that the dequeue order shows interleaving between topics.
   */
  private void verifyInterleaving(List<String> order) {
    // Count consecutive same-topic dequeues
    int maxConsecutive = 0;
    int currentConsecutive = 1;
    
    for (int i = 1; i < order.size(); i++) {
      if (order.get(i).equals(order.get(i - 1))) {
        currentConsecutive++;
        maxConsecutive = Math.max(maxConsecutive, currentConsecutive);
      } else {
        currentConsecutive = 1;
      }
    }
    
    // With fair queueing and equal-sized requests, we shouldn't see
    // long runs of the same topic (indicates starvation)
    assertTrue("Max consecutive from same topic should be reasonable: " + maxConsecutive, 
        maxConsecutive <= 5);
  }

  @Test
  public void testDRRFairnessWithDifferentSizes() {
    String smallTopic = "small";
    String largeTopic = "large";
    
    // Set quantum to 500 bytes
    QueueingConfig drrConfig = new QueueingConfig();
    drrConfig.setMaxQueueBytesPerTopic(1024 * 1024); // 1MB
    drrConfig.getStrategyConfig().put(DeficitRoundRobinStrategy.CONFIG_KEY_QUANTUM, 500);
    
    DeficitRoundRobinStrategy drrStrategy = new DeficitRoundRobinStrategy();
    drrStrategy.init(drrConfig);
    
    try {
      // Enqueue small requests (100 bytes each) and large requests (400 bytes each)
      for (int i = 0; i < 20; i++) {
        drrStrategy.enqueue(createRequest(smallTopic, 100));
        drrStrategy.enqueue(createRequest(largeTopic, 400));
      }
      
      assertEquals(40, drrStrategy.getPendingCount());
      
      Set<String> allTopics = new HashSet<>();
      allTopics.add(smallTopic);
      allTopics.add(largeTopic);
      
      Map<String, Long> bytesDequeued = new HashMap<>();
      bytesDequeued.put(smallTopic, 0L);
      bytesDequeued.put(largeTopic, 0L);
      
      // Dequeue all requests
      int dequeueCount = 0;
      while (drrStrategy.getPendingCount() > 0 && dequeueCount < 50) {
        QueuedRequest req = drrStrategy.dequeue(allTopics);
        if (req != null) {
          bytesDequeued.compute(req.getTopicName(), 
              (k, v) -> v + req.getSize());
          dequeueCount++;
        }
      }
      
      // Both topics should have been processed with roughly fair byte allocation
      // Small topic: 20 * 100 = 2000 bytes
      // Large topic: 20 * 400 = 8000 bytes
      assertEquals(2000L, (long) bytesDequeued.get(smallTopic));
      assertEquals(8000L, (long) bytesDequeued.get(largeTopic));
      assertEquals(0, drrStrategy.getPendingCount());
    } finally {
      drrStrategy.shutdown();
    }
  }

  @Test
  public void testFairnessUnderLoad() {
    // Test fairness when topics have different numbers of pending requests
    String hotTopic = "hot-topic";
    String coldTopic = "cold-topic";
    
    // Hot topic has many requests
    for (int i = 0; i < 50; i++) {
      strategy.enqueue(createRequest(hotTopic, 100));
    }
    
    // Cold topic has few requests
    for (int i = 0; i < 5; i++) {
      strategy.enqueue(createRequest(coldTopic, 100));
    }
    
    Set<String> allTopics = new HashSet<>();
    allTopics.add(hotTopic);
    allTopics.add(coldTopic);
    
    // Dequeue first 10 requests and check that cold topic gets fair share
    int coldCount = 0;
    
    for (int i = 0; i < 10; i++) {
      QueuedRequest req = strategy.dequeue(allTopics);
      if (req != null && coldTopic.equals(req.getTopicName())) {
        coldCount++;
      }
    }
    
    // Cold topic should get processed, not starved
    assertTrue("Cold topic should get some processing: " + coldCount, coldCount >= 1);
    
    // Continue until cold topic is empty
    while (strategy.getPendingCount(coldTopic) > 0) {
      QueuedRequest req = strategy.dequeue(allTopics);
      if (req != null && coldTopic.equals(req.getTopicName())) {
        coldCount++;
      }
    }
    
    assertEquals(5, coldCount);
  }

  // ===========================================
  // Thread Assignment Tests  
  // ===========================================

  @Test
  public void testTopicThreadAssignment() {
    // Test that topics are consistently assigned to threads
    List<String> topics = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      topics.add("topic-" + i);
    }
    
    int numThreads = 4;
    Map<Integer, Set<String>> threadAssignments = new HashMap<>();
    
    for (int i = 0; i < numThreads; i++) {
      threadAssignments.put(i, new HashSet<>());
    }
    
    // Simulate thread assignment logic from QueuedPacketSwitchingHandler
    for (String topic : topics) {
      int assignedThread = Math.abs(topic.hashCode()) % numThreads;
      threadAssignments.get(assignedThread).add(topic);
    }
    
    // Verify all topics are assigned
    int totalAssigned = threadAssignments.values().stream()
        .mapToInt(Set::size).sum();
    assertEquals(20, totalAssigned);
    
    // Verify each topic is assigned to exactly one thread
    Set<String> allAssigned = new HashSet<>();
    for (Set<String> assigned : threadAssignments.values()) {
      for (String topic : assigned) {
        assertFalse("Topic should only be assigned once: " + topic, 
            allAssigned.contains(topic));
        allAssigned.add(topic);
      }
    }
    
    // Verify distribution is somewhat balanced (not all in one thread)
    for (int i = 0; i < numThreads; i++) {
      int assigned = threadAssignments.get(i).size();
      assertTrue("Thread " + i + " should have some topics, but has " + assigned, 
          assigned >= 1);
    }
  }

  @Test
  public void testConsistentThreadAssignment() {
    // Verify that the same topic is always assigned to the same thread
    String topic = "consistent-topic";
    int numThreads = 4;
    
    int expectedThread = Math.abs(topic.hashCode()) % numThreads;
    
    // Check multiple times
    for (int i = 0; i < 100; i++) {
      int assignedThread = Math.abs(topic.hashCode()) % numThreads;
      assertEquals(expectedThread, assignedThread);
    }
  }

  // ===========================================
  // Concurrent Access Tests
  // ===========================================

  @Test
  public void testConcurrentEnqueue() throws InterruptedException {
    int numThreads = 10;
    int requestsPerThread = 100;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);
    AtomicInteger successCount = new AtomicInteger(0);
    
    // Create threads that enqueue concurrently
    for (int t = 0; t < numThreads; t++) {
      final int threadId = t;
      new Thread(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < requestsPerThread; i++) {
            String topic = "topic-" + (threadId % 3); // 3 topics
            if (strategy.enqueue(createRequest(topic, 100))) {
              successCount.incrementAndGet();
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          doneLatch.countDown();
        }
      }).start();
    }
    
    // Start all threads
    startLatch.countDown();
    
    // Wait for completion
    assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
    
    // All enqueues should succeed (queue is large enough)
    assertEquals(numThreads * requestsPerThread, successCount.get());
    assertEquals(numThreads * requestsPerThread, strategy.getPendingCount());
  }

  @Test
  public void testConcurrentEnqueueDequeue() throws InterruptedException {
    int numProducers = 5;
    int numConsumers = 3;
    int requestsPerProducer = 100;
    
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch producersDone = new CountDownLatch(numProducers);
    AtomicInteger produced = new AtomicInteger(0);
    AtomicInteger consumed = new AtomicInteger(0);
    AtomicInteger running = new AtomicInteger(1);
    
    Set<String> allTopics = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      allTopics.add("topic-" + i);
    }
    
    // Start consumers
    for (int c = 0; c < numConsumers; c++) {
      new Thread(() -> {
        try {
          startLatch.await();
          while (running.get() == 1 || strategy.getPendingCount() > 0) {
            QueuedRequest req = strategy.dequeue(allTopics);
            if (req != null) {
              consumed.incrementAndGet();
            } else {
              Thread.sleep(1);
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }).start();
    }
    
    // Start producers
    for (int p = 0; p < numProducers; p++) {
      final int producerId = p;
      new Thread(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < requestsPerProducer; i++) {
            String topic = "topic-" + (producerId % 3);
            if (strategy.enqueue(createRequest(topic, 100))) {
              produced.incrementAndGet();
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          producersDone.countDown();
        }
      }).start();
    }
    
    // Start all threads
    startLatch.countDown();
    
    // Wait for producers to finish
    assertTrue(producersDone.await(10, TimeUnit.SECONDS));
    
    // Give consumers time to drain
    Thread.sleep(500);
    running.set(0);
    
    // Wait a bit more for final consumption
    Thread.sleep(200);
    
    // All produced should be consumed
    assertEquals(produced.get(), consumed.get());
    assertEquals(0, strategy.getPendingCount());
  }

  // ===========================================
  // QueueingConfig Tests
  // ===========================================

  @Test
  public void testQueueingConfigDefaults() {
    QueueingConfig defaultConfig = new QueueingConfig();
    
    assertFalse(defaultConfig.isEnabled());
    assertEquals("com.pinterest.memq.core.rpc.queue.DeficitRoundRobinStrategy", 
        defaultConfig.getStrategyClass());
    assertEquals(4, defaultConfig.getDequeueThreadPoolSize());
    assertEquals(100 * 1024 * 1024, defaultConfig.getMaxQueueBytesPerTopic()); // 100MB
    assertNotNull(defaultConfig.getStrategyConfig());
    assertTrue(defaultConfig.getStrategyConfig().isEmpty());
  }

  @Test
  public void testQueueingConfigSetters() {
    QueueingConfig customConfig = new QueueingConfig();
    customConfig.setEnabled(true);
    customConfig.setStrategyClass("com.example.CustomStrategy");
    customConfig.setDequeueThreadPoolSize(8);
    customConfig.setMaxQueueBytesPerTopic(50 * 1024 * 1024); // 50MB
    customConfig.getStrategyConfig().put(DeficitRoundRobinStrategy.CONFIG_KEY_QUANTUM, 32768);
    
    assertTrue(customConfig.isEnabled());
    assertEquals("com.example.CustomStrategy", customConfig.getStrategyClass());
    assertEquals(8, customConfig.getDequeueThreadPoolSize());
    assertEquals(50 * 1024 * 1024, customConfig.getMaxQueueBytesPerTopic());
    assertEquals(32768, customConfig.getStrategyConfig().get(DeficitRoundRobinStrategy.CONFIG_KEY_QUANTUM));
  }

  @Test
  public void testDRRDefaultQuantum() {
    // Test that DRR uses default quantum when not configured
    QueueingConfig noQuantumConfig = new QueueingConfig();
    noQuantumConfig.setMaxQueueBytesPerTopic(1024 * 1024);
    // Don't set quantum in strategyConfig
    
    DeficitRoundRobinStrategy defaultStrategy = new DeficitRoundRobinStrategy();
    defaultStrategy.init(noQuantumConfig);
    
    try {
      assertEquals(DeficitRoundRobinStrategy.DEFAULT_QUANTUM, defaultStrategy.getQuantum());
      assertEquals(65536, defaultStrategy.getQuantum()); // 64KB
    } finally {
      defaultStrategy.shutdown();
    }
  }

  @Test
  public void testDRRCustomQuantum() {
    // Test that DRR uses custom quantum when configured
    QueueingConfig customQuantumConfig = new QueueingConfig();
    customQuantumConfig.setMaxQueueBytesPerTopic(1024 * 1024);
    customQuantumConfig.getStrategyConfig().put(DeficitRoundRobinStrategy.CONFIG_KEY_QUANTUM, 8192);
    
    DeficitRoundRobinStrategy customStrategy = new DeficitRoundRobinStrategy();
    customStrategy.init(customQuantumConfig);
    
    try {
      assertEquals(8192, customStrategy.getQuantum());
    } finally {
      customStrategy.shutdown();
    }
  }

  @Test
  public void testBytesDecrementOnDequeue() {
    // Verify that bytes are correctly decremented when requests are dequeued
    String topic = "test-topic";
    
    assertTrue(strategy.enqueue(createRequest(topic, 100)));
    assertTrue(strategy.enqueue(createRequest(topic, 200)));
    assertTrue(strategy.enqueue(createRequest(topic, 300)));
    
    assertEquals(600, strategy.getPendingBytes(topic));
    assertEquals(3, strategy.getPendingCount(topic));
    
    Set<String> topics = new HashSet<>();
    topics.add(topic);
    
    // Dequeue first request
    QueuedRequest req1 = strategy.dequeue(topics);
    assertNotNull(req1);
    assertEquals(100, req1.getSize());
    assertEquals(500, strategy.getPendingBytes(topic));
    assertEquals(2, strategy.getPendingCount(topic));
    
    // Dequeue second request
    QueuedRequest req2 = strategy.dequeue(topics);
    assertNotNull(req2);
    assertEquals(200, req2.getSize());
    assertEquals(300, strategy.getPendingBytes(topic));
    assertEquals(1, strategy.getPendingCount(topic));
    
    // Dequeue third request
    QueuedRequest req3 = strategy.dequeue(topics);
    assertNotNull(req3);
    assertEquals(300, req3.getSize());
    assertEquals(0, strategy.getPendingBytes(topic));
    assertEquals(0, strategy.getPendingCount(topic));
  }

  // ===========================================
  // ByteBuf Retain/Release Tests
  // ===========================================

  @Test
  public void testByteBufRetainReleaseOnEnqueueDequeue() {
    // This test verifies the ByteBuf retain/release pattern that prevents
    // "refCnt: 0" errors when requests are queued.
    //
    // The flow being tested:
    // 1. Request with ByteBuf is created (refCnt = 1)
    // 2. Before enqueueing, retain() is called (refCnt = 2)
    // 3. Original holder releases (simulating MemqRequestDecoder finally block) (refCnt = 1)
    // 4. After dequeue and processing, release() is called (refCnt = 0, buffer recycled)
    
    String topic = "test-topic";
    
    // Create a request with a pooled buffer
    ByteBuf pooledBuffer = PooledByteBufAllocator.DEFAULT.buffer(100);
    pooledBuffer.writeBytes(new byte[100]);
    assertEquals(1, pooledBuffer.refCnt());
    
    WriteRequestPacket writePacket = new WriteRequestPacket();
    writePacket.setTopicName(topic);
    writePacket.setData(pooledBuffer);
    
    // Simulate what QueuedPacketSwitchingHandler.handle() does:
    // Retain before enqueueing
    writePacket.getData().retain();
    assertEquals(2, pooledBuffer.refCnt());
    
    QueuedRequest request = new QueuedRequest(null, new RequestPacket(), writePacket);
    assertTrue(strategy.enqueue(request));
    
    // Simulate MemqRequestDecoder finally block releasing the original buffer
    pooledBuffer.release();
    assertEquals(1, pooledBuffer.refCnt()); // Still alive due to retain()
    
    // Dequeue the request
    Set<String> topics = new HashSet<>();
    topics.add(topic);
    QueuedRequest dequeued = strategy.dequeue(topics);
    assertNotNull(dequeued);
    
    // Buffer should still be usable
    assertEquals(1, dequeued.getWritePacket().getData().refCnt());
    assertTrue(dequeued.getWritePacket().getData().isReadable());
    
    // Simulate what processQueuedRequest finally block does:
    // Release after processing
    dequeued.getWritePacket().getData().release();
    assertEquals(0, pooledBuffer.refCnt()); // Now fully released
  }

  @Test
  public void testByteBufReleaseOnEnqueueFailure() {
    // Test that ByteBuf is properly released when enqueue fails (queue full)
    
    QueueingConfig smallConfig = new QueueingConfig();
    smallConfig.setMaxQueueBytesPerTopic(100); // Very small
    smallConfig.getStrategyConfig().put(DeficitRoundRobinStrategy.CONFIG_KEY_QUANTUM, 1024);
    
    DeficitRoundRobinStrategy smallStrategy = new DeficitRoundRobinStrategy();
    smallStrategy.init(smallConfig);
    
    try {
      String topic = "test-topic";
      
      // Fill the queue
      ByteBuf buf1 = PooledByteBufAllocator.DEFAULT.buffer(100);
      buf1.writeBytes(new byte[100]);
      WriteRequestPacket writePacket1 = new WriteRequestPacket();
      writePacket1.setTopicName(topic);
      writePacket1.setData(buf1);
      
      // Retain before enqueue (simulating handle())
      buf1.retain();
      assertEquals(2, buf1.refCnt());
      
      assertTrue(smallStrategy.enqueue(new QueuedRequest(null, new RequestPacket(), writePacket1)));
      
      // Simulate decoder finally block
      buf1.release();
      assertEquals(1, buf1.refCnt());
      
      // Now try to enqueue another request that will fail
      ByteBuf buf2 = PooledByteBufAllocator.DEFAULT.buffer(100);
      buf2.writeBytes(new byte[100]);
      WriteRequestPacket writePacket2 = new WriteRequestPacket();
      writePacket2.setTopicName(topic);
      writePacket2.setData(buf2);
      
      // Retain before enqueue attempt
      buf2.retain();
      assertEquals(2, buf2.refCnt());
      
      // Enqueue should fail
      boolean enqueued = smallStrategy.enqueue(new QueuedRequest(null, new RequestPacket(), writePacket2));
      assertFalse(enqueued);
      
      // On failure, QueuedPacketSwitchingHandler releases the buffer it retained
      if (!enqueued) {
        buf2.release(); // Simulating the release on failure in handle()
      }
      assertEquals(1, buf2.refCnt());
      
      // Simulate decoder finally block
      buf2.release();
      assertEquals(0, buf2.refCnt()); // Properly released
      
      // Clean up the first buffer
      Set<String> topics = new HashSet<>();
      topics.add(topic);
      QueuedRequest dequeued = smallStrategy.dequeue(topics);
      assertNotNull(dequeued);
      dequeued.getWritePacket().getData().release();
      assertEquals(0, buf1.refCnt());
      
    } finally {
      smallStrategy.shutdown();
    }
  }

  @Test
  public void testByteBufRemainsReadableWhileQueued() {
    // Test that the ByteBuf data remains readable while the request is queued
    
    String topic = "test-topic";
    byte[] testData = "Hello, World!".getBytes();
    
    ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer(testData.length);
    buffer.writeBytes(testData);
    
    WriteRequestPacket writePacket = new WriteRequestPacket();
    writePacket.setTopicName(topic);
    writePacket.setData(buffer);
    
    // Retain before enqueueing
    buffer.retain();
    assertEquals(2, buffer.refCnt());
    
    QueuedRequest request = new QueuedRequest(null, new RequestPacket(), writePacket);
    assertTrue(strategy.enqueue(request));
    
    // Simulate decoder releasing
    buffer.release();
    assertEquals(1, buffer.refCnt());
    
    // Dequeue
    Set<String> topics = new HashSet<>();
    topics.add(topic);
    QueuedRequest dequeued = strategy.dequeue(topics);
    assertNotNull(dequeued);
    
    // Verify data is still readable
    ByteBuf dequeuedData = dequeued.getWritePacket().getData();
    assertEquals(testData.length, dequeuedData.readableBytes());
    
    byte[] readData = new byte[testData.length];
    dequeuedData.readBytes(readData);
    
    for (int i = 0; i < testData.length; i++) {
      assertEquals(testData[i], readData[i]);
    }
    
    // Clean up
    dequeuedData.release();
    assertEquals(0, buffer.refCnt());
  }
}
