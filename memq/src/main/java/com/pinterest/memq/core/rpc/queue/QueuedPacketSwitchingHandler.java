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

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.ServiceUnavailableException;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.WriteRequestPacket;
import com.pinterest.memq.core.MemqManager;
import com.pinterest.memq.core.clustering.MemqGovernor;
import com.pinterest.memq.core.config.QueueingConfig;
import com.pinterest.memq.core.rpc.PacketSwitchingHandler;
import com.pinterest.memq.core.security.Authorizer;

import io.netty.channel.ChannelHandlerContext;

/**
 * A PacketSwitchingHandler that implements fair queueing for write requests.
 * 
 * Write requests are enqueued into topic-specific queues and processed by
 * a configurable thread pool using a fair scheduling algorithm (e.g., DRR).
 * 
 * Read and metadata requests are processed directly without queueing.
 */
public class QueuedPacketSwitchingHandler extends PacketSwitchingHandler {

  private static final Logger logger = Logger.getLogger(QueuedPacketSwitchingHandler.class.getName());

  private final QueueingStrategy strategy;
  private final ExecutorService dequeueExecutor;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final ConcurrentHashMap<String, Boolean> perTopicGaugeRegistered = new ConcurrentHashMap<>();

  /**
   * Number of dequeue threads.
   * Topics are distributed across threads using consistent hashing on topic name.
   */
  private final int numDequeueThreads;

  public QueuedPacketSwitchingHandler(MemqManager mgr,
                                       MemqGovernor governor,
                                       Authorizer authorizer,
                                       MetricRegistry registry,
                                       QueueingConfig queueingConfig) throws Exception {
    super(mgr, governor, authorizer, registry);
    this.numDequeueThreads = queueingConfig.getDequeueThreadPoolSize();
    
    // Initialize the queueing strategy first
    this.strategy = createStrategy(queueingConfig);
    
    // Initialize the dequeue thread pool with named daemon threads
    this.dequeueExecutor = Executors.newFixedThreadPool(
        numDequeueThreads,
        new ThreadFactory() {
          private final AtomicInteger threadNumber = new AtomicInteger(1);
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "memq-dequeue-" + threadNumber.getAndIncrement());
            t.setDaemon(true);
            return t;
          }
        });
    
    // Start the dequeue worker threads
    for (int i = 0; i < numDequeueThreads; i++) {
      final int threadIndex = i;
      dequeueExecutor.submit(() -> dequeueLoop(threadIndex));
    }
    
    logger.info("QueuedPacketSwitchingHandler initialized with strategy: " + 
        queueingConfig.getStrategyClass() + ", threads: " + numDequeueThreads);
  }

  /**
   * Create a QueueingStrategy instance from configuration.
   */
  @SuppressWarnings("unchecked")
  private QueueingStrategy createStrategy(QueueingConfig config) throws Exception {
    Class<? extends QueueingStrategy> strategyClass = 
        (Class<? extends QueueingStrategy>) Class.forName(config.getStrategyClass());
    QueueingStrategy strategy = strategyClass.getDeclaredConstructor().newInstance();
    strategy.init(config);
    return strategy;
  }

  @Override
  public void handle(ChannelHandlerContext ctx,
                     RequestPacket requestPacket,
                     Principal principal,
                     String clientAddress) throws Exception {
    // Only queue WRITE requests
    if (requestPacket.getRequestType() == RequestType.WRITE) {
      WriteRequestPacket writePacket = (WriteRequestPacket) requestPacket.getPayload();
      registerPerTopicMetricsIfNeeded(writePacket.getTopicName());
      
      // Retain the ByteBuf since it will be released by MemqRequestDecoder's finally block
      // but we need it to stay alive until the request is dequeued and processed.
      // The buffer will be released in processQueuedRequest after processing.
      writePacket.getData().retain();
      
      // Create queued request
      QueuedRequest queuedRequest = new QueuedRequest(ctx, requestPacket, writePacket);
      
      // Try to enqueue
      boolean enqueued = strategy.enqueue(queuedRequest);
      
      if (enqueued) {
        MetricRegistry topicRegistry = mgr.getRegistry().get(queuedRequest.getTopicName());
        if (topicRegistry != null) {
          topicRegistry.counter("queue.enqueued.bytes").inc(queuedRequest.getSize());
        }
      } else {
        // Release the buffer since we retained it but failed to enqueue
        writePacket.getData().release();
        MetricRegistry topicRegistry = mgr.getRegistry().get(queuedRequest.getTopicName());
        if (topicRegistry != null) {
          topicRegistry.counter("queue.rejected.bytes").inc(queuedRequest.getSize());
        }
        // TODO: we should return a response packet with an error code signalling congestion
        throw new ServiceUnavailableException("Queue full for topic: " + writePacket.getTopicName());
      }
    } else {
      // Process non-write requests directly (metadata, read requests)
      super.handle(ctx, requestPacket, principal, clientAddress);
    }
  }

  /**
   * Main dequeue loop for each worker thread.
   * Each thread is responsible for processing a subset of topics.
   */
  private void dequeueLoop(int threadIndex) {
    logger.info("Dequeue worker thread " + threadIndex + " started");
    
    while (running.get()) {
      try {
        // Get topics assigned to this thread
        Set<String> assignedTopics = getAssignedTopics(threadIndex);
        
        // Try to dequeue a request
        QueuedRequest request = strategy.dequeue(assignedTopics);
        
        if (request != null) {
          registerPerTopicMetricsIfNeeded(request.getTopicName());
          long queueLatencyNanos = System.nanoTime() - request.getEnqueueTimeNanos();
          MetricRegistry topicRegistry = mgr.getRegistry().get(request.getTopicName());
          if (topicRegistry != null) {
            topicRegistry.timer("queue.latency.nanos").update(queueLatencyNanos, TimeUnit.NANOSECONDS);
          }
          if (topicRegistry != null) {
            topicRegistry.counter("queue.dequeued.bytes").inc(request.getSize());
          }
          processQueuedRequest(request);
        } else {
          // No work available, sleep briefly to avoid spinning
          // TODO: event-driven approach to avoid spinning
          Thread.sleep(1);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        logger.log(Level.SEVERE, "Error in dequeue loop", e);
      }
    }
    
    logger.info("Dequeue worker thread " + threadIndex + " stopped");
  }

  /**
   * Get the set of topics assigned to a specific thread index.
   * Uses consistent hashing to distribute topics across threads.
   */
  private Set<String> getAssignedTopics(int threadIndex) {
    Set<String> assignedTopics = new HashSet<>();
    
    for (String topic : strategy.getActiveTopics()) {
      int assignedThread = Math.abs(topic.hashCode()) % numDequeueThreads;
      if (assignedThread == threadIndex) {
        assignedTopics.add(topic);
      }
    }
    
    return assignedTopics;
  }

  /**
   * Process a dequeued request by calling the parent's executeWriteRequest.
   */
  private void processQueuedRequest(QueuedRequest request) {
    try {
      executeWriteRequest(request.getCtx(), request.getRequestPacket(), request.getWritePacket());
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Error processing queued request for topic: " + 
          request.getTopicName(), e);
      // The channel context may need error handling
      handleProcessingError(request, e);
    } finally {
      // Release the ByteBuf that was retained when the request was enqueued.
      // This balances the retain() call in handle().
      try {
        request.getWritePacket().getData().release();
      } catch (Exception e) {
        logger.log(Level.WARNING, "Failed to release buffer for request", e);
      }
    }
  }

  /**
   * Handle errors that occur during request processing.
   */
  private void handleProcessingError(QueuedRequest request, Exception e) {
    try {
      ChannelHandlerContext ctx = request.getCtx();
      if (ctx.channel().isActive()) {
        // Error will be handled by the exception handler in the pipeline
        ctx.fireExceptionCaught(e);
      }
    } catch (Exception ex) {
      logger.log(Level.WARNING, "Failed to handle processing error", ex);
    }
  }

  private void registerPerTopicMetricsIfNeeded(String topicName) {
    if (perTopicGaugeRegistered.putIfAbsent(topicName, Boolean.TRUE) != null) {
      return;
    }
    MetricRegistry topicRegistry = mgr.getRegistry().get(topicName);
    if (topicRegistry == null) {
      return;
    }
    topicRegistry.gauge("queue.pending.bytes",
        () -> (Gauge<Long>) () -> strategy.getPendingBytes(topicName));
  }

  /**
   * Shutdown the handler and its thread pool.
   */
  public void shutdown() {
    running.set(false);
    strategy.shutdown();
    
    dequeueExecutor.shutdown();
    try {
      if (!dequeueExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
        dequeueExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      dequeueExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }
    
    logger.info("QueuedPacketSwitchingHandler shutdown complete");
  }

  /**
   * Get the queueing strategy being used.
   */
  public QueueingStrategy getStrategy() {
    return strategy;
  }

  /**
   * Get the pending bytes across all queues.
   */
  public long getPendingBytes() {
    return strategy.getPendingBytes();
  }
}
