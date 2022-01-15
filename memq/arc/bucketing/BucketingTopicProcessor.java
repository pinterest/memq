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
package com.pinterest.memq.core.processing.bucketing;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.ServiceUnavailableException;
import javax.ws.rs.core.Response;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer.Context;
import com.pinterest.memq.client.commons2.DataNotFoundException;
import com.pinterest.memq.commons.protocol.BatchData;
import com.pinterest.memq.commons.protocol.ReadRequestPacket;
import com.pinterest.memq.commons.protocol.ReadResponsePacket;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.protocol.WriteRequestPacket;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.core.commons.MemqProcessingThreadFactory;
import com.pinterest.memq.core.processing.Ackable;
import com.pinterest.memq.core.processing.MapAcker;
import com.pinterest.memq.core.processing.TopicProcessor;
import com.pinterest.memq.core.processing.TopicProcessorState;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedFile;

public class BucketingTopicProcessor extends TopicProcessor {

  private static final Logger logger = Logger.getLogger(BucketingTopicProcessor.class.getName());

  private ExecutorService processingService;
  private Batch[] batches;
  private int ringBufferSize;
  private volatile long currentBatchIndex;
  private Lock lock = new ReentrantLock();
  private AtomicLong serverRequestIdGenerator;
  private String topicName;
  private StorageHandler outputHandler;
  private MapAcker acker;
  private int bufferSize;
  private ScheduledExecutorService timerService;
  private int timeBatch;
  private int tickFrequencyMillis;

  private AtomicLong remainingSlots;

  // metrics
  private MetricRegistry registry;
  private Counter timeBasedBatchCounter;
  private Counter sizedBasedBatchCounter;

  private static volatile boolean enableDebug = false;

  private TopicConfig topicConfig;

  private ScheduledReporter reporter;

  public BucketingTopicProcessor(MetricRegistry registry,
                                 TopicConfig topicConfig,
                                 StorageHandler outputHandler,
                                 ScheduledExecutorService timerService,
                                 ScheduledReporter reporter) {
    this.topicConfig = topicConfig;
    this.timerService = timerService;
    this.reporter = reporter;
    this.tickFrequencyMillis = topicConfig.getTickFrequencyMillis();
    this.ringBufferSize = topicConfig.getRingBufferSize();
    this.remainingSlots = new AtomicLong(ringBufferSize);
    this.state = TopicProcessorState.INITIALIZING;
    this.timeBatch = topicConfig.getBatchMilliSeconds();
    this.registry = registry;
    this.outputHandler = outputHandler;
    this.topicName = topicConfig.getTopic();
    this.acker = new MapAcker(topicName, registry);
    int outputParallelism = topicConfig.getOutputParallelism();
    this.processingService = Executors.newFixedThreadPool(outputParallelism,
        new MemqProcessingThreadFactory("processing-"));
    this.serverRequestIdGenerator = new AtomicLong(ThreadLocalRandom.current().nextLong());
    this.bufferSize = topicConfig.getBufferSize();
    int countPerBatch = (int) topicConfig.getBatchSizeBytes() / bufferSize;
    int numberOfBatches = ringBufferSize / countPerBatch;
    this.batches = new Batch[numberOfBatches];
    for (int i = 0; i < numberOfBatches; i++) {
      this.batches[i] = new Batch(countPerBatch, remainingSlots);
    }

    initializeMetrics(registry);
    state = TopicProcessorState.RUNNING;
    logger.info("Initialized BucketingTopicProcessor with numberOfBatches:" + numberOfBatches
        + " countPerBatch:" + countPerBatch + " outputParallelism:" + outputParallelism);
    startTimeBasedBatch();
  }

  @Override
  protected void initializeMetrics(MetricRegistry registry) {
    super.initializeMetrics(registry);
    this.sizedBasedBatchCounter = registry.counter("batching.sizedbasedbatch");
    this.timeBasedBatchCounter = registry.counter("batching.timebasedbatch");
    registry.register("tp.processor.remainingSlots", new Gauge<Long>() {

      @Override
      public Long getValue() {
        return remainingSlots.get();
      }

    });
  }

  private void startTimeBasedBatch() {
    timerService.scheduleAtFixedRate(() -> {
      lock.lock();
      try {
        Batch batch = getOrFinalizeBatch();
        if (batch.hasData()
            && System.currentTimeMillis() - batch.getOriginationTimestamp() > timeBatch) {
          timeBasedBatchCounter.inc();
          dispatchBatch(batch);
        }
      } catch (Exception e) {
      } finally {
        lock.unlock();
      }
    }, 0, tickFrequencyMillis, TimeUnit.MILLISECONDS);
  }

  private Batch getOrFinalizeBatch() {
    Batch currentBatch = batches[(int) (currentBatchIndex % batches.length)];
    if (!currentBatch.hasSpace()) {
      // check if the barrier allows this request to be queued
      sizedBasedBatchCounter.inc();
      currentBatch = dispatchBatch(currentBatch);
    }
    if (!currentBatch.isAvailable()) {
      // if both current batch isn't available
      throw new ServiceUnavailableException(
          "Topic processor not available, current batch and next batch both haven't been upload yet");
    }
    return currentBatch;
  }

  private Batch dispatchBatch(Batch currentBatch) {
    if (enableDebug) {
      logger.warning("Dispatching batch:" + currentBatch);
    }
    currentBatch.setAvailable(false);
    // dispatch request
    processingService
        .execute(new BatchOutputTask(topicName, registry, currentBatch, outputHandler));
    currentBatchIndex++;
    int nextBatchIndex = (int) (currentBatchIndex % batches.length);
    logger.fine("Dispatched batch accumulationLatency:"
        + (currentBatch.getDispatchTimestamp() - currentBatch.getOriginationTimestamp())
        + "ms next batch index:" + nextBatchIndex);
    return batches[nextBatchIndex];
  }

  @Override
  public long write(RequestPacket basePacket,
                    WriteRequestPacket writePacket,
                    ChannelHandlerContext ctx) {
    if (writePacket.getDataLength() > bufferSize) {
      writeRejectCounter.inc();
      throw new BadRequestException(
          "Payload too big for " + basePacket.getClientRequestId() + " " + bufferSize);
    }
    Context totalWriteLatencyTimer = totalWriteLatency.time();
    lock.lock();
    long serverRequestId = serverRequestIdGenerator.getAndIncrement();
    try {
      Batch currentBatch = getOrFinalizeBatch();
      try {
        currentBatch.write(serverRequestId, basePacket, writePacket, ctx);
      } catch (Exception e) {
        throw new BadRequestException(
            "Failed to write message " + basePacket.getClientRequestId() + " : " + e.getMessage());
      }
      writeCounter.inc();
    } finally {
      lock.unlock();
      totalWriteLatencyTimer.stop();
    }
    return serverRequestId;
  }

  @Override
  public void read(RequestPacket requestPacket,
                   ReadRequestPacket readPacket,
                   ChannelHandlerContext ctx) {
    try {
      BatchData batch = outputHandler
          .fetchBatchStreamForNotificationBuf(readPacket.getNotification());
      if (batch.getDataAsBuf() != null) {
        ctx.writeAndFlush(new ReadResponsePacket(batch));
      } else {
        ctx.write(new ReadResponsePacket(batch));
        RandomAccessFile raf = (RandomAccessFile) batch.getSendFileRef();
        if (ctx.pipeline().get(SslHandler.class) == null) {
          // SSL not enabled - can use zero-copy file transfer.
          ctx.writeAndFlush(new DefaultFileRegion(raf.getChannel(), 0, batch.getLength()));
        } else {
          // SSL enabled - cannot use zero-copy file transfer.
          ctx.writeAndFlush(new ChunkedFile(raf));
        }
      }
    } catch (IOException e) {
      throw new InternalServerErrorException(Response.serverError().build(), e);
    } catch (DataNotFoundException e) {
      ctx.writeAndFlush(new ResponsePacket(RequestType.PROTOCOL_VERSION,
          requestPacket.getClientRequestId(), requestPacket.getRequestType(), ResponseCodes.NO_DATA,
          new ReadResponsePacket(new BatchData(0, null))));
    }
  }

  protected void forceDispatch() {
    lock.lock();
    Batch currentBatch = getOrFinalizeBatch();
    if (currentBatch.hasData()) {
      dispatchBatch(currentBatch);
    }
    lock.unlock();
  }

  @Override
  public void stopNow() {
    processingService.shutdownNow();
  }

  @Override
  public void stopAndAwait() throws InterruptedException {
    if (reporter != null) {
      reporter.close();
    }
    processingService.shutdown();
    processingService.awaitTermination(100, TimeUnit.SECONDS);
    outputHandler.closeWriter();
  }

  @Override
  public Ackable getAcker() {
    return acker;
  }

  @Override
  public int getRemaining() {
    return (int) remainingSlots.get();
  }

  @Override
  public float getAvailableCapacity() {
    return getRemaining() / ringBufferSize;
  }

  public static void setEnableDebug(boolean enableDebug) {
    BucketingTopicProcessor.enableDebug = enableDebug;
  }

  @Override
  public TopicConfig getTopicConfig() {
    return topicConfig;
  }
}
