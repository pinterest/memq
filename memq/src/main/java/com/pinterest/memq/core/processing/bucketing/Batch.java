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
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Timer;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.commons.protocol.WriteRequestPacket;
import com.pinterest.memq.commons.protocol.WriteResponsePacket;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.commons.storage.WriteFailedException;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.utils.CoreUtils;
import com.pinterest.memq.core.utils.MiscUtils;

import io.netty.channel.ChannelHandlerContext;

public class Batch {
  private static final Logger logger = Logger.getLogger(Batch.class.getName());
  private final ScheduledExecutorService scheduler;
  private final ExecutorService executor;
  private final StorageHandler handler;

  private long sizeDispatchThreshold;
  private int countDispatchThreshold;
  private Duration timeDispatchThreshold;

  private Message[] messages;

  private final AtomicInteger usedCapacity = new AtomicInteger();
  private final AtomicInteger activeWrites = new AtomicInteger();
  private final AtomicInteger messageIdx = new AtomicInteger();
  private final AtomicInteger messagesCount = new AtomicInteger();
  private final AtomicBoolean available = new AtomicBoolean(true);
  private final MetricRegistry registry;
  private final BatchManager manager;
  private boolean dispatching = false;
  private volatile Future<?> timeDispatchTask;
  private volatile long startTime;

  private Counter sizeBasedBatchCounter;
  private Counter timeBasedBatchCounter;
  private Counter countBasedBatchCounter;
  private Timer accumulationTime;

  public Batch(BatchManager manager,
               int countDispatchThreshold,
               long sizeDispatchThreshold,
               Duration timeDispatchThreshold,
               ScheduledExecutorService scheduler,
               ExecutorService executor,
               StorageHandler handler,
               MetricRegistry registry) {
    this.sizeDispatchThreshold = sizeDispatchThreshold;
    this.countDispatchThreshold = countDispatchThreshold;
    this.timeDispatchThreshold = timeDispatchThreshold;
    this.scheduler = scheduler;
    this.executor = executor;
    this.handler = handler;
    this.messages = new Message[countDispatchThreshold];
    this.registry = registry;
    this.manager = manager;

    initializeMetrics(registry);
  }

  protected void initializeMetrics(MetricRegistry registry) {
    this.sizeBasedBatchCounter = registry.counter("batching.sizedbasedbatch");
    this.timeBasedBatchCounter = registry.counter("batching.timebasedbatch");
    this.countBasedBatchCounter = registry.counter("batching.countbasedbatch");
    this.accumulationTime = MiscUtils.oneMinuteWindowTimer(registry,"batching.accumulation.time");
  }

  public void reset(long sizeDispatchThreshold, int countDispatchThreshold, Duration timeDispatchThreshold) {
    startTime = System.currentTimeMillis();
    scheduleTimeBasedDispatch();
    usedCapacity.set(0);
    messageIdx.set(0);
    for (int i = 0; i < messagesCount.get(); i++) {
      messages[i].recycle();
      messages[i] = null;
    }
    messagesCount.set(0);
    available.set(true);
    this.sizeDispatchThreshold = sizeDispatchThreshold;
    if (this.countDispatchThreshold != countDispatchThreshold) {
      // reset messages[] size to new countDispatchThreshold
      messages = new Message[countDispatchThreshold];
      this.countDispatchThreshold = countDispatchThreshold;
    }
    this.timeDispatchThreshold = timeDispatchThreshold;
    synchronized (this) {
      dispatching = false;
    }
  }

  protected void clear() {
    activeWrites.set(0);
  }

  protected void scheduleTimeBasedDispatch() {
    if (timeDispatchTask != null) {
      timeDispatchTask.cancel(true);
    }
    timeDispatchTask = scheduler.schedule(() -> {
      if (!Thread.interrupted()) {
        if (System.currentTimeMillis() - startTime >= timeDispatchThreshold.toMillis()) {
          // if wasAvailable == true, the payload was sealed due to time threshold, so we should try to dispatch
          // if it was false, it means that a write has been initiated and sealed the payload, so the dispatching is on that write
          boolean wasAvailable = seal();
          if (wasAvailable && isReadyToUpload()) {
            tryDispatch(true);
          }
        }
      }
    }, timeDispatchThreshold.toMillis(), TimeUnit.MILLISECONDS);
  }

  public boolean write(WriteRequestPacket writePacket,
                       long serverRequestId,
                       long clientRequestId,
                       short protocolVersion,
                       ChannelHandlerContext ctx) {
    int dataLength = writePacket.getDataLength();
    activeWrites.incrementAndGet();
    try {
      if (!available.get()) {
        return false;
      }
      int usage = usedCapacity.addAndGet(dataLength);
      if (usage < sizeDispatchThreshold) {
        int idx = messageIdx.getAndIncrement();
        if (idx < countDispatchThreshold) {
          messages[idx] = Message.newInstance(
              writePacket.getData().retainedSlice(),
              clientRequestId,
              serverRequestId,
              ctx,
              protocolVersion
          );
          messagesCount.getAndIncrement();
          if (idx == countDispatchThreshold - 1) {
            // last message should shut the door and finalize the batch
            seal();
          }
          return true;
        }
      }
      seal();
      return false;
    } finally {
      activeWrites.decrementAndGet();

      // if payload is not available
      if (!isAvailable() && isReadyToUpload()) {
        tryDispatch(false);
      }
    }
  }

  public boolean seal() {
    return available.getAndSet(false);
  }

  protected void tryDispatch(boolean isTimeBased) {
    if (!dispatching) {
      synchronized (this) {
        if (!dispatching) {
          if (!isTimeBased) {
            timeDispatchTask.cancel(true);
          }
          dispatching = true;
          if (isTimeBased) {
            timeBasedBatchCounter.inc();
          } else if (messagesCount.get() >= countDispatchThreshold) {
            countBasedBatchCounter.inc();
          } else {
            sizeBasedBatchCounter.inc();
          }
          accumulationTime.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
          dispatch(isTimeBased);
        }
      }
    }
  }

  protected void dispatch(boolean isTimeBased) {
    timeDispatchTask.cancel(true);
    executor.execute(new DispatchTask(isTimeBased));
  }

  protected boolean isReadyToUpload() {
    return activeWrites.get() == 0;
  }

  public boolean isAvailable() {
    return available.get();
  }

  protected class DispatchTask implements Runnable {
    private Counter uploadBatchCounter;
    private Counter uploadBytesCounter;
    private Timer uploadLatency;
    private Counter outputErrorCounter;
    private Counter uploadMessageCounter;
    private Counter ackChannelWriteError;
    private Timer ackLatency;
    private Counter activeParallelTasks;
    private Histogram batchSizeBytes;
    private final boolean isTimeBased;
    private Histogram batchMessageCountHistogram;

    public DispatchTask(boolean isTimeBased) {
      initializeMetrics();
      this.isTimeBased = isTimeBased;
    }

    private void initializeMetrics() {
      this.uploadBatchCounter = registry.counter("output.batchCount");
      this.activeParallelTasks = registry.counter("output.activeParallelTasks");
      this.uploadMessageCounter = registry.counter("output.messageCount");
      this.uploadBytesCounter = registry.counter("output.uploadBytes");
      this.outputErrorCounter = registry.counter("output.error");
      this.ackChannelWriteError = registry.counter("acker.ackerror");
      this.uploadLatency = MiscUtils.oneMinuteWindowTimer(registry,"output.uploadLatency");
      this.ackLatency = MiscUtils.oneMinuteWindowTimer(registry,"acker.push.latency");
      this.batchSizeBytes = registry.histogram("output.batchSizeBytes", () ->
          new Histogram(new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES)));
      this.batchMessageCountHistogram = registry.histogram("output.batchMessageCount", () ->
          new Histogram(new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES)));
    }

    @Override
    public void run() {
      final List<Message> messageList = Arrays.asList(messages).subList(0, messagesCount.get());
      if (messageList.isEmpty()) {
        clear();
        return;
      }

      short responseCode = ResponseCodes.INTERNAL_SERVER_ERROR;
      int sizeInBytes = CoreUtils.batchSizeInBytes(messageList);
      activeParallelTasks.inc();
      try {
        int checksum = CoreUtils.batchChecksum(messageList);
        Timer.Context uploadTimer = uploadLatency.time();
        handler.writeOutput(sizeInBytes, checksum, messageList);
        uploadTimer.stop();
        responseCode = ResponseCodes.OK;
        updateSuccessMetrics(messageList, sizeInBytes);
      } catch (WriteFailedException | IOException e) {
        logger.log(Level.SEVERE, "Failed to upload batch: ", e);
        responseCode = ResponseCodes.REQUEST_FAILED;
        outputErrorCounter.inc();
      } finally {
        activeParallelTasks.dec();
        clearMessageBuffers(messageList);
        ackMessages(messageList, responseCode);
        clear();
        manager.recycle(Batch.this, isTimeBased);
      }
    }

    private void updateSuccessMetrics(List<Message> messageList, int sizeInBytes) {
      uploadBytesCounter.inc(sizeInBytes);
      uploadMessageCounter.inc(messageList.size());
      batchMessageCountHistogram.update(messageList.size());
      uploadBatchCounter.inc();
      batchSizeBytes.update(sizeInBytes);
    }

    protected void clearMessageBuffers(List<Message> messages) {
      messages.forEach(message -> {
        message.getBuf().release();
      });
    }

    protected void ackMessages(List<Message> messages, short responseCode) {
      Timer.Context ackTimer = ackLatency.time();
      for (Message m : messages) {
        ChannelHandlerContext channelRef = m.getPipelineReference();
        if (channelRef != null) {
          try {
            channelRef.writeAndFlush(new ResponsePacket(m.getClientProtocolVersion(),
                m.getClientRequestId(), RequestType.WRITE, responseCode,
                new WriteResponsePacket()));
          } catch (Exception e2) {
            ackChannelWriteError.inc();
          }
        }
      }
      ackTimer.stop();
    }
  }

}
