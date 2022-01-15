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

import com.pinterest.memq.commons.protocol.WriteRequestPacket;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.core.commons.MemqProcessingThreadFactory;
import com.pinterest.memq.core.utils.MiscUtils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import javax.ws.rs.BadRequestException;

public class BatchManager {
  private volatile Batch currentBatch;
  private final Queue<Batch> recycledBatches;
  private final ScheduledExecutorService scheduler;
  private final ExecutorService dispatcher;
  private volatile Duration timeDispatchThreshold;
  private volatile long sizeDispatchThreshold;
  private volatile int countDispatchThreshold;
  private final StorageHandler handler;
  private final MetricRegistry registry;

  private static final int PAYLOAD_CACHE_SIZE_LIMIT = 10;

  private Histogram payloadRetries;
  private Timer payloadWriteTime;
  private Timer payloadAcquireTime;
  private Timer payloadValidationTime;
  private Counter payloadCreation;

  public BatchManager(long sizeDispatchThreshold, int countDispatchThreshold,
                      Duration timeDispatchThreshold,
                      ScheduledExecutorService scheduler, StorageHandler handler,
                      int outputParallelism, MetricRegistry registry) {
    this.sizeDispatchThreshold = sizeDispatchThreshold;
    this.countDispatchThreshold = countDispatchThreshold;
    this.timeDispatchThreshold = timeDispatchThreshold;
    this.scheduler = scheduler;
    this.handler = handler;
    this.dispatcher = Executors.newFixedThreadPool(outputParallelism, new MemqProcessingThreadFactory("processing-"));
    this.registry = registry;
    this.recycledBatches = new ArrayBlockingQueue<>(PAYLOAD_CACHE_SIZE_LIMIT);

    initializeMetrics(registry);
  }

  public boolean reconfigure(long sizeDispatchThreshold, int countDispatchThreshold, Duration timeDispatchThreshold) {
    if (sizeDispatchThreshold != this.sizeDispatchThreshold) {
      this.sizeDispatchThreshold = sizeDispatchThreshold;
    }
    if (countDispatchThreshold != this.countDispatchThreshold) {
      this.countDispatchThreshold = countDispatchThreshold;
    }
    if (!timeDispatchThreshold.equals(this.timeDispatchThreshold)) {
      this.timeDispatchThreshold = timeDispatchThreshold;
    }

    // the batches will be updated during batch.reset(sizeDispatchThreshold, countDispatchThreshold, timeDispatchThreshold)
    return true;
  }

  protected void initializeMetrics(MetricRegistry registry) {
    this.payloadRetries = registry.histogram("batching.payload.retries");
    registry.gauge("batching.payload.cache.size", () ->
        (Gauge<Integer>) recycledBatches::size
    );
    this.payloadCreation = registry.counter("batching.payload.creation");
    this.payloadWriteTime = MiscUtils.oneMinuteWindowTimer(registry,"batching.payload.write");
    this.payloadAcquireTime = MiscUtils.oneMinuteWindowTimer(registry, "batching.payload.acquire");
    this.payloadValidationTime = MiscUtils.oneMinuteWindowTimer(registry, "batching.payload.validate");
  }

  public void write(WriteRequestPacket writePacket,
                    long serverRequestId,
                    long clientRequestId,
                    short protocolVersion,
                    ChannelHandlerContext ctx) {
    if (writePacket.isChecksumExists()) {
      Timer.Context payloadValidationTimer = payloadValidationTime.time();
      try {
        validateChecksumAndRejectMessage(writePacket.getData().slice(), writePacket.getChecksum());
      } catch (Exception e) {
        throw new BadRequestException(clientRequestId + " : " + e.getMessage());
      } finally {
        payloadValidationTimer.stop();
      }
    }
    int retries = 0;
    Batch batch = getAvailablePayload();
    Timer.Context payloadWriteTimeTimer = payloadWriteTime.time();
    try {
      while (batch != null) {
        if(batch.write(writePacket, serverRequestId, clientRequestId, protocolVersion, ctx)) {
          payloadRetries.update(retries);
          return;
        } else {
          batch = getAvailablePayload();
          retries++;
        }
      }
      throw new BadRequestException(
          "Failed to write message " + clientRequestId + " : no available payload"
      );
    } finally {
      payloadWriteTimeTimer.stop();
    }
  }

  protected Batch getAvailablePayload() {
    Timer.Context acquirePayloadTimeTimer = payloadAcquireTime.time();
    try {
      if (currentBatch == null || !currentBatch.isAvailable()) {
        synchronized (this) {
          if (currentBatch == null || !currentBatch.isAvailable()) {
            Batch batch = recycledBatches.poll();
            if (batch == null) {
              batch = new Batch(
                  this,
                  countDispatchThreshold,
                  sizeDispatchThreshold,
                  timeDispatchThreshold,
                  scheduler,
                  dispatcher,
                  handler,
                  registry
              );
              payloadCreation.inc();
            }
            batch.reset(sizeDispatchThreshold, countDispatchThreshold, timeDispatchThreshold); // reset thresholds in case configs are updated
            currentBatch = batch;
          }
          return currentBatch;
        }
      }
      return currentBatch;
    } finally {
      acquirePayloadTimeTimer.stop();
    }
  }

  public void recycle(Batch p, boolean isTimeBased) {
    recycledBatches.offer(p);
  }

  private void validateChecksumAndRejectMessage(ByteBuf checksumBuffer,
                                                int payloadChecksum) throws Exception {
    ByteBuffer byteBuffer = checksumBuffer.nioBuffer();
    CRC32 crc32 = new CRC32();
    crc32.update(byteBuffer);
    long localChecksum = (int) crc32.getValue();
    if (localChecksum != payloadChecksum) {
      throw new Exception(
          "Invalid checksum - header: " + payloadChecksum + " payload: " + localChecksum);
    }
  }

  public void stopNow() {
    dispatcher.shutdownNow();
  }

  public void stop() throws InterruptedException {
    dispatcher.shutdown();
    dispatcher.awaitTermination(100, TimeUnit.SECONDS);
    handler.closeWriter();
  }

  public void forceDispatch() {
    currentBatch.seal();
    currentBatch.dispatch(false);
  }
}
