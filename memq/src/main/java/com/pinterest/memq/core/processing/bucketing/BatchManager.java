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
import com.pinterest.memq.core.eviction.EvictionManager;
import com.pinterest.memq.core.slot.SlotManager;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ServiceUnavailableException;

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
  private volatile EvictionManager evictionManager;
  private volatile SlotManager slotManager;

  private static final int PAYLOAD_CACHE_SIZE_LIMIT = 10;
  private static final long BYTES_PER_MB = 1024L * 1024L;

  // Broker-wide backpressure: a batch reserves in-flight memory when it is opened
  // (getAvailablePayload) and releases it exactly once when its DispatchTask
  // finishes and the message ByteBufs have been released. The semaphore is shared
  // across every topic on the broker (owned by MemqManager) because direct memory
  // is a broker-wide resource; bounding it prevents downstream (S3 / notification)
  // slowness from letting dispatched-but-not-yet-uploaded batches accumulate and
  // exhaust direct memory. Permits are counted in MB (see BYTES_PER_MB) so the
  // budget can exceed the Integer.MAX_VALUE byte ceiling of a Semaphore.
  private final Semaphore inflightMemoryPermits;
  private final int maxInflightMemoryMb;
  private final int maxBlockMs;

  private Histogram payloadRetries;
  private Timer payloadWriteTime;
  private Timer payloadAcquireTime;
  private Timer payloadValidationTime;
  private Counter payloadCreation;
  private Counter batchMemoryRejected;

  public BatchManager(long sizeDispatchThreshold, int countDispatchThreshold,
                      Duration timeDispatchThreshold,
                      ScheduledExecutorService scheduler, StorageHandler handler,
                      int outputParallelism, Semaphore inflightMemoryPermits,
                      int maxInflightMemoryMb, int maxBlockMs, MetricRegistry registry) {
    this.sizeDispatchThreshold = sizeDispatchThreshold;
    this.countDispatchThreshold = countDispatchThreshold;
    this.timeDispatchThreshold = timeDispatchThreshold;
    this.scheduler = scheduler;
    this.handler = handler;
    this.dispatcher = Executors.newFixedThreadPool(outputParallelism, new MemqProcessingThreadFactory("processing-"));
    this.registry = registry;
    this.recycledBatches = new ArrayBlockingQueue<>(PAYLOAD_CACHE_SIZE_LIMIT);

    this.inflightMemoryPermits = inflightMemoryPermits;
    this.maxInflightMemoryMb = Math.max(1, maxInflightMemoryMb);
    this.maxBlockMs = Math.max(0, maxBlockMs);

    initializeMetrics(registry);
  }

  public boolean reconfigure(long sizeDispatchThreshold, int countDispatchThreshold,
                             Duration timeDispatchThreshold) {
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

  /**
   * Memory a newly opened batch reserves, in MB permits: the (ceil) batch size,
   * clamped to at least 1MB and at most the whole budget so a single batch can
   * always eventually be admitted and the topic is never permanently unwritable.
   */
  private int reservationMb() {
    long mb = (sizeDispatchThreshold + BYTES_PER_MB - 1) / BYTES_PER_MB;
    return (int) Math.max(1, Math.min(mb, maxInflightMemoryMb));
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
    // The broker-wide in-flight memory gauges live on the broker-level (_misc)
    // registry in MemqManager since the budget is shared across topics; this
    // per-topic counter attributes which topic's writes were rejected when the
    // shared budget was exhausted.
    this.batchMemoryRejected = registry.counter("batching.backpressure.memory.rejected");
  }

  public void write(WriteRequestPacket writePacket,
                    long serverRequestId,
                    long clientRequestId,
                    short protocolVersion,
                    ChannelHandlerContext ctx,
                    String producerId) {
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
        if(batch.write(writePacket, serverRequestId, clientRequestId, protocolVersion, ctx, producerId)) {
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
            // Reserve broker-wide in-flight memory for the batch we are about to
            // open. The reservation (in MB) is stored on the batch so its
            // DispatchTask releases the exact amount.
            int reservation = reservationMb();
            acquireBatchPermits(reservation);
            try {
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
              batch.setReservedMemoryMb(reservation);
              currentBatch = batch;
            } catch (Throwable t) {
              // opening the batch failed after acquiring; release to avoid a leak
              releaseBatchPermits(reservation);
              throw t;
            }
          }
          return currentBatch;
        }
      }
      return currentBatch;
    } finally {
      acquirePayloadTimeTimer.stop();
    }
  }

  /**
   * Reserve {@code reservationMb} MB from the shared broker in-flight memory
   * budget. Waits up to {@code maxBlockMs} (0 = non-blocking). On exhaustion the
   * write is rejected with {@link ServiceUnavailableException}, which the broker
   * maps to SERVICE_UNAVAILABLE so producers back off instead of the batch
   * pipeline silently growing until direct memory is exhausted.
   */
  private void acquireBatchPermits(int reservationMb) {
    boolean acquired;
    try {
      acquired = inflightMemoryPermits.tryAcquire(reservationMb, maxBlockMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException("Interrupted while acquiring batch memory permits");
    }
    if (!acquired) {
      batchMemoryRejected.inc();
      throw new ServiceUnavailableException(String.format(
          "Could not acquire %sMB from broker in-flight memory budget in %sms. In-flight: %sMB, Max: %sMB",
          reservationMb, maxBlockMs, maxInflightMemoryMb - inflightMemoryPermits.availablePermits(),
          maxInflightMemoryMb));
    }
  }

  /**
   * Release the memory a batch reserved. Called exactly once per batch lifecycle
   * from the batch's DispatchTask after the message buffers have been released.
   * {@code reservationMb} must equal the amount acquired for that batch (stored on
   * the batch as its reserved memory).
   */
  void releaseBatchPermits(int reservationMb) {
    inflightMemoryPermits.release(reservationMb);
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

  public EvictionManager getEvictionManager() {
    return evictionManager;
  }

  public void setEvictionManager(EvictionManager evictionManager) {
    this.evictionManager = evictionManager;
  }

  public SlotManager getSlotManager() {
    return slotManager;
  }

  public void setSlotManager(SlotManager slotManager) {
    this.slotManager = slotManager;
  }
}
