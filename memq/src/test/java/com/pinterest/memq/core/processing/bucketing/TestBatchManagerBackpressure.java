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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import javax.ws.rs.ServiceUnavailableException;

import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.WriteRequestPacket;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.commons.storage.WriteFailedException;
import com.pinterest.memq.core.commons.Message;
import com.pinterest.memq.core.utils.DaemonThreadFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * Tests the backpressure caps ported from the producer's {@code RequestManager}
 * into {@link BatchManager}: the count and memory semaphores must reject writes
 * with {@link ServiceUnavailableException} when in-flight batches saturate the
 * caps, and must release the permits (and the message {@link ByteBuf}s) exactly
 * once when the batch finishes.
 */
public class TestBatchManagerBackpressure {

  private static final long MB = 1024 * 1024;

  @Test
  public void testCountBackpressureRejectsAndReleases() throws Exception {
    MetricRegistry registry = new MetricRegistry();
    CountDownLatch release = new CountDownLatch(1);
    BlockingStorageHandler handler = new BlockingStorageHandler(release);
    ScheduledExecutorService scheduler =
        Executors.newScheduledThreadPool(1, new DaemonThreadFactory());

    // maxDispatchCount=1 => each message becomes its own batch and is dispatched
    // immediately; the blocking handler keeps those batches (and their permits)
    // in flight. maxInflightBatches=2 with a large memory cap so count binds.
    BatchManager bm = new BatchManager(10 * MB, 1, Duration.ofMinutes(10), scheduler, handler,
        4, 2, 512 * MB, 0, registry);

    ByteBuf b1 = payload("one");
    ByteBuf b2 = payload("two");
    ByteBuf b3 = payload("three");
    try {
      write(bm, b1);
      write(bm, b2);
      waitFor(() -> handler.inFlight.get() == 2, 5000);

      // both permits are held by the two blocked uploads -> third write rejected
      try {
        write(bm, b3);
        fail("expected ServiceUnavailableException when batch count cap is hit");
      } catch (ServiceUnavailableException expected) {
        // expected
      }
      assertEquals(1, registry.counter("batching.backpressure.count.rejected").getCount());
      // a rejected write must not retain the caller's buffer
      assertEquals("rejected write must not retain the caller buffer", 1, b3.refCnt());
      assertEquals(2, inflightBatches(registry));

      // let the blocked uploads finish -> permits and message buffers released
      release.countDown();
      waitFor(() -> handler.uploads.get() == 2, 5000);
      waitFor(() -> inflightBatches(registry) == 0, 5000);

      // the retained slices were released, leaving only the caller's reference
      assertEquals(1, b1.refCnt());
      assertEquals(1, b2.refCnt());

      // capacity is available again
      ByteBuf b4 = payload("four");
      write(bm, b4);
      waitFor(() -> handler.uploads.get() == 3, 5000);
      waitFor(() -> inflightBatches(registry) == 0, 5000);
      assertEquals(1, b4.refCnt());
      b4.release();
    } finally {
      b1.release();
      b2.release();
      b3.release();
      bm.stop();
      scheduler.shutdownNow();
    }
  }

  @Test
  public void testMemoryBackpressureRejects() throws Exception {
    MetricRegistry registry = new MetricRegistry();
    CountDownLatch release = new CountDownLatch(1);
    BlockingStorageHandler handler = new BlockingStorageHandler(release);
    ScheduledExecutorService scheduler =
        Executors.newScheduledThreadPool(1, new DaemonThreadFactory());

    // per-batch reservation == sizeDispatchThreshold (1MB), memory cap == 1MB so
    // only ONE batch fits regardless of the (large) count cap => memory binds.
    BatchManager bm = new BatchManager(1 * MB, 1, Duration.ofMinutes(10), scheduler, handler,
        4, 100, 1 * MB, 0, registry);

    ByteBuf b1 = payload("one");
    ByteBuf b2 = payload("two");
    try {
      write(bm, b1);
      waitFor(() -> handler.inFlight.get() == 1, 5000);

      try {
        write(bm, b2);
        fail("expected ServiceUnavailableException when batch memory cap is hit");
      } catch (ServiceUnavailableException expected) {
        // expected
      }
      assertEquals(1, registry.counter("batching.backpressure.memory.rejected").getCount());
      assertEquals(0, registry.counter("batching.backpressure.count.rejected").getCount());
      assertEquals(1, b2.refCnt());

      release.countDown();
      waitFor(() -> handler.uploads.get() == 1, 5000);
      waitFor(() -> inflightBatchBytes(registry) == 0, 5000);
      assertEquals(1, b1.refCnt());
    } finally {
      b1.release();
      b2.release();
      bm.stop();
      scheduler.shutdownNow();
    }
  }

  @Test
  public void testEmptyBatchTimeDispatchReleasesPermits() throws Exception {
    MetricRegistry registry = new MetricRegistry();
    AtomicInteger uploads = new AtomicInteger();
    StorageHandler handler = new StorageHandler() {
      @Override
      public void writeOutput(int sizeInBytes, int checksum, List<Message> messages) {
        uploads.incrementAndGet();
      }

      @Override
      public String getReadUrl() {
        return null;
      }
    };
    ScheduledExecutorService scheduler =
        Executors.newScheduledThreadPool(1, new DaemonThreadFactory());

    BatchManager bm = new BatchManager(1 * MB, 100, Duration.ofMillis(150), scheduler, handler,
        2, 1, 1 * MB, 0, registry);
    try {
      // open a batch without writing anything to it
      Batch batch = bm.getAvailablePayload();
      assertNotNull(batch);
      assertEquals("opening a batch must consume one permit", 1, inflightBatches(registry));

      // the empty batch is time-dispatched and must release its permit even
      // though it uploads nothing (DispatchTask empty path)
      waitFor(() -> inflightBatches(registry) == 0, 5000);
      assertEquals("empty batch must not be uploaded", 0, uploads.get());
    } finally {
      bm.stop();
      scheduler.shutdownNow();
    }
  }

  private static int inflightBatches(MetricRegistry registry) {
    return (Integer) registry.getGauges().get("batching.batches.inflight").getValue();
  }

  private static int inflightBatchBytes(MetricRegistry registry) {
    return (Integer) registry.getGauges().get("batching.batches.memory.inflight").getValue();
  }

  private static void write(BatchManager bm, ByteBuf buf) {
    WriteRequestPacket packet =
        new WriteRequestPacket(true, "test".getBytes(), false, 0, buf);
    bm.write(packet, 1L, 1L, RequestType.PROTOCOL_VERSION, newCtx(), null);
  }

  private static ByteBuf payload(String s) {
    ByteBuf buf = Unpooled.buffer();
    buf.writeBytes(s.getBytes());
    return buf;
  }

  private static ChannelHandlerContext newCtx() {
    Channel ch = new EmbeddedChannel();
    ch.pipeline().addLast(new ChannelDuplexHandler());
    return ch.pipeline().firstContext();
  }

  private static void waitFor(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(10);
    }
    assertTrue("condition not met within " + timeoutMs + "ms", condition.getAsBoolean());
  }

  private static class BlockingStorageHandler implements StorageHandler {
    private final CountDownLatch release;
    final AtomicInteger uploads = new AtomicInteger();
    final AtomicInteger inFlight = new AtomicInteger();

    BlockingStorageHandler(CountDownLatch release) {
      this.release = release;
    }

    @Override
    public void writeOutput(int sizeInBytes, int checksum, List<Message> messages)
        throws WriteFailedException {
      inFlight.incrementAndGet();
      try {
        release.await(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      inFlight.decrementAndGet();
      uploads.incrementAndGet();
    }

    @Override
    public String getReadUrl() {
      return null;
    }
  }
}
