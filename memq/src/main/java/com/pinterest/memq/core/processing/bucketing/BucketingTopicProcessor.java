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

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.core.Response;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Timer;
import com.pinterest.memq.client.commons.MemqMessageHeader;
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
import com.pinterest.memq.commons.protocol.WriteResponsePacket;
import com.pinterest.memq.commons.storage.StorageHandler;
import com.pinterest.memq.core.processing.Ackable;
import com.pinterest.memq.core.processing.TopicProcessor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

public class BucketingTopicProcessor extends TopicProcessor {
  private static final BatchData EMPTY_BATCH_DATA = new BatchData();
  private static final Logger logger = Logger.getLogger(BucketingTopicProcessor.class.getName());
  private final BatchManager batchManager;
  private final AtomicLong serverRequestIdGenerator = new AtomicLong(
      ThreadLocalRandom.current().nextLong());
  private final ScheduledReporter reporter;
  private final String topicName;
  private final StorageHandler storageHandler;
  private volatile long sizeDispatchThreshold;
  private volatile boolean enableHeaderValidation;
  private ChannelGroup channelGroup;

  private Histogram messageSizeHistogram;
  private Counter invalidHeaderTooLargeCounter;
  private Counter invalidHeaderNegativeCounter;
  private Counter invalidHeaderExceptionCounter;
  private Counter emptyDataCounter;

  public BucketingTopicProcessor(MetricRegistry registry,
                                 TopicConfig topicConfig,
                                 StorageHandler storageHandler,
                                 ScheduledExecutorService timerService,
                                 ScheduledReporter reporter) {
    this.sizeDispatchThreshold = topicConfig.getBatchSizeBytes();
    this.enableHeaderValidation = topicConfig.isEnableServerHeaderValidation();
    this.reporter = reporter;
    this.topicName = topicConfig.getTopic();
    this.storageHandler = storageHandler;
    this.channelGroup = new DefaultChannelGroup(topicName, GlobalEventExecutor.INSTANCE);
    this.batchManager = new BatchManager(sizeDispatchThreshold, topicConfig.getMaxDispatchCount(),
        Duration.ofMillis(topicConfig.getBatchMilliSeconds()), timerService, storageHandler,
        topicConfig.getOutputParallelism(), registry);
    initializeMetrics(registry);
  }

  @Override
  public boolean reconfigure(TopicConfig topicConfig) {
    long newSizeDispatchThreshold = topicConfig.getBatchSizeBytes();
    if (newSizeDispatchThreshold != sizeDispatchThreshold) {
      sizeDispatchThreshold = newSizeDispatchThreshold;
    }

    if (topicConfig.isEnableServerHeaderValidation() != enableHeaderValidation) {
      enableHeaderValidation = topicConfig.isEnableServerHeaderValidation();
    }
    batchManager.reconfigure(sizeDispatchThreshold, topicConfig.getMaxDispatchCount(),
        Duration.ofMillis(topicConfig.getBatchMilliSeconds()));
    storageHandler.reconfigure(topicConfig.getStorageHandlerConfig());
    return true;
  }

  @Override
  public long write(RequestPacket basePacket,
                    WriteRequestPacket writePacket,
                    ChannelHandlerContext ctx) {
    if (writePacket.isDisableAcks()) {
      // send an OK to producer even if ack is disabled
      ctx.writeAndFlush(new ResponsePacket(basePacket.getProtocolVersion(),
          basePacket.getClientRequestId(), basePacket.getRequestType(), ResponseCodes.OK,
          new WriteResponsePacket()));
      // context no longer needed during the write, set it to null so acks won't be sent
      ctx = null;
    }
    messageSizeHistogram.update(writePacket.getDataLength());
    if (writePacket.getDataLength() > sizeDispatchThreshold) {
      writeRejectCounter.inc();
      throw new BadRequestException(
          "Payload too big for " + basePacket.getClientRequestId() + " " + sizeDispatchThreshold);
    }

    if (enableHeaderValidation) {
      validateHeader(ctx, basePacket, writePacket);
    }

    long serverRequestId = serverRequestIdGenerator.getAndIncrement();
    writeCounter.inc();
    if (writePacket.getDataLength() == MemqMessageHeader.getHeaderLength()) {
      // empty data, immediately respond without writing anything
      emptyDataCounter.inc();
      if (ctx != null) {
        ctx.writeAndFlush(
            new ResponsePacket(basePacket.getProtocolVersion(), basePacket.getClientRequestId(),
                basePacket.getRequestType(), ResponseCodes.OK, new WriteResponsePacket()));
      }
      return serverRequestId;
    }

    Timer.Context totalWriteLatencyTimer = totalWriteLatency.time();
    batchManager.write(writePacket, serverRequestId, basePacket.getClientRequestId(),
        basePacket.getProtocolVersion(), ctx);
    totalWriteLatencyTimer.stop();
    return serverRequestId;
  }

  @Override
  public void stopNow() {
    batchManager.stopNow();
  }

  @Override
  public void stopAndAwait() throws InterruptedException {
    if (reporter != null) {
      reporter.close();
    }
    batchManager.stop();
  }

  protected void forceDispatch() {
    batchManager.forceDispatch();
  }

  @Override
  public Ackable getAcker() {
    return null;
  }

  @Override
  public int getRemaining() {
    return 0;
  }

  @Override
  public float getAvailableCapacity() {
    return 0;
  }

  @Override
  public TopicConfig getTopicConfig() {
    return null;
  }

  @Override
  protected void initializeMetrics(MetricRegistry registry) {
    super.initializeMetrics(registry);
    messageSizeHistogram = registry.histogram("tp.message.size",
        () -> new Histogram(new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES)));
    invalidHeaderTooLargeCounter = registry
        .counter("tp.message.invalid.header.message_length_too_large");
    invalidHeaderNegativeCounter = registry
        .counter("tp.message.invalid.header.message_length_negative");
    invalidHeaderExceptionCounter = registry.counter("tp.message.invalid.header.exception");
    emptyDataCounter = registry.counter("tp.message.empty.data");
    registry.gauge("tp.channel.group.size", () -> channelGroup::size);
  }

  protected void validateHeader(ChannelHandlerContext ctx,
                                RequestPacket basePacket,
                                WriteRequestPacket writePacket) {
    MemqMessageHeader header;
    try {
      header = new MemqMessageHeader(writePacket.getData().slice());
    } catch (Exception e) {
      logger.log(
          Level.SEVERE, "Failed to parse message header from: " + getRemoteAddressFromCtx(ctx)
              + ", topic: " + topicName + ", clientRequestId: " + basePacket.getClientRequestId(),
          e);
      invalidHeaderExceptionCounter.inc();
      return;
    }
    if (header.getMessageLength() > sizeDispatchThreshold) {
      logger.severe("Received message with invalid header message length: "
          + header.getMessageLength() + " from " + getRemoteAddressFromCtx(ctx) + ", topic: "
          + topicName + ", clientRequestId: " + basePacket.getClientRequestId());
      invalidHeaderTooLargeCounter.inc();
    } else if (header.getMessageLength() < 0) {
      logger.severe("Received message with invalid header message length: "
          + header.getMessageLength() + " from " + getRemoteAddressFromCtx(ctx) + ", topic: "
          + topicName + ", clientRequestId: " + basePacket.getClientRequestId());
      invalidHeaderNegativeCounter.inc();
    }
  }

  private String getRemoteAddressFromCtx(ChannelHandlerContext ctx) {
    return (ctx != null && ctx.channel() != null) ? ctx.channel().remoteAddress().toString()
        : "n/a";
  }

  @Override
  public void read(RequestPacket requestPacket,
                   ReadRequestPacket readPacket,
                   ChannelHandlerContext ctx) {
    try {
      BatchData data = null;
      if (readPacket.isReadHeaderOnly()) {
        data = storageHandler.fetchHeaderForBatchBuf(readPacket.getNotification());
      } else if (readPacket.getReadIndex().getOffset() == ReadRequestPacket.DISABLE_READ_AT_INDEX) {
        data = storageHandler.fetchBatchStreamForNotificationBuf(readPacket.getNotification());
      } else if (readPacket.getReadIndex().getOffset() > ReadRequestPacket.DISABLE_READ_AT_INDEX) {
        data = storageHandler.fetchMessageAtIndexBuf(readPacket.getNotification(),
            readPacket.getReadIndex());
      } else {
        // unknown request type
        throw new BadRequestException("Invalid read request condition");
      }
      if (data.getDataAsBuf() != null) {
        // if send file is disabled then data will be sent in read packets
        ctx.writeAndFlush(
            new ResponsePacket(RequestType.PROTOCOL_VERSION, requestPacket.getClientRequestId(),
                requestPacket.getRequestType(), ResponseCodes.OK, new ReadResponsePacket(data)));
      } else {
        ctx.write(
            new ResponsePacket(RequestType.PROTOCOL_VERSION, requestPacket.getClientRequestId(),
                requestPacket.getRequestType(), ResponseCodes.OK, new ReadResponsePacket(data)));
        File file = (File) data.getSendFileRef();
        ctx.writeAndFlush(new DefaultFileRegion(file, 0, file.length()));
      }
      logger.fine(() -> "Read completed for topic:" + readPacket.getTopicName() + " "
          + readPacket.getNotification());
    } catch (DataNotFoundException e) {
      logger.fine("Data not found");
      ctx.writeAndFlush(new ResponsePacket(RequestType.PROTOCOL_VERSION,
          requestPacket.getClientRequestId(), requestPacket.getRequestType(), ResponseCodes.NO_DATA,
          new ReadResponsePacket(EMPTY_BATCH_DATA)));
    } catch (IOException e) {
      throw new InternalServerErrorException(Response.serverError().build(), e);
    }
  }

  @Override
  public void registerChannel(Channel channel) {
    channelGroup.add(channel);
  }
}
