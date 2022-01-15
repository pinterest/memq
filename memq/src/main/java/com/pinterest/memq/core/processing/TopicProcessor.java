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
package com.pinterest.memq.core.processing;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.pinterest.memq.commons.protocol.ReadRequestPacket;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.protocol.WriteRequestPacket;
import com.pinterest.memq.core.utils.MiscUtils;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

public abstract class TopicProcessor {

  protected volatile TopicProcessorState state = TopicProcessorState.STOPPED;
  protected Timer totalWriteLatency;
  protected Timer slotWaitLatency;
  protected Counter checkProcessedCounter;
  protected Counter writeCounter;
  protected Counter writeRejectCounter;
  protected Counter writeErrorCounter;
  protected Timer tickerLatency;
  protected Counter processedCounter;
  protected Counter failedCounter;
  protected Counter pendingCounter;
  protected Counter slotCheckCounter;

  protected void initializeMetrics(MetricRegistry registry) {
    totalWriteLatency = MiscUtils.oneMinuteWindowTimer(registry, "tp.totalWriteLatency");
    slotWaitLatency = MiscUtils.oneMinuteWindowTimer(registry, "tp.slotWaitLatency");
    tickerLatency = MiscUtils.oneMinuteWindowTimer(registry, "tp.tickPublishLatency");
    writeCounter = registry.counter("tp.writeCounter");
    writeErrorCounter = registry.counter("tp.writeErrors");
    writeRejectCounter = registry.counter("tp.writeRejectCounter");
    checkProcessedCounter = registry.counter("tp.checkProcessedCounter");
    processedCounter = registry.counter("tp.ackProcessed");
    failedCounter = registry.counter("tp.ackFailed");
    pendingCounter = registry.counter("tp.ackPending");
    slotCheckCounter = registry.counter("tp.slotCheckCounter");
  }

  public boolean reconfigure(TopicConfig topicConfig) {
    return true;
  }

  public abstract long write(RequestPacket basePacket,
                             WriteRequestPacket writePacket,
                             ChannelHandlerContext ctx);

  public abstract void stopNow();

  public abstract void stopAndAwait() throws InterruptedException;

  public TopicProcessorState getState() {
    return state;
  }

  public abstract Ackable getAcker();

  public abstract int getRemaining();

  public abstract float getAvailableCapacity();

  public abstract TopicConfig getTopicConfig();

  public void registerChannel(Channel channel) {

  }

  public abstract void read(RequestPacket requestPacket,
                            ReadRequestPacket readPacket,
                            ChannelHandlerContext ctx);

}
