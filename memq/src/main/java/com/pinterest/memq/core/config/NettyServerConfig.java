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
package com.pinterest.memq.core.config;

import com.pinterest.memq.commons.config.SSLConfig;

public class NettyServerConfig {

  private int maxFrameByteLength = 4 * 1024 * 1024;
  private short port = 9092;
  private int numEventLoopThreads = 8;
  private boolean enableEpoll = false;
  private int maxBrokerInputTrafficMbPerSec = -1; // -1 means no traffic limit by default
  private int brokerInputTrafficShapingCheckIntervalMs = 1000; // 1 second by default
  private int brokerInputTrafficShapingMetricsReportIntervalSec = 60; // 1 minute by default

  // Broker-wide backpressure on in-flight (dispatched-but-not-yet-uploaded) batch
  // memory. All topics on the broker share one budget because direct memory is a
  // broker-wide resource; bounding it prevents downstream (S3 / notification)
  // slowness from letting the batch pipeline exhaust direct memory and OOM.
  //
  // <= 0 means "auto": 80% of the JVM's max direct memory (Netty's
  // PlatformDependent.maxDirectMemory()). Applied at broker startup.
  private long maxInflightBatchBytes = 0;

  // Max time (ms) a write waits for in-flight batch memory before being rejected
  // with SERVICE_UNAVAILABLE. Writes run on the Netty event loop, so this defaults
  // to 0 (non-blocking, reject immediately) to avoid stalling other connections.
  private int maxBackpressureBlockMs = 0;

  // SSL
  private SSLConfig sslConfig;

  public int getBrokerInputTrafficShapingMetricsReportIntervalSec() {
    return brokerInputTrafficShapingMetricsReportIntervalSec;
  }

  public void setBrokerInputTrafficShapingMetricsReportIntervalSec(
      int brokerInputTrafficShapingMetricsReportIntervalSec) {
      this.brokerInputTrafficShapingMetricsReportIntervalSec = brokerInputTrafficShapingMetricsReportIntervalSec;
  }

  public int getMaxBrokerInputTrafficMbPerSec() {
    return maxBrokerInputTrafficMbPerSec;
  }

  public void setMaxBrokerInputTrafficMbPerSec(int maxBrokerInputTrafficMbPerSec) {
    this.maxBrokerInputTrafficMbPerSec = maxBrokerInputTrafficMbPerSec;
  }

  public int getBrokerInputTrafficShapingCheckIntervalMs() {
      return brokerInputTrafficShapingCheckIntervalMs;
  }

  public void setBrokerInputTrafficShapingCheckIntervalMs(int brokerInputTrafficShapingCheckIntervalMs) {
      this.brokerInputTrafficShapingCheckIntervalMs = brokerInputTrafficShapingCheckIntervalMs;
  }

  public int getMaxFrameByteLength() {
    return maxFrameByteLength;
  }

  public void setMaxFrameByteLength(int maxFrameByteLength) {
    this.maxFrameByteLength = maxFrameByteLength;
  }

  public short getPort() {
    return port;
  }

  public void setPort(short port) {
    this.port = port;
  }

  public int getNumEventLoopThreads() {
    return numEventLoopThreads;
  }

  public void setNumEventLoopThreads(int numEventLoopThreads) {
    this.numEventLoopThreads = numEventLoopThreads;
  }

  public long getMaxInflightBatchBytes() {
    return maxInflightBatchBytes;
  }

  public void setMaxInflightBatchBytes(long maxInflightBatchBytes) {
    this.maxInflightBatchBytes = maxInflightBatchBytes;
  }

  public int getMaxBackpressureBlockMs() {
    return maxBackpressureBlockMs;
  }

  public void setMaxBackpressureBlockMs(int maxBackpressureBlockMs) {
    this.maxBackpressureBlockMs = maxBackpressureBlockMs;
  }

  public SSLConfig getSslConfig() {
    return sslConfig;
  }

  public void setSslConfig(SSLConfig sslConfig) {
    this.sslConfig = sslConfig;
  }

  public boolean isEnableEpoll() {
    return enableEpoll;
  }

  public void setEnableEpoll(boolean enableEpoll) {
    this.enableEpoll = enableEpoll;
  }

}
