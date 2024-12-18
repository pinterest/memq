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
