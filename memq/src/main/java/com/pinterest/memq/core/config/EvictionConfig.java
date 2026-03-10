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

public class EvictionConfig {

  private boolean enabled = false;
  private String strategyClass = "com.pinterest.memq.core.eviction.CurrConnectionsEvictionStrategy";
  private double intervalSeconds = 5.0;
  private double evictionPercentageThreshold = 10.0;
  private double pendingEvictionCooldownSeconds = 10.0;
  private int topNTargets = 3;
  private int brokerPort = 9092;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getStrategyClass() {
    return strategyClass;
  }

  public void setStrategyClass(String strategyClass) {
    this.strategyClass = strategyClass;
  }

  public double getIntervalSeconds() {
    return intervalSeconds;
  }

  public void setIntervalSeconds(double intervalSeconds) {
    this.intervalSeconds = intervalSeconds;
  }

  public double getEvictionPercentageThreshold() {
    return evictionPercentageThreshold;
  }

  public void setEvictionPercentageThreshold(double evictionPercentageThreshold) {
    this.evictionPercentageThreshold = evictionPercentageThreshold;
  }

  public double getPendingEvictionCooldownSeconds() {
    return pendingEvictionCooldownSeconds;
  }

  public void setPendingEvictionCooldownSeconds(double pendingEvictionCooldownSeconds) {
    this.pendingEvictionCooldownSeconds = pendingEvictionCooldownSeconds;
  }

  public int getTopNTargets() {
    return topNTargets;
  }

  public void setTopNTargets(int topNTargets) {
    this.topNTargets = topNTargets;
  }

  public int getBrokerPort() {
    return brokerPort;
  }

  public void setBrokerPort(int brokerPort) {
    this.brokerPort = brokerPort;
  }
}
