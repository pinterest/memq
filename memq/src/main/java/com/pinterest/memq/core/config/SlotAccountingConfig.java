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

public class SlotAccountingConfig {

  private boolean enabled = false;
  private double slotSizeMbps = 10.0;
  private double slotOverhead = 0.2;
  private double acquireThresholdSeconds = 30.0;
  private double releaseThresholdSeconds = 30.0;
  private double cooldownSeconds = 10.0;
  private double emaWindowSeconds = 60.0;
  private long tickIntervalMs = 1000;
  private long idleProducerTimeoutMs = 300_000;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public double getSlotSizeMbps() {
    return slotSizeMbps;
  }

  public void setSlotSizeMbps(double slotSizeMbps) {
    this.slotSizeMbps = slotSizeMbps;
  }

  public double getSlotOverhead() {
    return slotOverhead;
  }

  public void setSlotOverhead(double slotOverhead) {
    this.slotOverhead = slotOverhead;
  }

  public double getAcquireThresholdSeconds() {
    return acquireThresholdSeconds;
  }

  public void setAcquireThresholdSeconds(double acquireThresholdSeconds) {
    this.acquireThresholdSeconds = acquireThresholdSeconds;
  }

  public double getReleaseThresholdSeconds() {
    return releaseThresholdSeconds;
  }

  public void setReleaseThresholdSeconds(double releaseThresholdSeconds) {
    this.releaseThresholdSeconds = releaseThresholdSeconds;
  }

  public double getCooldownSeconds() {
    return cooldownSeconds;
  }

  public void setCooldownSeconds(double cooldownSeconds) {
    this.cooldownSeconds = cooldownSeconds;
  }

  public double getEmaWindowSeconds() {
    return emaWindowSeconds;
  }

  public void setEmaWindowSeconds(double emaWindowSeconds) {
    this.emaWindowSeconds = emaWindowSeconds;
  }

  public long getTickIntervalMs() {
    return tickIntervalMs;
  }

  public void setTickIntervalMs(long tickIntervalMs) {
    this.tickIntervalMs = tickIntervalMs;
  }

  public long getIdleProducerTimeoutMs() {
    return idleProducerTimeoutMs;
  }

  public void setIdleProducerTimeoutMs(long idleProducerTimeoutMs) {
    this.idleProducerTimeoutMs = idleProducerTimeoutMs;
  }
}
