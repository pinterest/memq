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
  /**
   * After the broker forcibly releases slots from a producer (the eviction
   * code path), block any further slot acquisition for the same
   * (producer, topic) for this many seconds. Voluntary EMA-driven release
   * does not arm the cooldown -- only forced eviction does.
   * <p>
   * The default of {@code 60.0} is large enough that:
   * <ol>
   *   <li>after the eviction, the producer's traffic to this broker has
   *       time to actually decrease as the redirected slot's load shifts
   *       to the eviction target, and</li>
   *   <li>the EMA (default {@code emaWindowSeconds = 60}) has time to
   *       decay toward the new lower rate, so the next post-cooldown
   *       acquisition decision is based on fresh data instead of stale
   *       pre-eviction throughput.</li>
   * </ol>
   * Without this gate the broker re-acquires the just-evicted slot the
   * moment its global {@code cooldownSeconds} (10s) plus
   * {@code acquireThresholdSeconds} (30s) elapses, because the EMA still
   * reflects the pre-eviction load -- producing the eviction "flap" we
   * see in production.
   * <p>
   * Set to {@code 0.0} to disable this feature and restore legacy
   * behavior (re-acquisition gated only by global cooldown +
   * acquireThresholdSeconds).
   */
  private double postEvictionCooldownSeconds = 60.0;

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

  public double getPostEvictionCooldownSeconds() {
    return postEvictionCooldownSeconds;
  }

  public void setPostEvictionCooldownSeconds(double postEvictionCooldownSeconds) {
    this.postEvictionCooldownSeconds = postEvictionCooldownSeconds;
  }
}
