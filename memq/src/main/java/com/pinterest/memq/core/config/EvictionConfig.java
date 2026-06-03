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

import com.pinterest.memq.core.eviction.CurrConnectionsEvictionStrategy;

public class EvictionConfig {

  private boolean enabled = false;
  private String strategyClass = CurrConnectionsEvictionStrategy.class.getName(); // default strategy
  private double intervalSeconds = 5.0;
  private double evictionPercentageThreshold = 10.0;
  private double pendingEvictionCooldownSeconds = 10.0;
  private int topNTargets = 3;
  private int heavyProducerSlotMargin = 2;
  private boolean preferConnectedTarget = true;

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

  /**
   * How many more slots an unconnected producer must hold than the heaviest
   * producer already connected to the eviction target before the strategy
   * will prefer it (and incur a new producer-side connection). This keeps the
   * cheap "evict a producer already connected to the target" behavior for
   * roughly balanced producers, while still letting the strategy target a
   * materially heavier producer -- moving the heavy producer is what actually
   * drains a saturated broker, since a backpressured heavy producer otherwise
   * just reabsorbs whatever slot a lighter eviction frees. A very large value
   * restores the old "always prefer connected" behavior.
   */
  public int getHeavyProducerSlotMargin() {
    return heavyProducerSlotMargin;
  }

  public void setHeavyProducerSlotMargin(int heavyProducerSlotMargin) {
    this.heavyProducerSlotMargin = heavyProducerSlotMargin;
  }

  /**
   * When true (default), eviction prefers a target broker that the producer it
   * would evict is already connected to, so the producer does not have to drop
   * an existing connection to honor its client-side {@code maxConnections} cap.
   * Broker balance remains the hard gate -- only already-valid candidate targets
   * are considered, and a swap-requiring target is still chosen when no
   * swap-free target relieves congestion. A large {@code heavyProducerSlotMargin}
   * combined with this restores connection-affinity-first behavior.
   */
  public boolean isPreferConnectedTarget() {
    return preferConnectedTarget;
  }

  public void setPreferConnectedTarget(boolean preferConnectedTarget) {
    this.preferConnectedTarget = preferConnectedTarget;
  }

}
