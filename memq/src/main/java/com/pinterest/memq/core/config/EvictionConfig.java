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
  private double consolidationPercentageThreshold = 20.0;
  private double pendingEvictionCooldownSeconds = 10.0;
  private int topNTargets = 3;
  private int topNProducers = 3;
  private int maxConnectionsPerProducer = 3;

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

  /**
   * Cluster-spread deadband within which {@linkplain
   * CurrConnectionsEvictionStrategy steady-state consolidation} is allowed to
   * fire. Expressed as a percentage of {@code totalSlots}. The strategy
   * computes {@code spread = max(freeSlots) - min(freeSlots)} across
   * topic-sharing brokers (including local) and fires consolidation only when
   * {@code spread <= consolidationPercentageThreshold}.
   * <p>
   * Decoupled from {@link #getEvictionPercentageThreshold()} so consolidation
   * can run on a looser deadband than the one that triggers normal eviction.
   * Under sparse producer topologies (low producer-to-broker ratio), normal
   * eviction often gets stuck on the cap-skip filter and the cluster
   * persistently hovers just above the strict eviction deadband -- a strict
   * consolidation deadband would never fire in that regime. A looser
   * consolidation deadband lets over-cap producers gracefully shrink while
   * normal eviction wrestles with the remaining imbalance.
   * <p>
   * <b>Invariant:</b> at runtime the strategy uses
   * {@code max(consolidationPercentageThreshold, evictionPercentageThreshold)}.
   * If misconfigured below the eviction threshold the value is silently
   * clamped up -- consolidation must never be tighter than the band where
   * normal eviction is still actively trying to shed load.
   *
   * @return the configured consolidation spread deadband, in percent.
   */
  public double getConsolidationPercentageThreshold() {
    return consolidationPercentageThreshold;
  }

  public void setConsolidationPercentageThreshold(double consolidationPercentageThreshold) {
    this.consolidationPercentageThreshold = consolidationPercentageThreshold;
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
   * Size of the producer top-N for randomized picking inside the eviction
   * strategy. Within the top-N (sorted ascending by source slots in routine
   * mode, descending in drain mode) the strategy picks uniformly at random.
   * Setting this to 1 makes producer selection deterministic. Larger values
   * spread evictions across roughly equivalent producers, breaking the
   * deterministic two-body cycles that produced the connection "harmonic
   * dance" against a single victim broker.
   *
   * @return the configured top-N producer pool size.
   */
  public int getTopNProducers() {
    return topNProducers;
  }

  public void setTopNProducers(int topNProducers) {
    this.topNProducers = topNProducers;
  }

  /**
   * Cluster-wide cap on how many distinct broker connections a single producer
   * should hold. This is the broker-side authority for what was previously
   * enforced on the producer as {@code maxConnections}. The strategy uses it
   * two ways:
   * <ul>
   *   <li><b>Routine mode (drain latch not engaged):</b> if an eviction would
   *       push a producer above this cap (it is already at the cap, the chosen
   *       target is not in its connection set, and it would still own slots on
   *       this broker after the eviction), the strategy refuses the move so
   *       the producer is not forced to drop one of its existing connections.
   *       It instead waits for a "graceful swap" — a producer whose source-slot
   *       count would naturally drop to zero on eviction, releasing the source
   *       connection without ever exceeding the cap.</li>
   *   <li><b>Drain mode (drain latch engaged):</b> the broker is saturated and
   *       must shed load even at the cost of a connection drop, so the cap
   *       check is relaxed and the strategy picks the heaviest producer in
   *       the top-N regardless.</li>
   * </ul>
   * A value {@code <= 0} disables the cap entirely.
   *
   * @return the configured per-producer connection cap.
   */
  public int getMaxConnectionsPerProducer() {
    return maxConnectionsPerProducer;
  }

  public void setMaxConnectionsPerProducer(int maxConnectionsPerProducer) {
    this.maxConnectionsPerProducer = maxConnectionsPerProducer;
  }

}
