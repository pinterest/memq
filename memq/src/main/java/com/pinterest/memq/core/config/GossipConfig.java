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

public class GossipConfig {

  private boolean enabled = false;
  private int port = 9094;
  private long intervalMs = 1000;
  /**
   * How old (wall-clock ms) a peer's last-received gossip message can be
   * before {@code GossipServer.getPeerStates()} treats it as stale and
   * drops it from the map. Default is 10x the default intervalMs: tolerates
   * ~10 consecutive dropped UDP packets before a peer disappears from
   * eviction-target consideration.
   */
  private long peerTtlMs = 10_000L;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public long getIntervalMs() {
    return intervalMs;
  }

  public void setIntervalMs(long intervalMs) {
    this.intervalMs = intervalMs;
  }

  public long getPeerTtlMs() {
    return peerTtlMs;
  }

  public void setPeerTtlMs(long peerTtlMs) {
    this.peerTtlMs = peerTtlMs;
  }
}
