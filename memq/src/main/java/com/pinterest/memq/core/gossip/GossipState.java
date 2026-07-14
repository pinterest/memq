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
package com.pinterest.memq.core.gossip;

public class GossipState {

  private final GossipMessage message;
  private final long receiveTimestampMs;

  public GossipState(GossipMessage message, long receiveTimestampMs) {
    this.message = message;
    this.receiveTimestampMs = receiveTimestampMs;
  }

  public GossipMessage getMessage() {
    return message;
  }

  public long getReceiveTimestampMs() {
    return receiveTimestampMs;
  }

  @Override
  public String toString() {
    return "GossipState{message=" + message + ", receiveTimestampMs=" + receiveTimestampMs + "}";
  }
}
