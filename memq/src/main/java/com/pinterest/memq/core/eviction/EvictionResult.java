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
package com.pinterest.memq.core.eviction;

public class EvictionResult {

  private final String pid;
  private final String targetBrokerIp;
  private final int numSlotsToEvict;

  public EvictionResult(String pid, String targetBrokerIp, int numSlotsToEvict) {
    this.pid = pid;
    this.targetBrokerIp = targetBrokerIp;
    this.numSlotsToEvict = numSlotsToEvict;
  }

  public String getPid() {
    return pid;
  }

  public String getTargetBrokerIp() {
    return targetBrokerIp;
  }

  public int getNumSlotsToEvict() {
    return numSlotsToEvict;
  }

  @Override
  public String toString() {
    return "EvictionResult{pid='" + pid + "', targetBrokerIp='" + targetBrokerIp
        + "', numSlotsToEvict=" + numSlotsToEvict + "}";
  }
}
