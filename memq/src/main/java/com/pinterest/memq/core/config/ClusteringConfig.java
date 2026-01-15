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

public class ClusteringConfig {

  private String zookeeperConnectionString;
  private int batchMultiplier = 3;
  private boolean enableLeaderSelector = true;
  private boolean enableBalancer = true;
  private boolean enableLocalAssigner = true;
  private boolean addBootstrapTopics = true;
  private boolean enableExpiration = true;
  private boolean enableAssignments = true;
  private long maxIdleMs = 5 * 60 * 1000; // 5 minutes

  public boolean isAddBootstrapTopics() {
    return addBootstrapTopics;
  }

  public void setAddBootstrapTopics(boolean addBootstrapTopics) {
    this.addBootstrapTopics = addBootstrapTopics;
  }

  public String getZookeeperConnectionString() {
    return zookeeperConnectionString;
  }

  public void setZookeeperConnectionString(String zookeeperConnectionString) {
    this.zookeeperConnectionString = zookeeperConnectionString;
  }

  public int getBatchMultiplier() {
    return batchMultiplier;
  }

  public void setBatchMultiplier(int batchMultiplier) {
    this.batchMultiplier = batchMultiplier;
  }

  public boolean isEnableLeaderSelector() {
    return enableLeaderSelector;
  }

  public void setEnableLeaderSelector(boolean enableLeaderSelector) {
    this.enableLeaderSelector = enableLeaderSelector;
  }

  public boolean isEnableBalancer() {
    return enableBalancer;
  }

  public void setEnableBalancer(boolean enableBalancer) {
    this.enableBalancer = enableBalancer;
  }

  public boolean isEnableLocalAssigner() {
    return enableLocalAssigner;
  }

  public void setEnableLocalAssigner(boolean enableLocalAssigner) {
    this.enableLocalAssigner = enableLocalAssigner;
  }

  public boolean isEnableExpiration() {
    return enableExpiration;
  }

  public void setEnableExpiration(boolean enableExpiration) {
    this.enableExpiration = enableExpiration;
  }

  public boolean isEnableAssignments() {
    return enableAssignments;
  }

  public void setEnableAssignments(boolean enableAssignments) {
    this.enableAssignments = enableAssignments;
  }

  public long getMaxIdleMs() {
    return maxIdleMs;
  }

  public void setMaxIdleMs(long maxIdleMs) {
    this.maxIdleMs = maxIdleMs;
  }
}
