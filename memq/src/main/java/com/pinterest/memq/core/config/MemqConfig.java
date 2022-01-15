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

import com.google.common.collect.ImmutableList;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.commons.protocol.Broker.BrokerType;

import io.dropwizard.Configuration;
import io.dropwizard.request.logging.LogbackAccessRequestLogFactory;
import io.dropwizard.server.DefaultServerFactory;

public class MemqConfig extends Configuration {

  public MemqConfig() {
    DefaultServerFactory defaultServerFactory = (DefaultServerFactory) getServerFactory();

    // Note that if someone explicitly enables gzip in the Dropwizard config YAML
    // then settings will be over-ruled causing the UI to stop working

    // Disable HTTP request logging
    LogbackAccessRequestLogFactory accessRequestLogFactory = new LogbackAccessRequestLogFactory();
    accessRequestLogFactory.setAppenders(ImmutableList.of());
    defaultServerFactory.setRequestLogFactory(accessRequestLogFactory);
  }

  private int defaultBufferSize = 1024 * 1024;

  private int defaultRingBufferSize = 128;

  private int defaultSlotTimeout = 10_000;

  private int cleanerThreadCount = 2;

  private OpenTsdbConfiguration openTsdbConfig = null;

  private String[] storageHandlerPackageNames = null;

  private double nodeTrafficLimitInMB = 100;

  private double awsUploadRatePerStreamInMB = 3;

  private boolean cluster = false;

  private boolean resetEnabled = false;
  
  private BrokerType brokerType = BrokerType.WRITE;
  
  private NettyServerConfig nettyServerConfig = new NettyServerConfig();

  private AuthorizerConfig authorizerConfig = null;

  private String environmentProvider = EC2EnvironmentProvider.class.getCanonicalName();
  
  private TopicConfig[] topicConfig = null;
  
  private ClusteringConfig clusteringConfig = null;
  
  private String topicCacheFile = "/tmp/.memq_topic_cache";

  public String getTopicCacheFile() {
    return topicCacheFile;
  }

  public void setTopicCacheFile(String topicCacheFile) {
    this.topicCacheFile = topicCacheFile;
  }

  public ClusteringConfig getClusteringConfig() {
    return clusteringConfig;
  }

  public void setClusteringConfig(ClusteringConfig clusteringConfig) {
    this.clusteringConfig = clusteringConfig;
  }

  public String getEnvironmentProvider() {
    return environmentProvider;
  }

  public void setEnvironmentProvider(String environmentProvider) {
    this.environmentProvider = environmentProvider;
  }

  public TopicConfig[] getTopicConfig() {
    return topicConfig;
  }

  public void setTopicConfig(TopicConfig[] topicConfig) {
    this.topicConfig = topicConfig;
  }

  /**
   * @return the defaultBufferSize
   */
  public int getDefaultBufferSize() {
    return defaultBufferSize;
  }

  /**
   * @param defaultBufferSize the defaultBufferSize to set
   */
  public void setDefaultBufferSize(int defaultBufferSize) {
    this.defaultBufferSize = defaultBufferSize;
  }

  /**
   * @return the defaultRingBufferSize
   */
  public int getDefaultRingBufferSize() {
    return defaultRingBufferSize;
  }

  /**
   * @param defaultRingBufferSize the defaultRingBufferSize to set
   */
  public void setDefaultRingBufferSize(int defaultRingBufferSize) {
    this.defaultRingBufferSize = defaultRingBufferSize;
  }

  /**
   * @return the cleanerThreadCount
   */
  public int getCleanerThreadCount() {
    return cleanerThreadCount;
  }

  /**
   * @param cleanerThreadCount the cleanerThreadCount to set
   */
  public void setCleanerThreadCount(int cleanerThreadCount) {
    this.cleanerThreadCount = cleanerThreadCount;
  }

  /**
   * @return the defaultSlotTimeout
   */
  public int getDefaultSlotTimeout() {
    return defaultSlotTimeout;
  }

  /**
   * @param defaultSlotTimeout the defaultSlotTimeout to set
   */
  public void setDefaultSlotTimeout(int defaultSlotTimeout) {
    this.defaultSlotTimeout = defaultSlotTimeout;
  }

  /**
   * @return the openTsdbConfig
   */
  public OpenTsdbConfiguration getOpenTsdbConfig() {
    return openTsdbConfig;
  }

  /**
   * @param openTsdbConfig the openTsdbConfig to set
   */
  public void setOpenTsdbConfig(OpenTsdbConfiguration openTsdbConfig) {
    this.openTsdbConfig = openTsdbConfig;
  }

  /**
   * @return the outputHandlerPackageNames
   */
  public String[] getStorageHandlerPackageNames() {
    return storageHandlerPackageNames;
  }

  /**
   * @param outputHandlerPackageNames the outputHandlerPackageNames to set
   */
  public void setStorageHandlerPackageNames(String[] outputHandlerPackageNames) {
    this.storageHandlerPackageNames = outputHandlerPackageNames;
  }

  /**
   * @return the nodeTrafficLimitInMB
   */
  public double getNodeTrafficLimitInMB() {
    return nodeTrafficLimitInMB;
  }

  /**
   * @param nodeTrafficLimitInMB the nodeTrafficLimitInMB to set
   */
  public void setNodeTrafficLimitInMB(double nodeTrafficLimitInMB) {
    this.nodeTrafficLimitInMB = nodeTrafficLimitInMB;
  }

  /**
   * @return the awsUploadRatePerStreamInMB
   */
  public double getAwsUploadRatePerStreamInMB() {
    return awsUploadRatePerStreamInMB;
  }

  /**
   * @param awsUploadRatePerStreamInMB the awsUploadRatePerStreamInMB to set
   */
  public void setAwsUploadRatePerStreamInMB(double awsUploadRatePerStreamInMB) {
    this.awsUploadRatePerStreamInMB = awsUploadRatePerStreamInMB;
  }

  /**
   * @return the cluster
   */
  public boolean isCluster() {
    return cluster;
  }

  /**
   * @param cluster the cluster to set
   */
  public void setCluster(boolean cluster) {
    this.cluster = cluster;
  }

  public boolean isResetEnabled() {
    return resetEnabled;
  }

  public void setResetEnabled(boolean resetEnabled) {
    this.resetEnabled = resetEnabled;
  }

  public NettyServerConfig getNettyServerConfig() {
    return nettyServerConfig;
  }

  public void setNettyServerConfig(NettyServerConfig nettyServerConfig) {
    this.nettyServerConfig = nettyServerConfig;
  }

  public AuthorizerConfig getAuthorizerConfig() {
    return authorizerConfig;
  }

  public void setAuthorizerConfig(AuthorizerConfig authorizerConfig) {
    this.authorizerConfig = authorizerConfig;
  }

  public BrokerType getBrokerType() {
    return brokerType;
  }

  public void setBrokerType(BrokerType brokerType) {
    this.brokerType = brokerType;
  }

}