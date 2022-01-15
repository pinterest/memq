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

public class OpenTsdbConfiguration {
  
  private String host;
  private int port;
  private int frequencyInSeconds;
  
  /**
   * @return the host
   */
  public String getHost() {
    return host;
  }
  /**
   * @param host the host to set
   */
  public void setHost(String host) {
    this.host = host;
  }
  /**
   * @return the port
   */
  public int getPort() {
    return port;
  }
  /**
   * @param port the port to set
   */
  public void setPort(int port) {
    this.port = port;
  }
  /**
   * @return the frequencyInSeconds
   */
  public int getFrequencyInSeconds() {
    return frequencyInSeconds;
  }
  /**
   * @param frequencyInSeconds the frequencyInSeconds to set
   */
  public void setFrequencyInSeconds(int frequencyInSeconds) {
    this.frequencyInSeconds = frequencyInSeconds;
  }

}