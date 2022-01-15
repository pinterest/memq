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
package com.pinterest.memq.client.commons2;

import com.pinterest.memq.commons.protocol.Broker;

import java.net.InetSocketAddress;

public class Endpoint {

  public static final String DEFAULT_LOCALITY = "n/a";
  private InetSocketAddress address;
  private String locality = DEFAULT_LOCALITY;

  public Endpoint(InetSocketAddress address, String locality) {
    this.address = address;
    this.locality = locality;
  }

  public Endpoint(InetSocketAddress address) {
    this.address = address;
  }

  public InetSocketAddress getAddress() {
    return address;
  }

  public void setAddress(InetSocketAddress address) {
    this.address = address;
  }

  public String getLocality() {
    return locality;
  }

  public void setLocality(String locality) {
    this.locality = locality;
  }

  public static Endpoint fromBroker(Broker broker) {
    return new Endpoint(InetSocketAddress.createUnresolved(broker.getBrokerIP(),
        broker.getBrokerPort()), broker.getLocality());
  }
}
