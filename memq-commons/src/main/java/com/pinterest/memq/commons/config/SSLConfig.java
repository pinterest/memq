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
package com.pinterest.memq.commons.config;

import java.util.Collections;
import java.util.List;

/**
 * listeners=PLAINTEXT://:9092,SSL://:9093
 * security.inter.broker.protocol=PLAINTEXT ssl.client.auth=required
 * ssl.enabled.protocols=TLSv1.2,TLSv1.1,TLSv1
 * ssl.endpoint.identification.algorithm=HTTPS ssl.key.password=pintastic
 * ssl.keystore.location=/var/lib/normandie/fuse/jks/generic
 * ssl.keystore.password=pintastic ssl.keystore.type=JKS
 * ssl.secure.random.implementation=SHA1PRNG
 * ssl.truststore.location=/var/lib/normandie/fuse/jkstrust/generic
 * ssl.truststore.password=pintastic ssl.truststore.type=JKS
 * authorizer.class.name=com.pinterest.commons.kafka.authorizers.PastisAuthorizer
 * kafka.authorizer.pastis_policy=kafka
 */
public class SSLConfig {

  private String keystorePath;
  private String keystoreType;
  private String keystorePassword;
  private String truststorePath;
  private String truststoreType;
  private String truststorePassword;
  private List<String> protocols = Collections.singletonList("TLSv1.2");

  public String getKeystorePath() {
    return keystorePath;
  }

  public void setKeystorePath(String keystorePath) {
    this.keystorePath = keystorePath;
  }

  public String getKeystoreType() {
    return keystoreType;
  }

  public void setKeystoreType(String keystoreType) {
    this.keystoreType = keystoreType;
  }

  public String getKeystorePassword() {
    return keystorePassword;
  }

  public void setKeystorePassword(String keystorePassword) {
    this.keystorePassword = keystorePassword;
  }

  public String getTruststorePath() {
    return truststorePath;
  }

  public void setTruststorePath(String truststorePath) {
    this.truststorePath = truststorePath;
  }

  public String getTruststoreType() {
    return truststoreType;
  }

  public void setTruststoreType(String truststoreType) {
    this.truststoreType = truststoreType;
  }

  public String getTruststorePassword() {
    return truststorePassword;
  }

  public void setTruststorePassword(String truststorePassword) {
    this.truststorePassword = truststorePassword;
  }

  public List<String> getProtocols() {
    return protocols;
  }

  public void setProtocols(List<String> protocols) {
    this.protocols = protocols;
  }
}
