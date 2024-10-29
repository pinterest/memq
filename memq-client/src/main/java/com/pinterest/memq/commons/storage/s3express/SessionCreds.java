/**
 * Copyright 2024 Pinterest, Inc.
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
package com.pinterest.memq.commons.storage.s3express;

/**
 * Hold AWS Session Credentials
 */
public class SessionCreds {

  public String key, secret, token;

  public SessionCreds() {
  }

  public SessionCreds(String key, String secret, String token) {
    this.key = key;
    this.secret = secret;
    this.token = token;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setSecret(String secret) {
      this.secret = secret;
  }

  public void setToken(String token) {
      this.token = token;
  }

  public String getKey() {
      return key;
  }

  public String getSecret() {
      return secret;
  }

  public String getToken() {
      return token;
  }

  @Override
  public String toString() {
    return "SessionCreds [key=" + key + ", secret=" + secret + ", token=" + token + "]";
  }

}
