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
package com.pinterest.memq.commons.storage.s3;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.logging.Logger;

import org.apache.http.conn.DnsResolver;

public class MemqS3DNSResolver implements DnsResolver {

  private static final Logger logger = Logger.getLogger(MemqS3DNSResolver.class.getCanonicalName());

  @Override
  public InetAddress[] resolve(String host) throws UnknownHostException {
    InetAddress[] address = InetAddress.getAllByName(host);
    logger.fine(() -> "Host:" + host + " address:" + Arrays.toString(address));
    return address;
  }
}