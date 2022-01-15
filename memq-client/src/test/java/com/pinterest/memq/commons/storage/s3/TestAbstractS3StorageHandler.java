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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;

public class TestAbstractS3StorageHandler {

  @Test
  public void testAmazonCertMigration() {
    HttpClient client = HttpClient.create().secure();
    HttpClientResponse block = client.get()
        .uri("https://s3-ats-migration-test.s3.eu-west-3.amazonaws.com/test.jpg").response()
        .block();
    int code = block.status().code();
    assertEquals(200, code);
  }

}
