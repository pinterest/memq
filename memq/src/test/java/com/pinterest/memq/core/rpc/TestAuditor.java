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
package com.pinterest.memq.core.rpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.pinterest.memq.client.commons.audit.Auditor;

public class TestAuditor extends Auditor {

  private static List<TestAuditMessage> auditMessageList = new ArrayList<>();

  @Override
  public void init(Properties props) throws Exception {
  }

  @Override
  public void auditMessage(byte[] cluster,
                                        byte[] topic,
                                        byte[] hostAddress,
                                        long epoch,
                                        long id,
                                        byte[] hash,
                                        int messageCount,
                                        boolean isProducer,
                                        String clientId) throws IOException {
    synchronized(TestAuditor.class) {
      auditMessageList.add(new TestAuditMessage(cluster, hash, topic, hostAddress, epoch, id,
          messageCount, isProducer));
    }
  }

  @Override
  public void close() {
  }

  public static List<TestAuditMessage> getAuditMessageList() {
    return auditMessageList;
  }
  
  public static void reset() {
    auditMessageList.clear();
  }

}