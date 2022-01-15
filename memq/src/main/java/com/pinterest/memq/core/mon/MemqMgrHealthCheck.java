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
package com.pinterest.memq.core.mon;

import com.codahale.metrics.health.HealthCheck;
import com.pinterest.memq.core.MemqManager;

public class MemqMgrHealthCheck extends HealthCheck {

  private MemqManager mgr;

  public MemqMgrHealthCheck(MemqManager mgr) {
    this.mgr = mgr;
  }

  @Override
  protected Result check() throws Exception {
    if (mgr.isRunning()) {
      return Result.healthy();
    } else {
      return Result.unhealthy("Memq Manager is not healthy");
    }
  }

}
