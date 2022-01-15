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
package com.pinterest.memq.client.commons;

public class CommonConfigs {

  public static final String CLUSTER = "cluster";
  public static final String SERVERSET_FILE = "serverset.file";
  public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
  public static final String CLIENT_LOCALITY = "client.locality";

  public static final String AUDITOR_CONFIG_PREFIX = "auditor.";
  public static final String AUDITOR_ENABLED = AUDITOR_CONFIG_PREFIX + "enabled";
  public static final String AUDITOR_CLASS = AUDITOR_CONFIG_PREFIX + "class";

}
