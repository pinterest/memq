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
package com.pinterest.memq.client.consumer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.pinterest.memq.commons.mon.OpenTSDBClient;
import com.pinterest.memq.commons.mon.OpenTSDBReporter;

public class Metrics implements Runnable {

  private static Metrics INSTANCE;
  private Map<String, MetricRegistry> registryMap;
  private Map<String, ScheduledReporter> reporterMap;
  private ScheduledExecutorService es;

  private Metrics() {
    registryMap = new ConcurrentHashMap<>();
    reporterMap = new ConcurrentHashMap<>();
    es = Executors.newScheduledThreadPool(1);
    es.scheduleAtFixedRate(this, 0, 60, TimeUnit.SECONDS);
  }
  
  public static String getHostname() {
    String hostName;
    try {
      hostName = InetAddress.getLocalHost().getHostName();
      int firstDotPos = hostName.indexOf('.');
      if (firstDotPos > 0) {
        hostName = hostName.substring(0, firstDotPos);
      }
    } catch (Exception e) {
      // fall back to env var.
      hostName = System.getenv("HOSTNAME");
    }
    return hostName;
  }

  public synchronized static Metrics getInstance() throws Exception {
    if (INSTANCE == null) {
      INSTANCE = new Metrics();
    }
    return INSTANCE;
  }

  @Override
  public void run() {
    for (Entry<String, MetricRegistry> entry : registryMap.entrySet()) {
      ScheduledReporter reporter = reporterMap.get(entry.getKey());
      if (reporter == null) {
        try {
          reporter = OpenTSDBReporter.createReporter("consumer", entry.getValue(), entry.getKey(),
              (String name, Metric metric) -> true, TimeUnit.SECONDS, TimeUnit.SECONDS,
              new OpenTSDBClient("localhost", 18126), getHostname());
          reporter.start(60, TimeUnit.SECONDS);
          reporterMap.put(entry.getKey(), reporter);
        } catch (UnknownHostException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
  }

  public synchronized MetricRegistry getOrCreate(String name) {
    MetricRegistry r = registryMap.get(name);
    if (r == null) {
      r = new MetricRegistry();
      registryMap.put(name, r);
    }
    return r;
  }

}
