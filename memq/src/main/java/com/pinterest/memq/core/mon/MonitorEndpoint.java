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

import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

@Path("/metrics")
@Produces({ MediaType.APPLICATION_JSON })
public class MonitorEndpoint {

  private Map<String, MetricRegistry> registryMap;

  public MonitorEndpoint(Map<String, MetricRegistry> map) {
    this.registryMap = map;
  }

  @SuppressWarnings("rawtypes")
  @GET
  public String getMetrics() {
    JsonObject topicMetrics = new JsonObject();
    for (Entry<String, MetricRegistry> entry2 : registryMap.entrySet()) {
      MetricRegistry registry = entry2.getValue();
      JsonObject metrics = new JsonObject();
      topicMetrics.add(entry2.getKey(), metrics);

      JsonObject allCounters = new JsonObject();
      for (Entry<String, Counter> entry : registry.getCounters().entrySet()) {
        allCounters.addProperty(entry.getKey(), entry.getValue().getCount());
      }
      JsonObject allLatencies = new JsonObject();
      for (Entry<String, Timer> entry : registry.getTimers().entrySet()) {
        JsonObject obj = new JsonObject();
        Snapshot snapshot = entry.getValue().getSnapshot();
        obj.addProperty("max", snapshot.getMax());
        obj.addProperty("min", snapshot.getMin());
        obj.addProperty("mean", snapshot.getMean());
        allLatencies.add(entry.getKey(), obj);
      }

      JsonObject allGauges = new JsonObject();
      for (Entry<String, Gauge> entry : registry.getGauges().entrySet()) {
        Gauge gauge = entry.getValue();
        if (gauge.getValue() instanceof Long) {
          allGauges.addProperty(entry.getKey(), (Long) gauge.getValue());
        } else if (gauge.getValue() instanceof Double) {
          allGauges.addProperty(entry.getKey(), (Double) gauge.getValue());
        } else {
          String val = entry.getValue().getValue().toString();
          if (!val.contains("[")) {
            allGauges.addProperty(entry.getKey(), Double.parseDouble(val));
          }
        }
      }

      metrics.add("timers", allLatencies);
      metrics.add("counters", allCounters);
      metrics.add("gauges", allGauges);
    }
    return new GsonBuilder().setPrettyPrinting().create().toJson(topicMetrics);
  }

}