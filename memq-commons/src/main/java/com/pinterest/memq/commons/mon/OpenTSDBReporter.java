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
package com.pinterest.memq.commons.mon;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.pinterest.memq.commons.mon.OpenTSDBClient.MetricsBuffer;

public class OpenTSDBReporter extends ScheduledReporter {

  private static final Logger logger = Logger.getLogger(OpenTSDBClient.class.getName());
  private OpenTSDBClient client;
  private String[] tags;
  private String baseName;
  private static final int RETRY_COUNT = 3;

  protected OpenTSDBReporter(String baseName,
                             MetricRegistry registry,
                             String registryName,
                             MetricFilter filter,
                             TimeUnit rateUnit,
                             TimeUnit durationUnit,
                             OpenTSDBClient client,
                             String localHostAddress,
                             Map<String, Object> tags) throws UnknownHostException {
    super(registry, registryName, filter, rateUnit, durationUnit);
    if (baseName == null || baseName.isEmpty()) {
      this.baseName = "";
    } else {
      this.baseName = baseName + ".";
    }
    this.tags = new String[tags.size() + 1];
    this.tags[tags.size()] = "host=" + localHostAddress;
    int i = 0;
    for (Map.Entry<String, Object> e : tags.entrySet()) {
      this.tags[i] = e.getKey() + "=" + e.getValue().toString();
      i++;
    }
    this.client = client;
  }

  public static ScheduledReporter createReporter(String baseName,
                                                 MetricRegistry registry,
                                                 String name,
                                                 MetricFilter filter,
                                                 TimeUnit rateUnit,
                                                 TimeUnit durationUnit,
                                                 OpenTSDBClient client,
                                                 String localHostAddress) throws UnknownHostException {
    return new OpenTSDBReporter(baseName, registry, name, filter, rateUnit, durationUnit, client,
        localHostAddress, Collections.emptyMap());
  }

  public static ScheduledReporter createReporterWithTags(String baseName,
                                                         MetricRegistry registry,
                                                         String name,
                                                         MetricFilter filter,
                                                         TimeUnit rateUnit,
                                                         TimeUnit durationUnit,
                                                         OpenTSDBClient client,
                                                         String localHostAddress,
                                                         Map<String, Object> tags) throws UnknownHostException {
    return new OpenTSDBReporter(baseName, registry, name, filter, rateUnit, durationUnit, client,
        localHostAddress, tags);
  }

  @Override
  public void report(@SuppressWarnings("rawtypes") SortedMap<String, Gauge> gauges,
                     SortedMap<String, Counter> counters,
                     SortedMap<String, Histogram> histograms,
                     SortedMap<String, Meter> meters,
                     SortedMap<String, Timer> timers) {
    try {
      int epochSecs = (int) (System.currentTimeMillis() / 1000);
      MetricsBuffer buffer = new MetricsBuffer("memq." + baseName);
      for (Entry<String, Counter> entry : counters.entrySet()) {
        buffer.addMetric(entry.getKey(), epochSecs, entry.getValue().getCount(), tags);
      }

      for (Entry<String, Meter> entry : meters.entrySet()) {
        buffer.addMetric(entry.getKey(), epochSecs, entry.getValue().getCount(), tags);
      }

      for (Entry<String, Histogram> entry : histograms.entrySet()) {
        Snapshot snapshot = entry.getValue().getSnapshot();
        buffer.addMetric(entry.getKey() + ".avg", epochSecs, snapshot.getMean(), tags);
        buffer.addMetric(entry.getKey() + ".min", epochSecs, snapshot.getMin(), tags);
        buffer.addMetric(entry.getKey() + ".median", epochSecs, snapshot.getMedian(), tags);
        buffer.addMetric(entry.getKey() + ".p50", epochSecs, snapshot.getMedian(), tags);
        buffer.addMetric(entry.getKey() + ".p75", epochSecs, snapshot.get75thPercentile(), tags);
        buffer.addMetric(entry.getKey() + ".p95", epochSecs, snapshot.get95thPercentile(), tags);
        buffer.addMetric(entry.getKey() + ".p98", epochSecs, snapshot.get98thPercentile(), tags);
        buffer.addMetric(entry.getKey() + ".p99", epochSecs, snapshot.get99thPercentile(), tags);
        buffer.addMetric(entry.getKey() + ".p999", epochSecs, snapshot.get999thPercentile(), tags);
        buffer.addMetric(entry.getKey() + ".max", epochSecs, snapshot.getMax(), tags);
        buffer.addMetric(entry.getKey() + ".stddev", epochSecs, snapshot.getStdDev(), tags);
      }

      for (Entry<String, Timer> entry : timers.entrySet()) {
        Snapshot snapshot = entry.getValue().getSnapshot();
        buffer.addMetric(entry.getKey() + ".avg", epochSecs, snapshot.getMean(), tags);
        buffer.addMetric(entry.getKey() + ".min", epochSecs, snapshot.getMin(), tags);
        buffer.addMetric(entry.getKey() + ".median", epochSecs, snapshot.getMedian(), tags);
        buffer.addMetric(entry.getKey() + ".p50", epochSecs, snapshot.getMedian(), tags);
        buffer.addMetric(entry.getKey() + ".p75", epochSecs, snapshot.get75thPercentile(), tags);
        buffer.addMetric(entry.getKey() + ".p95", epochSecs, snapshot.get95thPercentile(), tags);
        buffer.addMetric(entry.getKey() + ".p98", epochSecs, snapshot.get98thPercentile(), tags);
        buffer.addMetric(entry.getKey() + ".p99", epochSecs, snapshot.get99thPercentile(), tags);
        buffer.addMetric(entry.getKey() + ".p999", epochSecs, snapshot.get999thPercentile(), tags);
        buffer.addMetric(entry.getKey() + ".max", epochSecs, snapshot.getMax(), tags);
        buffer.addMetric(entry.getKey() + ".stddev", epochSecs, snapshot.getStdDev(), tags);
      }

      for (@SuppressWarnings("rawtypes")
      Entry<String, Gauge> entry : gauges.entrySet()) {
        if (entry.getValue().getValue() instanceof Long) {
          buffer.addMetric(entry.getKey(), epochSecs, (Long) entry.getValue().getValue(), tags);
        } else if (entry.getValue().getValue() instanceof Double) {
          buffer.addMetric(entry.getKey(), epochSecs, (Double) entry.getValue().getValue(), tags);
        } else {
          String val = entry.getValue().getValue().toString();
          if (!val.contains("[")) {
            buffer.addMetric(entry.getKey(), epochSecs, Double.parseDouble(val), tags);
          }
        }
      }

      int retryCounter = RETRY_COUNT;

      while (retryCounter > 0) {
        try {
          client.sendMetrics(buffer);
          break;
        } catch (Exception ex) {
          if (retryCounter == 1) {
            logger.log(Level.SEVERE, "Failed to send metrics to OpenTSDB after " + RETRY_COUNT + " retries", ex);
            throw ex;
          }
          retryCounter--;
        }
      }
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Failed to write metrics to opentsdb", e);
    }
  }

}