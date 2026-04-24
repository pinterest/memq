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

  /**
   * Per-metric tag delimiter. Codahale {@link MetricRegistry} keys are
   * arbitrary strings, so we let callers encode extra OpenTSDB tags inline:
   * {@code "<metric.name>|key1=value1|key2=value2"}. When a registry key
   * contains this delimiter, the segment before the first delimiter is the
   * emitted metric name and the rest are merged with the reporter's static
   * tag set on a per-line basis. Names without this character are emitted
   * unchanged. The character is intentionally one that no current memq
   * metric uses.
   */
  static final char TAG_DELIMITER = '|';

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
        String name = extractName(entry.getKey());
        String mergedTags = mergeTags(entry.getKey());
        buffer.addMetric(name, epochSecs, (float) entry.getValue().getCount(), mergedTags);
      }

      for (Entry<String, Meter> entry : meters.entrySet()) {
        String name = extractName(entry.getKey());
        String mergedTags = mergeTags(entry.getKey());
        buffer.addMetric(name, epochSecs, (float) entry.getValue().getCount(), mergedTags);
      }

      for (Entry<String, Histogram> entry : histograms.entrySet()) {
        String name = extractName(entry.getKey());
        String mergedTags = mergeTags(entry.getKey());
        Snapshot snapshot = entry.getValue().getSnapshot();
        buffer.addMetric(name + ".avg", epochSecs, (float) snapshot.getMean(), mergedTags);
        buffer.addMetric(name + ".min", epochSecs, (float) snapshot.getMin(), mergedTags);
        buffer.addMetric(name + ".median", epochSecs, (float) snapshot.getMedian(), mergedTags);
        buffer.addMetric(name + ".p50", epochSecs, (float) snapshot.getMedian(), mergedTags);
        buffer.addMetric(name + ".p75", epochSecs, (float) snapshot.get75thPercentile(), mergedTags);
        buffer.addMetric(name + ".p95", epochSecs, (float) snapshot.get95thPercentile(), mergedTags);
        buffer.addMetric(name + ".p98", epochSecs, (float) snapshot.get98thPercentile(), mergedTags);
        buffer.addMetric(name + ".p99", epochSecs, (float) snapshot.get99thPercentile(), mergedTags);
        buffer.addMetric(name + ".p999", epochSecs, (float) snapshot.get999thPercentile(), mergedTags);
        buffer.addMetric(name + ".max", epochSecs, (float) snapshot.getMax(), mergedTags);
        buffer.addMetric(name + ".stddev", epochSecs, (float) snapshot.getStdDev(), mergedTags);
      }

      for (Entry<String, Timer> entry : timers.entrySet()) {
        String name = extractName(entry.getKey());
        String mergedTags = mergeTags(entry.getKey());
        Snapshot snapshot = entry.getValue().getSnapshot();
        buffer.addMetric(name + ".avg", epochSecs, (float) snapshot.getMean(), mergedTags);
        buffer.addMetric(name + ".min", epochSecs, (float) snapshot.getMin(), mergedTags);
        buffer.addMetric(name + ".median", epochSecs, (float) snapshot.getMedian(), mergedTags);
        buffer.addMetric(name + ".p50", epochSecs, (float) snapshot.getMedian(), mergedTags);
        buffer.addMetric(name + ".p75", epochSecs, (float) snapshot.get75thPercentile(), mergedTags);
        buffer.addMetric(name + ".p95", epochSecs, (float) snapshot.get95thPercentile(), mergedTags);
        buffer.addMetric(name + ".p98", epochSecs, (float) snapshot.get98thPercentile(), mergedTags);
        buffer.addMetric(name + ".p99", epochSecs, (float) snapshot.get99thPercentile(), mergedTags);
        buffer.addMetric(name + ".p999", epochSecs, (float) snapshot.get999thPercentile(), mergedTags);
        buffer.addMetric(name + ".max", epochSecs, (float) snapshot.getMax(), mergedTags);
        buffer.addMetric(name + ".stddev", epochSecs, (float) snapshot.getStdDev(), mergedTags);
      }

      for (@SuppressWarnings("rawtypes")
      Entry<String, Gauge> entry : gauges.entrySet()) {
        String name = extractName(entry.getKey());
        String mergedTags = mergeTags(entry.getKey());
        Object v = entry.getValue().getValue();
        if (v instanceof Long) {
          buffer.addMetric(name, epochSecs, (float) ((Long) v).longValue(), mergedTags);
        } else if (v instanceof Double) {
          buffer.addMetric(name, epochSecs, ((Double) v).floatValue(), mergedTags);
        } else {
          String val = v.toString();
          if (!val.contains("[")) {
            buffer.addMetric(name, epochSecs, (float) Double.parseDouble(val), mergedTags);
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

  /**
   * Strip any inline {@link #TAG_DELIMITER}-encoded tag suffix from a
   * codahale registry key, returning just the metric name to emit. Names
   * without the delimiter pass through unchanged.
   * <p>
   * Visible for test.
   */
  static String extractName(String registryKey) {
    int idx = registryKey.indexOf(TAG_DELIMITER);
    return idx < 0 ? registryKey : registryKey.substring(0, idx);
  }

  /**
   * Build the per-line tag string for a registry key by merging any inline
   * tags encoded after {@link #TAG_DELIMITER} with this reporter's static
   * tag set (host plus any constructor-supplied tags). Inline tags come
   * first so that callers can shadow reporter-wide tags by re-declaring
   * them, but in practice the two are disjoint.
   * <p>
   * Wire format produced is OpenTSDB-style space-separated {@code key=value}
   * tokens, ready to drop into the line protocol.
   */
  String mergeTags(String registryKey) {
    int idx = registryKey.indexOf(TAG_DELIMITER);
    StringBuilder sb = new StringBuilder();
    if (idx >= 0 && idx < registryKey.length() - 1) {
      // Replace remaining delimiters with spaces -- each segment is already
      // a key=value token, so this gives us a valid OpenTSDB tag list.
      String inline = registryKey.substring(idx + 1).replace(TAG_DELIMITER, ' ');
      sb.append(inline);
    }
    for (String t : tags) {
      if (sb.length() > 0) {
        sb.append(' ');
      }
      sb.append(t);
    }
    return sb.toString();
  }

}