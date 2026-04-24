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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * Unit tests for {@link OpenTSDBReporter}'s inline tag-encoding parser.
 * <p>
 * Codahale registry keys can encode extra OpenTSDB tags after a {@code |}
 * delimiter (see {@link OpenTSDBReporter#TAG_DELIMITER}). Names without
 * the delimiter must pass through unchanged so existing metrics are not
 * affected.
 */
public class TestOpenTSDBReporter {

  @Test
  public void testExtractNameStripsTagSuffix() {
    // No delimiter -> name returned verbatim. Critical for backwards
    // compat with every metric currently in the registry.
    assertEquals("tp.writeCounter", OpenTSDBReporter.extractName("tp.writeCounter"));
    assertEquals("slot.total", OpenTSDBReporter.extractName("slot.total"));
    assertEquals("", OpenTSDBReporter.extractName(""));

    // Single tag.
    assertEquals("producer.ema", OpenTSDBReporter.extractName("producer.ema|pid=10.0.0.1"));

    // Multiple tags.
    assertEquals("producer.ema",
        OpenTSDBReporter.extractName("producer.ema|pid=10.0.0.1|topic=foo"));

    // Pathological: leading delimiter -> empty name. We don't try to
    // recover; this is a contract bug for the caller.
    assertEquals("", OpenTSDBReporter.extractName("|pid=foo"));
  }

  @Test
  public void testMergeTagsAppendsReporterTags() throws Exception {
    OpenTSDBReporter reporter = newReporter(map("dc", "us-east-1"));

    // No inline tags -> just the reporter's static tags. host comes
    // from InetAddress lookup at construction time and may be the
    // hostname or an IP -- so just assert prefix shape.
    String tagsOnly = reporter.mergeTags("slot.total");
    assertTrue("must contain dc tag: " + tagsOnly, tagsOnly.contains("dc=us-east-1"));
    assertTrue("must contain host tag: " + tagsOnly, tagsOnly.contains("host="));
    assertFalse("must not surface inline-tag delimiter when there is none: " + tagsOnly,
        tagsOnly.contains("|"));

    // Inline tags -> appended before reporter tags, single-space separated.
    String merged = reporter.mergeTags("producer.ema|pid=10.0.0.1|topic=foo");
    assertTrue("must contain pid tag: " + merged, merged.contains("pid=10.0.0.1"));
    assertTrue("must contain topic tag: " + merged, merged.contains("topic=foo"));
    assertTrue("must contain dc tag (reporter-static): " + merged,
        merged.contains("dc=us-east-1"));
    assertTrue("must contain host tag (reporter-static): " + merged,
        merged.contains("host="));
    assertFalse("inline pipe delimiter must be replaced with spaces: " + merged,
        merged.contains("|"));

    // Trailing pipe with empty tag list: no inline tags appended, but
    // the reporter tags still come out cleanly (no leading space gap).
    String trailing = reporter.mergeTags("producer.ema|");
    assertFalse(trailing.startsWith(" "));
    assertTrue(trailing.contains("dc=us-east-1"));
  }

  @Test
  public void testReportEmitsTaggedGaugeWithMergedTags() throws Exception {
    // End-to-end: a gauge registered with a |-encoded key must surface
    // in the OpenTSDB wire payload as `put memq.<base>.<name> <ts> <val>
    // <inline-tags> <reporter-static-tags>`. We capture the payload by
    // subclassing OpenTSDBClient.
    final StringBuilder sentPayload = new StringBuilder();
    OpenTSDBClient capturingClient = new OpenTSDBClient("127.0.0.1", 4242) {
      @Override
      public void sendMetrics(MetricsBuffer buffer) {
        sentPayload.append(buffer.toString());
      }
    };

    MetricRegistry registry = new MetricRegistry();
    registry.gauge("producer.ema|pid=10.0.0.1|topic=foo",
        () -> (Gauge<Double>) () -> 1.5);
    // Untagged gauge to confirm backwards compat in the same report cycle.
    registry.gauge("slot.total", () -> (Gauge<Integer>) () -> 32);

    OpenTSDBReporter reporter = (OpenTSDBReporter) OpenTSDBReporter.createReporter(
        "slot", registry, "test", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.SECONDS,
        capturingClient, "test-host");

    reporter.report();

    String payload = sentPayload.toString();

    // Tagged gauge: name is `memq.slot.producer.ema`, value is 1.5,
    // and both the inline tags and the host tag are present.
    assertTrue("missing tagged gauge in payload: " + payload,
        payload.contains("put memq.slot.producer.ema "));
    assertTrue("tagged gauge value missing: " + payload, payload.contains(" 1.5 "));
    assertTrue("inline pid tag missing: " + payload, payload.contains("pid=10.0.0.1"));
    assertTrue("inline topic tag missing: " + payload, payload.contains("topic=foo"));
    assertTrue("host tag missing: " + payload, payload.contains("host=test-host"));

    // Untagged gauge: must still emit cleanly with no leftover delimiter
    // and the value preserved.
    assertTrue("missing untagged gauge in payload: " + payload,
        payload.contains("put memq.slot.slot.total "));
    assertFalse("untagged gauge line must not contain inline-tag delimiter: " + payload,
        lineFor(payload, "memq.slot.slot.total").contains("|"));
  }

  private static String lineFor(String payload, String needle) {
    for (String line : payload.split("\n")) {
      if (line.contains(needle)) {
        return line;
      }
    }
    return "";
  }

  private static OpenTSDBReporter newReporter(Map<String, Object> tags) throws Exception {
    OpenTSDBClient noopClient = new OpenTSDBClient("127.0.0.1", 4242) {
      @Override
      public void sendMetrics(MetricsBuffer buffer) {
        // intentional no-op -- mergeTags() does not actually send.
      }
    };
    return (OpenTSDBReporter) OpenTSDBReporter.createReporterWithTags(
        "test", new MetricRegistry(), "test", MetricFilter.ALL,
        TimeUnit.SECONDS, TimeUnit.SECONDS, noopClient, "test-host",
        tags == null ? Collections.emptyMap() : tags);
  }

  private static Map<String, Object> map(String k, String v) {
    Map<String, Object> m = new HashMap<>();
    m.put(k, v);
    return m;
  }
}
