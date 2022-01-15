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
package com.pinterest.memq.client.commons.audit;

import java.io.File;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.util.Recycler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Timer;
import com.pinterest.memq.commons.mon.OpenTSDBClient;
import com.pinterest.memq.commons.mon.OpenTSDBReporter;
import com.pinterest.memq.core.utils.DaemonThreadFactory;

public class KafkaBackedAuditAnalyzer {
  public static class Payload {
    private static final Recycler<Payload> RECYCLER = new Recycler<Payload>() {

      @Override
      protected Payload newObject(Handle<Payload> handle) {
        return new Payload(handle);
      }
    };

    byte[] hash;
    long ts;
    byte producer;
    int count;
    Recycler.Handle<Payload> handle;

    private Payload(Recycler.Handle<Payload> handle) {
      this.handle = handle;
    }

    public static Payload newInstance(byte[] hash, long ts, byte producer, int count) {
      Payload payload = RECYCLER.get();
      payload.hash = hash;
      payload.ts = ts;
      payload.producer = producer;
      payload.count = count;
      return payload;
    }

    public void recycle() {
      hash = null;
      ts = 0;
      producer = 0;
      count = 0;
      handle.recycle(this);
    }
  }

  private KafkaConsumer<byte[], byte[]> consumer;
  private ScheduledExecutorService es;
  private String hostName;
  private final Map<String, KafkaTopicAuditAnalyzer> topicAnalyzers = new ConcurrentHashMap<>();

  public KafkaBackedAuditAnalyzer() {
  }

  public void init(Properties props) throws Exception {
    hostName = InetAddress.getLocalHost().getHostName();
    if (hostName.contains(".")) {
      hostName = hostName.substring(0, hostName.indexOf("."));
    }

    Thread th = new Thread(() -> {
      Scanner sc = new Scanner(System.in);
      while (sc.hasNext()) {
        String next = sc.next();
        switch (next) {
        case "purge":
          for (KafkaTopicAuditAnalyzer a : topicAnalyzers.values()) {
            a.purge();
          }
          System.out.println("Purged");
          break;
        }
      }
      System.out.println("Completed");
      sc.close();
    });
    th.setDaemon(true);
    th.start();

    consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList(props.getProperty("audittopic")));

    es = Executors.newScheduledThreadPool(1, new DaemonThreadFactory());
    es.schedule(() -> {
      int c = 0;
      for (KafkaTopicAuditAnalyzer a : topicAnalyzers.values()) {
        c += a.truncateInitialDelta();
      }
      System.out.println("Purging initial delta records:" + c);
    }, 1, TimeUnit.MINUTES);

    es.scheduleAtFixedRate(() -> {
      long delta = 0;
      long noAcksCount = 0;
      long trackingSize = 0;

      for (KafkaTopicAuditAnalyzer a : topicAnalyzers.values()) {
        delta += a.getDelta();
        noAcksCount += a.getNoAckCount();
        trackingSize += a.getTrackingSize();
      }

      System.out.println(
          new Date() + " audit events>300s MissingAcks:" + delta
              + " NoAcksFor300s:" + noAcksCount + " AuditMapSize:" + trackingSize);
    }, 0, 60, TimeUnit.SECONDS);
  }

  public void run() {
    while (true) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(2000));
      for (ConsumerRecord<byte[], byte[]> record : records) {
        ByteBuffer wrap = ByteBuffer.wrap(record.value());
        byte[] cluster = new byte[wrap.getShort()];
        wrap.get(cluster);
        byte[] topic = new byte[wrap.getShort()];
        wrap.get(topic);
        long recordTimestamp = record.timestamp();

        String clusterStr = new String(cluster);
        String topicStr = new String(topic);

        String key = String.format("%s-%s", clusterStr, topicStr);
        KafkaTopicAuditAnalyzer ta = topicAnalyzers.get(key);
        if (ta == null) {
          try {
            ta = new KafkaTopicAuditAnalyzer(clusterStr, topicStr);
            topicAnalyzers.put(key, ta);
          } catch (Exception e) {
            System.out.println("Failed to init topic audit analyzer for " + key);
          }
        }

        if (ta != null) {
          ta.audit(wrap, recordTimestamp);
        }
      }
      consumer.commitAsync();
    }
  }

  public static void main(String[] args) throws Exception {
    KafkaBackedAuditAnalyzer analyzer = new KafkaBackedAuditAnalyzer();

    String auditTopicServerset = args[0];
    String auditTopic = args[1];
    String groupId = args[2];

    Properties props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServersFromServerset(auditTopicServerset));
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getCanonicalName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getCanonicalName());
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10000");
    props.setProperty("audittopic", auditTopic);
    analyzer.init(props);
    analyzer.run();
  }

  private class KafkaTopicAuditAnalyzer {
    private String cluster;
    private String topic;

    private Map<String, Payload> auditTrackerMap = new ConcurrentHashMap<>();
    private AtomicLong counter = new AtomicLong();
    private AtomicLong ackedCounter = new AtomicLong();
    private Counter logmessageCounter;
    private Counter duplicateAckCounter;
    private Counter hashMismatchCounter;
    private Timer ackLatency;

    public KafkaTopicAuditAnalyzer(String cluster, String topic) throws Exception {
      this.cluster = cluster;
      this.topic = topic;
      init();
    }

    public void init() throws Exception {
      MetricRegistry registry = new MetricRegistry();
      registry.register("delta", (Gauge<Long>) () -> counter.get() - ackedCounter.get());
      registry.register("trackersize", (Gauge<Long>) () -> (long) auditTrackerMap.size());

      ackLatency = registry.timer("latency", () -> new Timer(new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES)));
      logmessageCounter = registry.counter("logmessages");
      duplicateAckCounter = registry.counter("duplicate.acks");
      hashMismatchCounter = registry.counter("hash.mismatch");

      Map<String, Object> tags = new HashMap<>();
      tags.put("cluster", cluster);
      tags.put("topic", topic);
      ScheduledReporter
          reporter =
          OpenTSDBReporter.createReporterWithTags("memqaudit", registry, cluster,
              (String name, Metric m) -> true, TimeUnit.SECONDS, TimeUnit.SECONDS,
              new OpenTSDBClient("localhost", 18126), hostName, tags);
      reporter.start(1, TimeUnit.MINUTES);
    }

    public void audit(ByteBuffer message, long recordTimestamp) {
      // host address + epochId
      byte[] key = new byte[message.getShort() + 16];
      message.get(key);
      byte[] hash = new byte[message.getShort()];
      message.get(hash);
      String keyString = Base64.getEncoder().encodeToString(key);
      int count = message.getInt();
      byte producer = message.get();
      Payload output = auditTrackerMap.get(keyString);
      if (output == null) {
        auditTrackerMap.put(keyString, Payload.newInstance(hash, recordTimestamp, producer, count));
        if (producer == 1) {
          // only increment counter if producer message comes first
          // if consumer message comes first, we want to ignore the count so singer restarts won't corrupt the counter
          counter.addAndGet(count);
        }
      } else {
        byte[] array = output.hash;
        if (producer == output.producer) {
          duplicateAckCounter.inc();
          return;
        }
        if (!Arrays.equals(hash, array)) {
          hashMismatchCounter.inc();
        } else {
          long latency = Math.abs(recordTimestamp - output.ts);
          ackLatency.update(latency, TimeUnit.MILLISECONDS);
          if (producer == 1) {
            // producer message comes after consumer message
            // need to bump both counters so data loss cancels out
            counter.addAndGet(count);
          }
          ackedCounter.addAndGet(count);
          logmessageCounter.inc(count);
        }
        auditTrackerMap.remove(keyString).recycle();
      }
    }

    public int truncateInitialDelta() {
      Iterator<Entry<String, Payload>> iterator = auditTrackerMap.entrySet().iterator();
      int c = 0;
      while (iterator.hasNext()) {
        Entry<String, Payload> next = iterator.next();
        if (System.currentTimeMillis() - next.getValue().ts > 300) {
          iterator.remove();
          c++;
        }
      }
      return c;
    }

    public void purge() {
      auditTrackerMap.clear();
    }

    public long getDelta() {
      return counter.get() - ackedCounter.get();
    }

    public long getTrackingSize() {
      return auditTrackerMap.size();
    }

    public long getNoAckCount() {
      return auditTrackerMap.values().stream().filter(v -> System.currentTimeMillis() - v.ts > 300_000).count();
    }

  }

  private static String getBootstrapServersFromServerset(String serversetFile) throws Exception {
    return String.join(",", Files.readAllLines(new File(serversetFile).toPath()));
  }
}