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
package com.pinterest.memq.core;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.jvm.CachedThreadStatesGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.pinterest.memq.commons.mon.OpenTSDBClient;
import com.pinterest.memq.commons.mon.OpenTSDBReporter;
import com.pinterest.memq.core.clustering.MemqGovernor;
import com.pinterest.memq.core.config.EnvironmentProvider;
import com.pinterest.memq.core.config.MemqConfig;
import com.pinterest.memq.core.mon.MemqMgrHealthCheck;
import com.pinterest.memq.core.mon.MonitorEndpoint;
import com.pinterest.memq.core.rpc.MemqNettyServer;
import com.pinterest.memq.core.utils.DaemonThreadFactory;
import com.pinterest.memq.core.utils.MiscUtils;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

public class MemqMain extends Application<MemqConfig> {

  private static final Logger logger = Logger.getLogger(MemqMain.class.getName());

  @Override
  public void run(MemqConfig configuration, Environment environment) throws Exception {
    MiscUtils.printAllLines(ClassLoader.getSystemResourceAsStream("logo.txt"));
    Map<String, MetricRegistry> metricsRegistryMap = new HashMap<>();
    enableJVMMetrics(metricsRegistryMap);
    MetricRegistry misc = new MetricRegistry();
    emitStartMetrics(misc);
    metricsRegistryMap.put("_misc", misc);
    OpenTSDBClient client = initializeMetricsTransmitter(configuration, environment,
        metricsRegistryMap);
    MemqManager memqManager = new MemqManager(client, configuration, metricsRegistryMap);
    memqManager.init();
    environment.lifecycle().manage(memqManager);
    addAPIs(environment, memqManager);

    if (configuration.isResetEnabled()) {
      Executors.newScheduledThreadPool(1, new DaemonThreadFactory()).schedule(() -> {
        logger.info("Memq scheduled restart, this is by design to reset heap.\n"
            + "Supervisord should restart Memq instance");
        System.exit(0);
      }, 1, TimeUnit.HOURS);
    }

    environment.healthChecks().register("base", new MemqMgrHealthCheck(memqManager));

    MemqGovernor memqGovernor = initializeGovernor(configuration, memqManager);
    MemqNettyServer nettyServer = initializeNettyServer(configuration, memqManager, memqGovernor,
        metricsRegistryMap, client);
    logger.info("Memq started");

    initializeShutdownHooks(memqManager, memqGovernor, nettyServer);
  }

  private void initializeShutdownHooks(MemqManager memqManager,
                                       MemqGovernor memqGovernor,
                                       MemqNettyServer nettyServer) {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        nettyServer.stop();
        memqGovernor.stop();
        try {
          memqManager.stop();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }

  private void enableJVMMetrics(Map<String, MetricRegistry> metricsRegistryMap) {
    MetricRegistry registry = new MetricRegistry();
    registry.register("gc", new GarbageCollectorMetricSet());
    registry.register("threads", new CachedThreadStatesGaugeSet(10, TimeUnit.SECONDS));
    registry.register("memory", new MemoryUsageGaugeSet());
    metricsRegistryMap.put("_jvm", registry);
  }

  public MemqGovernor initializeGovernor(MemqConfig configuration,
                                         MemqManager memqManager) throws Exception {
    EnvironmentProvider provider = null;
    try {
      provider = Class.forName(configuration.getEnvironmentProvider())
          .asSubclass(EnvironmentProvider.class).newInstance();
    } catch (Exception e) {
      throw new Exception("Failed to initialize environment provider", e);
    }
    MemqGovernor memqGovernor = new MemqGovernor(memqManager, configuration, provider);
    if (configuration.getClusteringConfig() != null) {
      memqGovernor.init();
    }
    return memqGovernor;
  }

  public MemqNettyServer initializeNettyServer(MemqConfig configuration,
                                               MemqManager memqManager,
                                               MemqGovernor memqGovernor,
                                               Map<String, MetricRegistry> metricsRegistryMap,
                                               OpenTSDBClient client) throws Exception {
    MemqNettyServer server = new MemqNettyServer(configuration, memqManager, memqGovernor,
        metricsRegistryMap, client);
    server.initialize();
    return server;
  }

  private OpenTSDBClient initializeMetricsTransmitter(MemqConfig configuration,
                                                      Environment environment,
                                                      Map<String, MetricRegistry> metricsRegistryMap) throws UnknownHostException {
    if (configuration.getOpenTsdbConfig() != null) {
      OpenTSDBClient client = new OpenTSDBClient(configuration.getOpenTsdbConfig().getHost(),
          configuration.getOpenTsdbConfig().getPort());
      String localHostname = MiscUtils.getHostname();
      for (Entry<String, MetricRegistry> entry : metricsRegistryMap.entrySet()) {
        ScheduledReporter reporter;
        if (entry.getKey().startsWith("_")) { // non-topic metrics
          reporter = OpenTSDBReporter.createReporter("", entry.getValue(), entry.getKey(),
              (String name, Metric metric) -> true, TimeUnit.SECONDS, TimeUnit.SECONDS, client,
              localHostname);
        } else { // topic metrics
          reporter = OpenTSDBReporter.createReporterWithTags("", entry.getValue(), entry.getKey(),
              (String name, Metric metric) -> true, TimeUnit.SECONDS, TimeUnit.SECONDS, client,
              localHostname, Collections.singletonMap("topic", entry.getKey()));
        }
        reporter.start(configuration.getOpenTsdbConfig().getFrequencyInSeconds(), TimeUnit.SECONDS);
      }
      return client;
    }
    return null;
  }

  private void addAPIs(Environment environment, MemqManager memqManager) {
    environment.jersey().setUrlPattern("/api/*");
    environment.jersey().register(new MonitorEndpoint(memqManager.getRegistry()));
  }

  private void emitStartMetrics(MetricRegistry registry) {
    final long startTs = System.currentTimeMillis();
    registry.gauge("start", () -> (Gauge<Integer>) () -> System.currentTimeMillis() - startTs < 3 * 60_000 ? 1 : 0);
  }

  public static void main(String[] args) throws Exception {
    new MemqMain().run(args);
  }

}