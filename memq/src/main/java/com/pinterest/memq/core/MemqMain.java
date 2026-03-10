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
import com.pinterest.memq.core.config.EvictionConfig;
import com.pinterest.memq.core.config.GossipConfig;
import com.pinterest.memq.core.config.MemqConfig;
import com.pinterest.memq.core.config.SlotAccountingConfig;
import com.pinterest.memq.core.eviction.EvictionManager;
import com.pinterest.memq.core.eviction.EvictionStrategy;
import com.pinterest.memq.core.gossip.GossipServer;
import com.pinterest.memq.core.slot.SlotManager;
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

    SlotManager slotManager = initializeSlotManager(configuration, memqManager, metricsRegistryMap,
        client);

    MemqGovernor memqGovernor = initializeGovernor(configuration, memqManager);
    MemqNettyServer nettyServer = initializeNettyServer(configuration, memqManager, memqGovernor,
        metricsRegistryMap, client);

    GossipServer gossipServer = initializeGossipServer(configuration, memqGovernor,
        metricsRegistryMap, client, slotManager);

    EvictionManager evictionManager = initializeEvictionManager(configuration, memqManager,
        slotManager, gossipServer);
    logger.info("Memq started");

    initializeShutdownHooks(memqManager, memqGovernor, nettyServer, gossipServer, slotManager,
        evictionManager);
    initializeAdditionalModules(configuration, environment, memqManager, memqGovernor);
  }

  private void initializeShutdownHooks(MemqManager memqManager,
                                       MemqGovernor memqGovernor,
                                       MemqNettyServer nettyServer,
                                       GossipServer gossipServer,
                                       SlotManager slotManager,
                                       EvictionManager evictionManager) {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        if (evictionManager != null) {
          evictionManager.stop();
        }
        if (slotManager != null) {
          slotManager.stop();
        }
        if (gossipServer != null) {
          gossipServer.stop();
        }
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

  private EvictionManager initializeEvictionManager(MemqConfig configuration,
                                                      MemqManager memqManager,
                                                      SlotManager slotManager,
                                                      GossipServer gossipServer) throws Exception {
    EvictionConfig evictionConfig = configuration.getEvictionConfig();
    if (evictionConfig == null || !evictionConfig.isEnabled()) {
      return null;
    }
    if (slotManager == null || gossipServer == null) {
      logger.warning("Eviction enabled but SlotManager or GossipServer is not available; "
          + "disabling eviction");
      return null;
    }

    EnvironmentProvider provider = Class.forName(configuration.getEnvironmentProvider())
        .asSubclass(EnvironmentProvider.class).newInstance();

    EvictionStrategy strategy = Class.forName(evictionConfig.getStrategyClass())
        .asSubclass(EvictionStrategy.class)
        .getConstructor(String.class, EvictionConfig.class)
        .newInstance(provider.getIP(), evictionConfig);
    EvictionManager evictionManager = new EvictionManager(strategy, slotManager,
        gossipServer::getPeerStates, evictionConfig);
    memqManager.setEvictionManager(evictionManager);
    evictionManager.start();

    logger.info("Eviction manager started (interval=" + evictionConfig.getIntervalSeconds() + "s)");
    return evictionManager;
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

  private GossipServer initializeGossipServer(MemqConfig configuration,
                                                MemqGovernor memqGovernor,
                                                Map<String, MetricRegistry> metricsRegistryMap,
                                                OpenTSDBClient client,
                                                SlotManager slotManager) throws Exception {
    GossipConfig gossipConfig = configuration.getGossipConfig();
    if (gossipConfig != null && gossipConfig.isEnabled()) {
      EnvironmentProvider provider = Class.forName(configuration.getEnvironmentProvider())
          .asSubclass(EnvironmentProvider.class).newInstance();
      MetricRegistry gossipRegistry = new MetricRegistry();
      metricsRegistryMap.put("gossip", gossipRegistry);
      GossipServer gossipServer = new GossipServer(provider.getIP(), provider.getRack(),
          gossipConfig, memqGovernor, gossipRegistry, slotManager);
      gossipServer.start();

      if (client != null) {
        String localHostname = MiscUtils.getHostname();
        for (String metricName : gossipRegistry.getNames()) {
          ScheduledReporter reporter = OpenTSDBReporter.createReporter("gossip", gossipRegistry,
              metricName, (String name, Metric metric) -> true, TimeUnit.SECONDS, TimeUnit.SECONDS,
              client, localHostname);
          reporter.start(configuration.getOpenTsdbConfig().getFrequencyInSeconds(),
              TimeUnit.SECONDS);
        }
      }

      logger.info("Gossip server started on port " + gossipConfig.getPort());
      return gossipServer;
    }
    return null;
  }

  private SlotManager initializeSlotManager(MemqConfig configuration,
                                              MemqManager memqManager,
                                              Map<String, MetricRegistry> metricsRegistryMap,
                                              OpenTSDBClient client) throws Exception {
    SlotAccountingConfig slotConfig = configuration.getSlotAccountingConfig();
    if (slotConfig == null || !slotConfig.isEnabled()) {
      return null;
    }

    int maxTrafficMbps = configuration.getNettyServerConfig().getMaxBrokerInputTrafficMbPerSec();
    if (maxTrafficMbps <= 0) {
      logger.warning("Slot accounting enabled but maxBrokerInputTrafficMbPerSec is not set;"
          + " disabling slot accounting");
      return null;
    }

    double effectiveCapacity = maxTrafficMbps * (1 - slotConfig.getSlotOverhead());
    int totalSlots = (int) Math.ceil(effectiveCapacity / slotConfig.getSlotSizeMbps());

    MetricRegistry slotRegistry = new MetricRegistry();

    SlotManager slotManager = new SlotManager(slotConfig, totalSlots, slotRegistry);
    memqManager.setSlotManager(slotManager);
    slotManager.start();
    metricsRegistryMap.put("slot", slotRegistry);
    slotRegistry.gauge("total", () -> (Gauge<Integer>) slotManager::getTotalSlots);
    slotRegistry.gauge("occupied", () -> (Gauge<Integer>) slotManager::getOccupiedSlots);
    slotRegistry.gauge("free", () -> (Gauge<Integer>) slotManager::getFreeSlots);
    slotRegistry.gauge("frozen",
        () -> (Gauge<Integer>) () -> slotManager.isFrozen() ? 1 : 0);
    slotRegistry.gauge("producers", () -> (Gauge<Integer>) slotManager::getProducerCount);

    if (client != null) {
      String localHostname = MiscUtils.getHostname();
      for (String metricName : slotRegistry.getNames()) {
        ScheduledReporter reporter = OpenTSDBReporter.createReporter("slot", slotRegistry,
            metricName, (String name, Metric metric) -> true, TimeUnit.SECONDS, TimeUnit.SECONDS,
            client, localHostname);
        reporter.start(configuration.getOpenTsdbConfig().getFrequencyInSeconds(), TimeUnit.SECONDS);
      }
    }

    logger.info("Slot accounting started: totalSlots=" + totalSlots
        + " slotSizeMbps=" + slotConfig.getSlotSizeMbps()
        + " effectiveCapacityMbps=" + effectiveCapacity);
    return slotManager;
  }

  public void initializeAdditionalModules(MemqConfig config, Environment environment, MemqManager memqManager, MemqGovernor memqGovernor) throws Exception {

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