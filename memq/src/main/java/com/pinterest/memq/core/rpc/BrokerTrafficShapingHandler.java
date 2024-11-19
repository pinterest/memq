package com.pinterest.memq.core.rpc;

import com.codahale.metrics.MetricRegistry;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class BrokerTrafficShapingHandler extends GlobalTrafficShapingHandler {

    public static final String READ_LIMIT_METRIC_NAME = "broker.traffic.read.limit";
    private static final Logger logger = Logger.getLogger(BrokerTrafficShapingHandler.class.getName());
    private static int metricsReportingIntervalMinutes = 1;
    private final MetricRegistry registry;

    public BrokerTrafficShapingHandler(ScheduledExecutorService executor,
                                       long writeLimit,
                                       long readLimit,
                                       long checkInterval,
                                       MetricRegistry registry) {
        super(executor, writeLimit, readLimit, checkInterval);
        this.registry = registry;
    }

    public void setMetricsReportingIntervalMinutes(int metricsReportingIntervalMinutes) {
        this.metricsReportingIntervalMinutes = metricsReportingIntervalMinutes;
    }

    public int getMetricsReportingIntervalMinutes() {
        return metricsReportingIntervalMinutes;
    }

    public void startPeriodicMetricsReporting(ScheduledExecutorService executorService) {
        logger.info("[TEST] Starting periodic metrics reporting");
        Runnable reportTask = this::reportMetrics; // Using method reference for brevity
        executorService.scheduleAtFixedRate(
            reportTask, 0, metricsReportingIntervalMinutes, TimeUnit.MINUTES);
    }

    public void reportMetrics() {
        logger.info("[TEST] Reporting metrics");
        registry.histogram(READ_LIMIT_METRIC_NAME).update(this.getReadLimit());
    }
}
