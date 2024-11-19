package com.pinterest.memq.core.rpc;

import com.codahale.metrics.MetricRegistry;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class BrokerTrafficShapingHandler extends GlobalTrafficShapingHandler {

    public static final String READ_LIMIT_METRIC_NAME = "broker.traffic.read.limit";
    private static final Logger logger = Logger.getLogger(BrokerTrafficShapingHandler.class.getName());
    private static int metricsReportingIntervalSec = 60; // default 1 minute
    private final MetricRegistry registry;

    public BrokerTrafficShapingHandler(ScheduledExecutorService executor,
                                       long writeLimit,
                                       long readLimit,
                                       long checkInterval,
                                       MetricRegistry registry) {
        super(executor, writeLimit, readLimit, checkInterval);
        this.registry = registry;
    }

    public void setMetricsReportingIntervalSec(int intervalSec) {
        metricsReportingIntervalSec = intervalSec;
    }

    public int getMetricsReportingIntervalSec() {
        return metricsReportingIntervalSec;
    }

    /**
     * Start periodic metrics reporting.
     * Overriding channel methods to send metrics can cause performance issues.
     * So we choose to send metrics in a separate thread periodically.
     * @param executorService
     */
    public void startPeriodicMetricsReporting(ScheduledExecutorService executorService) {
        logger.info(String.format("Starting periodic metrics reporting every %d seconds.",
            metricsReportingIntervalSec));
        Runnable reportTask = this::reportMetrics;
        executorService.scheduleAtFixedRate(
            reportTask, 0, metricsReportingIntervalSec, TimeUnit.SECONDS);
    }

    /**
     * Report read limit metric to the registry.
     */
    public void reportMetrics() {
        long readLimit = this.getReadLimit();
        registry.gauge(READ_LIMIT_METRIC_NAME, () -> () -> readLimit);
    }
}
