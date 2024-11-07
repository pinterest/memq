package com.pinterest.memq.core.rpc;

import com.codahale.metrics.MetricRegistry;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.handler.traffic.TrafficCounter;

import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

public class BrokerTrafficShapingHandler extends GlobalTrafficShapingHandler {

    public static String BROKER_TRAFFIC_READ_THROTTLING_METRIC_NAME = "broker.traffic.read.throttling";
    private final MetricRegistry registry;
    private final double READ_LIMIT_ALERTING_THRESHOLD = 0.9;
    private static final Logger logger = Logger.getLogger(BrokerTrafficShapingHandler.class.getName());

    public BrokerTrafficShapingHandler(ScheduledExecutorService executor,
                                       long writeLimit,
                                       long readLimit,
                                       long checkInterval,
                                       MetricRegistry registry) {
        super(executor, writeLimit, readLimit, checkInterval);
        this.registry = registry;
    }

    private void recordReadThrottling() {
        logger.info("GlobalTrafficShapingHandler channel read throttling detected");
//        registry.counter(BROKER_TRAFFIC_READ_THROTTLING_METRIC_NAME).inc();
    }

    @Override
    protected void doAccounting(TrafficCounter counter) {
        super.doAccounting(counter);
        if (isReadThrottled(counter)) {
            recordReadThrottling();
        }
    }

    private boolean isReadThrottled(TrafficCounter counter) {
        logger.info("[TEST] counter.lastReadThroughput() = "
            + counter.lastReadThroughput()
            + "; getReadLimit() = "
            + getReadLimit()
            + "; counter.cumulativeReadBytes() = "
            + counter.cumulativeReadBytes()
            + "; counter.lastReadBytes() = "
            + counter.lastReadBytes());
        return getReadLimit() * READ_LIMIT_ALERTING_THRESHOLD < counter.lastReadThroughput();
    }
}
