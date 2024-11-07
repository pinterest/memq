package com.pinterest.memq.core.rpc;

import com.codahale.metrics.MetricRegistry;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.handler.traffic.TrafficCounter;

import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

public class BrokerTrafficShapingHandler extends GlobalTrafficShapingHandler {

    private final MetricRegistry registry;
    public static String BROKER_TRAFFIC_THROTTLING_METRIC_NAME = "broker.traffic.throttling";

    private static final Logger logger = Logger.getLogger(BrokerTrafficShapingHandler.class.getName());

    public BrokerTrafficShapingHandler(ScheduledExecutorService executor,
                                       long writeLimit,
                                       long readLimit,
                                       long checkInterval,
                                       MetricRegistry registry) {
        super(executor, writeLimit, readLimit, checkInterval);
        this.registry = registry;
    }

    private void recordThrottling() {
        logger.info("GlobalTrafficShapingHandler throttling detected");
        registry.counter(BROKER_TRAFFIC_THROTTLING_METRIC_NAME).inc();
    }

    @Override
    protected void doAccounting(TrafficCounter counter) {
        super.doAccounting(counter);
        if (isThrottling(counter)) {
            recordThrottling();
        }
    }

    private boolean isThrottling(TrafficCounter counter) {
        return counter.cumulativeReadBytes() > getReadLimit() ||
            counter.cumulativeWrittenBytes() > getWriteLimit();
    }
}
