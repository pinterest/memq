package com.pinterest.memq.core.rpc;

import com.codahale.metrics.MetricRegistry;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.handler.traffic.TrafficCounter;

import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

public class BrokerTrafficShapingHandler extends GlobalTrafficShapingHandler {

    public static String BROKER_TRAFFIC_READ_THROTTLING_METRIC_NAME = "broker.traffic.read.throttling.counter";
    private static final Logger logger = Logger.getLogger(BrokerTrafficShapingHandler.class.getName());
    private final MetricRegistry registry;

    public BrokerTrafficShapingHandler(ScheduledExecutorService executor,
                                       long writeLimit,
                                       long readLimit,
                                       long checkInterval,
                                       MetricRegistry registry) {
        super(executor, writeLimit, readLimit, checkInterval);
        this.registry = registry;
    }

    private void recordReadThrottling() {
        registry.counter(BROKER_TRAFFIC_READ_THROTTLING_METRIC_NAME).inc();
    }

    @Override
    protected void doAccounting(TrafficCounter counter) {
        super.doAccounting(counter);
        if (isReadThrottled(counter)) {
            recordReadThrottling();
        }
    }

    private boolean isReadThrottled(TrafficCounter counter) {
        return counter.lastReadThroughput() == 0;
    }
}
