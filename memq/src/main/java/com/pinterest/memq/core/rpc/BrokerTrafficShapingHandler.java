package com.pinterest.memq.core.rpc;

import com.codahale.metrics.MetricRegistry;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.handler.traffic.TrafficCounter;

import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

public class BrokerTrafficShapingHandler extends GlobalTrafficShapingHandler {

    public static String M1 = "test.broker.traffic.read.throughput";
    public static String M2 = "broker.traffic.read.limit";
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

    @Override
    protected void doAccounting(TrafficCounter counter) {
        super.doAccounting(counter);
        long readThroughput = counter.lastReadThroughput();
        registry.gauge(M1, () -> () -> readThroughput);
        long readLimit = getReadLimit();
        registry.gauge(M2, () -> () -> readLimit);
    }
}
