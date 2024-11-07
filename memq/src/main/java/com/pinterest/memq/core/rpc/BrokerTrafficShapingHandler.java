package com.pinterest.memq.core.rpc;

import com.codahale.metrics.MetricRegistry;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;

import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

public class BrokerTrafficShapingHandler extends GlobalTrafficShapingHandler {

    private final MetricRegistry registry;
    public static String STATE_CHANGE_METRIC_NAME = "autoread.state.change.count";

    private static final Logger logger = Logger.getLogger(BrokerTrafficShapingHandler.class.getName());

    public BrokerTrafficShapingHandler(ScheduledExecutorService executor,
                                       long writeLimit,
                                       long readLimit,
                                       long checkInterval,
                                       MetricRegistry registry) {
        super(executor, writeLimit, readLimit, checkInterval);
        this.registry = registry;
    }

    private void recordAutoReadStateChange(
        String channelId, boolean autoReadStartState, boolean autoReadEndState) {
        logger.info(String.format("Channel %s autoRead state changed from %s to %s",
            channelId, autoReadStartState, autoReadEndState));
        registry.counter(STATE_CHANGE_METRIC_NAME).inc();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        Channel channel = ctx.channel();
        boolean autoReadStartState = channel.config().isAutoRead();
        super.channelRead(ctx, msg);
        boolean autoReadEndState = channel.config().isAutoRead();

        if (autoReadStartState != autoReadEndState) {
            recordAutoReadStateChange(
                channel.id().asShortText(), autoReadStartState, autoReadEndState);
        }
    }
}
