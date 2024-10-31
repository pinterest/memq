package com.pinterest.memq.core.rpc;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;

import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

public class ExperimentGlobalTrafficShapingHandler extends GlobalTrafficShapingHandler {

    private static final Logger logger = Logger.getLogger(ExperimentGlobalTrafficShapingHandler.class.getName());

    public ExperimentGlobalTrafficShapingHandler(ScheduledExecutorService executor, long writeLimit, long readLimit) {
        super(executor, writeLimit, readLimit);
    }

    public ExperimentGlobalTrafficShapingHandler(
        ScheduledExecutorService executor, long writeLimit, long readLimit, long checkInterval) {
        super(executor, writeLimit, readLimit, checkInterval);
    }


    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        ChannelConfig config = channel.config();
        boolean autoReadStartState = config.isAutoRead();
        super.handlerRemoved(ctx);
        boolean autoReadEndState = config.isAutoRead();
        if (autoReadStartState != autoReadEndState) {
            logger.info("[TEST] Channel " + channel.id() + "autoRead state changed from " + autoReadStartState + " to " + autoReadEndState + " after handlerRemoved");
        }
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        Channel channel = ctx.channel();
        ChannelConfig config = channel.config();
        boolean autoReadStartState = config.isAutoRead();
        super.channelRead(ctx, msg);
        boolean autoReadEndState = config.isAutoRead();
        if (autoReadStartState != autoReadEndState) {
            logger.info("[TEST] Channel " + channel.id() + "autoRead state changed from " + autoReadStartState + " to " + autoReadEndState + " after channelRead");
        }
    }
}
