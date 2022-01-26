package com.pinterest.memq.core.load;

import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.core.processing.TopicProcessor;

import io.netty.channel.ChannelHandlerContext;

import java.util.Properties;

public class NoOpBalancingStrategy implements BalancingStrategy {

  @Override
  public void configure(Properties configs) {

  }

  @Override
  public LoadBalancer.Action evaluate(RequestPacket rawPacket, ChannelHandlerContext context,
                                      TopicProcessor topicProcessor) {
    return LoadBalancer.Action.NO_OP;
  }
}
