package com.pinterest.memq.core.load;

import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.core.processing.TopicProcessor;

import io.netty.channel.ChannelHandlerContext;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class SimpleRandomBalancingStrategy implements BalancingStrategy {
  private long samplingRate;

  @Override
  public void configure(Properties configs) {
    samplingRate = Integer.parseInt((String) configs.getOrDefault("samplingRate", "10_000"));
  }

  @Override
  public LoadBalancer.Action evaluate(RequestPacket rawPacket, ChannelHandlerContext context, TopicProcessor topicProcessor) {
    if (0L == ThreadLocalRandom.current().nextLong(0, samplingRate)) {
      return LoadBalancer.Action.DROP;
    }
    return LoadBalancer.Action.NO_OP;
  }
}
