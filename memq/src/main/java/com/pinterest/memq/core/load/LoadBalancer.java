package com.pinterest.memq.core.load;

import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.core.processing.TopicProcessor;

import io.netty.channel.ChannelHandlerContext;

import java.util.Properties;

public class LoadBalancer {
  private BalancingStrategy strategy;
  private TopicProcessor processor;

  public LoadBalancer(Properties configs, TopicProcessor processor)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    this.processor = processor;
    this.strategy = Class.forName(configs.getProperty("balancingStrategy",NoOpBalancingStrategy.class.getName())).asSubclass(BalancingStrategy.class).newInstance();
    strategy.configure(configs);
  }

  public Action evaluate(RequestPacket rawPacket, ChannelHandlerContext context) {
    return strategy.evaluate(rawPacket, context, processor);
  }

  public enum Action {
    NO_OP,
    DROP, // DROP_IMMEDIATELY
    // DROP_AFTER_ACK
  }
}
