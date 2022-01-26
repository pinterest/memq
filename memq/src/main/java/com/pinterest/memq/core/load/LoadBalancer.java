package com.pinterest.memq.core.load;

import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.core.processing.TopicProcessor;

import io.netty.channel.ChannelHandlerContext;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LoadBalancer {
  public static final String BALANCING_STRATEGY_KEY = "balancingStrategy";
  private static final Logger logger = Logger.getLogger(LoadBalancer.class.getName());
  private BalancingStrategy strategy;
  private TopicProcessor processor;

  public LoadBalancer(Properties configs, TopicProcessor processor)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    this.processor = processor;
    this.strategy = Class.forName(configs.getProperty(BALANCING_STRATEGY_KEY ,NoOpBalancingStrategy.class.getName())).asSubclass(BalancingStrategy.class).newInstance();
    strategy.configure(configs);
  }

  public Action evaluate(RequestPacket rawPacket, ChannelHandlerContext context) {
    return strategy.evaluate(rawPacket, context, processor);
  }

  public boolean reconfigure(Properties configs) {
    if (strategy.getClass().getName().equals(configs.getProperty(BALANCING_STRATEGY_KEY))) {
      strategy.configure(configs);
    } else {
      BalancingStrategy newStrategy;
      try {
        newStrategy = Class.forName(configs.getProperty(BALANCING_STRATEGY_KEY ,NoOpBalancingStrategy.class.getName())).asSubclass(BalancingStrategy.class).newInstance();
        strategy.cleanup();
        strategy = newStrategy;
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        logger.log(Level.SEVERE, "Failed to construct new balancer strategy: ", e);
      }
    }
    return true;
  }

  public enum Action {
    NO_OP,
    DROP, // DROP_IMMEDIATELY
    // DROP_AFTER_ACK
  }
}
