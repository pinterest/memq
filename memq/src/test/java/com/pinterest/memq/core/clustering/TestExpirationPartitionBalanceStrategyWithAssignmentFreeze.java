package com.pinterest.memq.core.clustering;

import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.TopicConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TestExpirationPartitionBalanceStrategyWithAssignmentFreeze {
    @Test
    public void testAssignment() throws Exception {
        short port = 9092;
        ExpirationPartitionBalanceStrategyWithAssignmentFreeze strategy = new ExpirationPartitionBalanceStrategyWithAssignmentFreeze();
        strategy.setDefaultExpirationTime(500);

        TopicConfig baseConfig = new TopicConfig(0, 1024 * 1024 * 50, 256, "hello", 50, 0, 3);
        TopicConfig conf = new TopicConfig(baseConfig);
        Set<Broker> brokers = new HashSet<>(
            Arrays.asList(
                new Broker("1.1.1.1", port, "c5.2xlarge", "us-east-1a", Broker.BrokerType.WRITE, new HashSet<>()),
                new Broker("1.1.1.2", port, "c5.2xlarge", "us-east-1b", Broker.BrokerType.WRITE, new HashSet<>()),
                new Broker("1.1.1.3", port, "c5.2xlarge", "us-east-1c", Broker.BrokerType.WRITE, new HashSet<>()),
                new Broker("1.1.1.4", port, "c5.2xlarge", "us-east-1a", Broker.BrokerType.WRITE, new HashSet<>()),
                new Broker("1.1.1.5", port, "c5.2xlarge", "us-east-1b", Broker.BrokerType.WRITE, new HashSet<>()),
                new Broker("1.1.1.6", port, "c5.2xlarge", "us-east-1c", Broker.BrokerType.WRITE, new HashSet<>())
            )
        );

        Set<TopicConfig> topics = new HashSet<>();
        conf.setInputTrafficMB(900);
        topics.add(conf);
        brokers = strategy.balance(topics, brokers);
        int assigned = 0;
        for (Broker broker : brokers) {
            assigned += broker.getAssignedTopics().size();
        }
        assertEquals(6, assigned);

        topics.clear();
        conf.setInputTrafficMB(1350);
        topics.add(conf);
        brokers = strategy.balance(topics, brokers);
        assigned = 0;
        for (Broker broker : brokers) {
            assigned += broker.getAssignedTopics().size();
        }
        assertEquals(6, assigned);

        Thread.sleep(1000);
        topics.clear();
        conf.setInputTrafficMB(1350);
        topics.add(conf);
        brokers = strategy.balance(topics, brokers);
        assigned = 0;
        for (Broker broker : brokers) {
            assigned += broker.getAssignedTopics().size();
        }
        assertEquals(6, assigned);

        brokers.add(new Broker("1.1.1.7", port, "c5.2xlarge", "us-east-1a", Broker.BrokerType.WRITE, new HashSet<>()));
        brokers.add(new Broker("1.1.1.8", port, "c5.2xlarge", "us-east-1b", Broker.BrokerType.WRITE, new HashSet<>()));
        brokers.add(new Broker("1.1.1.9", port, "c5.2xlarge", "us-east-1c", Broker.BrokerType.WRITE, new HashSet<>()));
        brokers = strategy.balance(topics, brokers);
        assigned = 0;
        for (Broker broker : brokers) {
            assigned += broker.getAssignedTopics().size();
        }
        assertEquals(9, assigned);
    }
}
