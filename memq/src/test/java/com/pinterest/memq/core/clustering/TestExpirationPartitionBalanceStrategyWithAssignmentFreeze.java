package com.pinterest.memq.core.clustering;

import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.TopicConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TestExpirationPartitionBalanceStrategyWithAssignmentFreeze {

    private static Broker generateBroker(int index, String localityLetter) throws Exception {
        return new Broker(
            "1.1.1." + index,
            (short) 9092,
            "c6i.2xlarge",
            "us-east-1" + localityLetter,
            Broker.BrokerType.WRITE,
            new HashSet<>()
        );
    }

    private static TopicConfig generateTopicConfig(int index) throws Exception {
        return new TopicConfig(
            index,
            1024 * 1024 * 50,
            256,
            "test_topic_" + index,
            50,
            0,
            3
        );
    }

    private static int getAssignedTopicsCount(Set<Broker> brokers) {
        int assigned = 0;
        for (Broker broker : brokers) {
            assigned += broker.getAssignedTopics().size();
        }
        return assigned;
    }

    @Test
    public void testExpirationPartitionBalanceStrategyWithAssignmentFreeze() throws Exception {
        int expirationTime = 500; // 500ms
        ExpirationPartitionBalanceStrategyWithAssignmentFreeze strategy = new ExpirationPartitionBalanceStrategyWithAssignmentFreeze();
        strategy.setDefaultExpirationTime(expirationTime); // 500ms

        // 3 brokers, 1 topic with 450MB input traffic
        TopicConfig topicConfig = generateTopicConfig(0);
        topicConfig.setInputTrafficMB(450);
        Set<TopicConfig> topics = new HashSet<>(Arrays.asList(topicConfig));
        Set<Broker> brokers = new HashSet<>(
            Arrays.asList(
                generateBroker(1, "a"),
                generateBroker(2, "b"),
                generateBroker(3, "c")
            )
        );

        // topic assigned to all 3 brokers
        brokers = strategy.balance(topics, brokers);
        assertEquals(3, getAssignedTopicsCount(brokers));

        // topic requires 6 brokers, but only 3 available, still keep the assignment
        topicConfig.setInputTrafficMB(900);
        brokers = strategy.balance(topics, brokers);
        assertEquals(3, getAssignedTopicsCount(brokers));

        // after expiration, still keep the assignment
        Thread.sleep(expirationTime); // 1000ms
        brokers = strategy.balance(topics, brokers);
        assertEquals(3, getAssignedTopicsCount(brokers));

        // add 3 more brokers, topic should be assigned to all 6 brokers
        brokers.add(generateBroker(4, "a"));
        brokers.add(generateBroker(5, "b"));
        brokers.add(generateBroker(6, "c"));
        brokers = strategy.balance(topics, brokers);
        assertEquals(6, getAssignedTopicsCount(brokers));

        // reduce input traffic to 450MB, topic should be assigned to 3 brokers
        topicConfig.setInputTrafficMB(450);
        brokers = strategy.balance(topics, brokers);
        assertEquals(3, getAssignedTopicsCount(brokers));
    }
}
