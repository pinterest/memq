package com.pinterest.memq.core.clustering;

import com.google.common.collect.Sets;
import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.TopicAssignment;
import com.pinterest.memq.commons.protocol.TopicConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestExpirationPartitionBalanceStrategyWithAssignmentFreeze {

    @Test
    public void testSingleTopicSufficientBrokersBalancedAz() throws Exception {
        long expirationTime = 500;
        BalanceStrategy strategy = getExpirationBalanceStrategyWithFreeze(expirationTime);

        // 6 brokers, 1 topic with 450MB input traffic
        TopicConfig topicConfig = generateTopicConfig(0);
        topicConfig.setInputTrafficMB(450);
        Set<TopicConfig> topics = new HashSet<>(Arrays.asList(topicConfig));
        Set<Broker> brokers = getBrokers(2, 2, 2);

        // topic assigned to 3 brokers
        brokers = strategy.balance(topics, brokers);
        Set<Broker> assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(3, assignedBrokers.size());
        assertLocalityBalance(assignedBrokers);

        // topic requires 6 brokers
        topicConfig.setInputTrafficMB(900);
        brokers = strategy.balance(topics, brokers);
        assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(6, assignedBrokers.size());
        assertLocalityBalance(assignedBrokers);

        // after expiration, still keep the assignment
        Thread.sleep(expirationTime * 2);
        brokers = strategy.balance(topics, brokers);
        assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(6, assignedBrokers.size());
        assertLocalityBalance(assignedBrokers);

        // reduce input traffic to 450MB, topic should be assigned to 3 brokers
        topicConfig.setInputTrafficMB(450);
        brokers = strategy.balance(topics, brokers);
        assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(3, assignedBrokers.size());
        assertLocalityBalance(assignedBrokers);
    }

    @Test
    public void testSingleTopicSufficientBrokersImbalancedAz() throws Exception {
        long expirationTime = 500;
        BalanceStrategy strategy = getExpirationBalanceStrategyWithFreeze(expirationTime);

        // 6 brokers, 1 topic with 450MB input traffic
        TopicConfig topicConfig = generateTopicConfig(0);
        topicConfig.setInputTrafficMB(450);
        Set<TopicConfig> topics = new HashSet<>(Arrays.asList(topicConfig));
        Set<Broker> brokers = getBrokers(2, 2, 2);

        // topic assigned to 3 brokers
        brokers = strategy.balance(topics, brokers);
        Set<Broker> assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(3, assignedBrokers.size());
        assertLocalityBalance(assignedBrokers);

        Thread.sleep(expirationTime * 2);

        // remove one broker from a
        Set<Broker> removedA = removeBrokers(brokers, "a", 1);
        brokers = strategy.balance(topics, brokers);
        assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(3, assignedBrokers.size());

        Thread.sleep(expirationTime * 2);

        // remove one broker from b
        Set<Broker> removedB = removeBrokers(brokers, "b", 1);
        brokers = strategy.balance(topics, brokers);
        assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(3, assignedBrokers.size());

        // remove another broker from b
        Set<Broker> removedB2 = removeBrokers(brokers, "b", 1);
        assertEquals(3, brokers.size());
        brokers = strategy.balance(topics, brokers);
        assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(2, assignedBrokers.size());    // only 2 brokers left from previous assignment and it's frozen

        Thread.sleep(expirationTime * 2);

        // add removed brokers from b back
        brokers.addAll(removedB);
        brokers.addAll(removedB2);

        brokers = strategy.balance(topics, brokers);
        assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(3, assignedBrokers.size());

        // add removed broker from a back
        brokers.addAll(removedA);

        Thread.sleep(expirationTime * 2);

        brokers = strategy.balance(topics, brokers);
        assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(3, assignedBrokers.size());
    }

    @Test
    public void testSingleTopicInsufficientBrokersBalancedAz() throws Exception {
        long expirationTime = 500;
        BalanceStrategy strategy = getExpirationBalanceStrategyWithFreeze(expirationTime);

        // 3 brokers, 1 topic with 450MB input traffic
        TopicConfig topicConfig = generateTopicConfig(0);
        topicConfig.setInputTrafficMB(450);
        Set<TopicConfig> topics = new HashSet<>(Arrays.asList(topicConfig));
        Set<Broker> brokers = getBrokers(1, 1, 1);

        // topic assigned to all 3 brokers
        brokers = strategy.balance(topics, brokers);
        Set<Broker> assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(3, assignedBrokers.size());
        assertLocalityBalance(assignedBrokers);

        // topic requires 6 brokers, but only 3 available, still keep the assignment
        topicConfig.setInputTrafficMB(900);
        brokers = strategy.balance(topics, brokers);
        assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(3, assignedBrokers.size());
        assertLocalityBalance(assignedBrokers);

        // after expiration, still keep the assignment
        Thread.sleep(expirationTime * 2);
        brokers = strategy.balance(topics, brokers);
        assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(3, assignedBrokers.size());
        assertLocalityBalance(assignedBrokers);

        // add 3 more brokers, topic should be assigned to all 6 brokers
        brokers.add(generateBroker(4, "a"));
        brokers.add(generateBroker(5, "b"));
        brokers.add(generateBroker(6, "c"));
        brokers = strategy.balance(topics, brokers);
        assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(6, assignedBrokers.size());
        assertLocalityBalance(assignedBrokers);

        // reduce input traffic to 450MB, topic should be assigned to 3 brokers
        topicConfig.setInputTrafficMB(450);
        brokers = strategy.balance(topics, brokers);
        assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(3, assignedBrokers.size());
        assertLocalityBalance(assignedBrokers);
    }

    @Test
    public void testSingleTopicInsufficientBrokersImbalancedAz() throws Exception {
        long expirationTime = 500;
        BalanceStrategy strategy = getExpirationBalanceStrategyWithFreeze(expirationTime);

        // 6 brokers, 1 topic with 900MB input traffic
        TopicConfig topicConfig = generateTopicConfig(0);
        topicConfig.setInputTrafficMB(900);
        Set<TopicConfig> topics = new HashSet<>(Arrays.asList(topicConfig));
        Set<Broker> brokers = getBrokers(2, 2, 2);

        brokers = strategy.balance(topics, brokers);
        Set<Broker> assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(6, assignedBrokers.size());
        assertLocalityBalance(assignedBrokers);

        Thread.sleep(expirationTime * 2);

        Set<Broker> removed = removeBrokers(brokers, "a", 1);
        brokers = strategy.balance(topics, brokers);
        assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(5, assignedBrokers.size());

        Thread.sleep(expirationTime * 2);

        brokers = strategy.balance(topics, brokers);
        assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(5, assignedBrokers.size());

        // add removed brokers back
        brokers.addAll(removed);
        brokers = strategy.balance(topics, brokers);
        assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(6, assignedBrokers.size());
        assertLocalityBalance(assignedBrokers);

        Thread.sleep(expirationTime * 2);

        brokers = strategy.balance(topics, brokers);
        assignedBrokers = getAssignedBrokersForTopic(brokers, topicConfig.getTopic());
        assertEquals(6, assignedBrokers.size());
        assertLocalityBalance(assignedBrokers);
    }

    @Test
    public void testMultiTopicSufficientBrokersBalancedAz() throws Exception {
        long expirationTime = 500;
        BalanceStrategy strategy = getExpirationBalanceStrategyWithFreeze(expirationTime);

        // 12 brokers, 2 topics with 450 and 900MB input traffic, 900MB topic has priority
        TopicConfig topicConfig1 = generateTopicConfig(0);
        topicConfig1.setInputTrafficMB(450);
        topicConfig1.setTopicOrder(1000);
        TopicConfig topicConfig2 = generateTopicConfig(1);
        topicConfig2.setInputTrafficMB(900);
        topicConfig2.setTopicOrder(0);

        Set<TopicConfig> topics = new HashSet<>(Arrays.asList(topicConfig1, topicConfig2));
        Set<Broker> brokers = getBrokers(4, 4, 4);

        // topic 2 assigned to 6 brokers, topic 1 assigned to 3 brokers
        brokers = strategy.balance(topics, brokers);
        Set<Broker> assignedBrokers1 = getAssignedBrokersForTopic(brokers, topicConfig1.getTopic());
        Set<Broker> assignedBrokers2 = getAssignedBrokersForTopic(brokers, topicConfig2.getTopic());
        assertEquals(3, assignedBrokers1.size());
        assertEquals(6, assignedBrokers2.size());
        assertLocalityBalance(assignedBrokers1);
        assertLocalityBalance(assignedBrokers2);

        // remove all assigned brokers from each AZ for topic 2 and reassign with inputTrafficMB = 450
        brokers.removeAll(assignedBrokers2);
        Set<Broker> removed = new HashSet<>(assignedBrokers2);
        topicConfig2.setInputTrafficMB(450);
        brokers = strategy.balance(topics, brokers);
        assignedBrokers2 = getAssignedBrokersForTopic(brokers, topicConfig2.getTopic());
        assertEquals(3, assignedBrokers2.size());
        assertLocalityBalance(assignedBrokers2);
        assertTrue(Sets.intersection(assignedBrokers2, removed).isEmpty());

        topicConfig2.setInputTrafficMB(1350);

        brokers.addAll(removed);

        // topic 2 assigned to 9 brokers, topic 1 assigned to 3 brokers
        brokers = strategy.balance(topics, brokers);
        assignedBrokers1 = getAssignedBrokersForTopic(brokers, topicConfig1.getTopic());
        assignedBrokers2 = getAssignedBrokersForTopic(brokers, topicConfig2.getTopic());
        assertEquals(3, assignedBrokers1.size());
        assertEquals(9, assignedBrokers2.size());
        assertLocalityBalance(assignedBrokers1);
        assertLocalityBalance(assignedBrokers2);

        Thread.sleep(expirationTime * 2);

        brokers = strategy.balance(topics, brokers);
        assignedBrokers1 = getAssignedBrokersForTopic(brokers, topicConfig1.getTopic());
        assignedBrokers2 = getAssignedBrokersForTopic(brokers, topicConfig2.getTopic());
        assertEquals(3, assignedBrokers1.size());
        assertEquals(9, assignedBrokers2.size());
        assertLocalityBalance(assignedBrokers1);
        assertLocalityBalance(assignedBrokers2);
    }

    @Test
    public void testMultiTopicInsufficientBrokersBalancedAz() throws Exception {
        long expirationTime = 500;
        BalanceStrategy strategy = getExpirationBalanceStrategyWithFreeze(expirationTime);

        // 12 brokers, 2 topics with 450 and 900MB input traffic, topic0 has priority over topic1
        TopicConfig topicConfig0 = generateTopicConfig(0);
        topicConfig0.setInputTrafficMB(450);
        topicConfig0.setTopicOrder(0);
        TopicConfig topicConfig1 = generateTopicConfig(1);
        topicConfig1.setInputTrafficMB(900);
        topicConfig1.setTopicOrder(1000);

        Set<TopicConfig> topics = new HashSet<>(Arrays.asList(topicConfig0, topicConfig1));
        Set<Broker> brokers = getBrokers(4, 4, 4);

        brokers = strategy.balance(topics, brokers);
        Set<Broker> assignedBrokers0 = getAssignedBrokersForTopic(brokers, topicConfig0.getTopic());
        Set<Broker> assignedBrokers1 = getAssignedBrokersForTopic(brokers, topicConfig1.getTopic());
        assertEquals(3, assignedBrokers0.size());
        assertEquals(6, assignedBrokers1.size());
        assertLocalityBalance(assignedBrokers0);
        assertLocalityBalance(assignedBrokers1);

        Thread.sleep(expirationTime * 2);

        // topic0 needs 9 brokers, topic1 still needs 6 brokers, but only 12 available in total
        topicConfig0.setInputTrafficMB(1350);   // setting this to 1800 (all 12 brokers) will result in topic1 being fully dropped

        brokers = strategy.balance(topics, brokers);
        assignedBrokers0 = getAssignedBrokersForTopic(brokers, topicConfig0.getTopic());
        assignedBrokers1 = getAssignedBrokersForTopic(brokers, topicConfig1.getTopic());
        assertEquals(9, assignedBrokers0.size());
        assertEquals(3, assignedBrokers1.size());   // TODO: this scenario is concerning because topic1's assignment got dropped

        brokers.add(generateBroker(13, "a"));
        brokers.add(generateBroker(14, "b"));
        brokers.add(generateBroker(15, "c"));

        // topic 1 needs 9 brokers, topic 2 needs 6 brokers, 15 brokers available
        brokers = strategy.balance(topics, brokers);
        assignedBrokers0 = getAssignedBrokersForTopic(brokers, topicConfig0.getTopic());
        assignedBrokers1 = getAssignedBrokersForTopic(brokers, topicConfig1.getTopic());
        assertEquals(9, assignedBrokers0.size());
        assertEquals(6, assignedBrokers1.size());
        assertLocalityBalance(assignedBrokers0);
        assertLocalityBalance(assignedBrokers1);

        Thread.sleep(expirationTime * 2);

        brokers = strategy.balance(topics, brokers);
        assignedBrokers0 = getAssignedBrokersForTopic(brokers, topicConfig0.getTopic());
        assignedBrokers1 = getAssignedBrokersForTopic(brokers, topicConfig1.getTopic());
        assertEquals(9, assignedBrokers0.size());
        assertEquals(6, assignedBrokers1.size());
        assertLocalityBalance(assignedBrokers0);
        assertLocalityBalance(assignedBrokers1);

        // remove 3 brokers, 1 from each AZ
        Set<Broker> removed = removeBrokers(brokers, "a", 1);
        removed = Sets.union(removed, removeBrokers(brokers, "b", 1));
        removed = Sets.union(removed, removeBrokers(brokers, "c", 1));

        assertEquals(12, brokers.size());
        assertEquals(3, removed.size());

        Set<Broker> removed0 = Sets.intersection(removed, assignedBrokers0);
        Set<Broker> removed1 = Sets.intersection(removed, assignedBrokers1);

        long now = System.currentTimeMillis();
        while (System.currentTimeMillis() - now < expirationTime * 2) {
            brokers = strategy.balance(topics, brokers);
            assignedBrokers0 = getAssignedBrokersForTopic(brokers, topicConfig0.getTopic());
            assignedBrokers1 = getAssignedBrokersForTopic(brokers, topicConfig1.getTopic());
            assertEquals(9 - removed0.size(), assignedBrokers0.size());
            assertEquals(6 - removed1.size(), assignedBrokers1.size());
            Thread.sleep(50);
        }

        brokers.addAll(removed);

        brokers = strategy.balance(topics, brokers);
        assignedBrokers0 = getAssignedBrokersForTopic(brokers, topicConfig0.getTopic());
        assignedBrokers1 = getAssignedBrokersForTopic(brokers, topicConfig1.getTopic());

        assertEquals(9, assignedBrokers0.size());
        assertEquals(6, assignedBrokers1.size());
    }

    @Test
    public void testMultiTopicSufficientBrokersImbalancedAz() throws Exception {
        long expirationTime = 500;
        BalanceStrategy strategy = getExpirationBalanceStrategyWithFreeze(expirationTime);

        // 12 brokers, 2 topics with 450 and 900MB input traffic, topic0 has priority over topic1
        TopicConfig topicConfig0 = generateTopicConfig(0);
        topicConfig0.setInputTrafficMB(450);
        topicConfig0.setTopicOrder(0);
        TopicConfig topicConfig1 = generateTopicConfig(1);
        topicConfig1.setInputTrafficMB(900);
        topicConfig1.setTopicOrder(1000);

        Set<TopicConfig> topics = new HashSet<>(Arrays.asList(topicConfig0, topicConfig1));
        Set<Broker> brokers = getBrokers(4, 4, 4);

        brokers = strategy.balance(topics, brokers);
        Set<Broker> assignedBrokers0 = getAssignedBrokersForTopic(brokers, topicConfig0.getTopic());
        Set<Broker> assignedBrokers1 = getAssignedBrokersForTopic(brokers, topicConfig1.getTopic());
        assertEquals(3, assignedBrokers0.size());
        assertEquals(6, assignedBrokers1.size());
        assertLocalityBalance(assignedBrokers0);
        assertLocalityBalance(assignedBrokers1);

        Set<Broker> removed = removeBrokers(brokers, "a", 3);
        Set<Broker> removed0 = Sets.intersection(removed, assignedBrokers0);
        Set<Broker> removed1 = Sets.intersection(removed, assignedBrokers1);

        assertEquals(9, brokers.size());

        long now = System.currentTimeMillis();
        while (System.currentTimeMillis() - now < expirationTime * 2) {
            brokers = strategy.balance(topics, brokers);
            assignedBrokers0 = getAssignedBrokersForTopic(brokers, topicConfig0.getTopic());
            assignedBrokers1 = getAssignedBrokersForTopic(brokers, topicConfig1.getTopic());
            assertEquals(3 - removed0.size(), assignedBrokers0.size());
            assertEquals(6 - removed1.size(), assignedBrokers1.size());
            Thread.sleep(50);
        }

        brokers.addAll(removed);

        brokers = strategy.balance(topics, brokers);
        assignedBrokers0 = getAssignedBrokersForTopic(brokers, topicConfig0.getTopic());
        assignedBrokers1 = getAssignedBrokersForTopic(brokers, topicConfig1.getTopic());
        assertEquals(3, assignedBrokers0.size());
        assertEquals(6, assignedBrokers1.size());
        assertLocalityBalance(assignedBrokers0);
        assertLocalityBalance(assignedBrokers1);

        Thread.sleep(expirationTime * 2);

        brokers = strategy.balance(topics, brokers);
        assignedBrokers0 = getAssignedBrokersForTopic(brokers, topicConfig0.getTopic());
        assignedBrokers1 = getAssignedBrokersForTopic(brokers, topicConfig1.getTopic());
        assertEquals(3, assignedBrokers0.size());
        assertEquals(6, assignedBrokers1.size());
        assertLocalityBalance(assignedBrokers0);
        assertLocalityBalance(assignedBrokers1);
    }

    @Test
    public void testMultiTopicInsufficientBrokersImbalancedAz() throws Exception {
        long expirationTime = 500;
        BalanceStrategy strategy = getExpirationBalanceStrategyWithFreeze(expirationTime);

        // 12 brokers, 2 topics with 450 and 900MB input traffic, topic0 has priority over topic1
        TopicConfig topicConfig0 = generateTopicConfig(0);
        topicConfig0.setInputTrafficMB(450);
        topicConfig0.setTopicOrder(0);
        TopicConfig topicConfig1 = generateTopicConfig(1);
        topicConfig1.setInputTrafficMB(900);
        topicConfig1.setTopicOrder(1000);

        Set<TopicConfig> topics = new HashSet<>(Arrays.asList(topicConfig0, topicConfig1));
        Set<Broker> brokers = getBrokers(3, 3, 3);

        brokers = strategy.balance(topics, brokers);
        Set<Broker> assignedBrokers0 = getAssignedBrokersForTopic(brokers, topicConfig0.getTopic());
        Set<Broker> assignedBrokers1 = getAssignedBrokersForTopic(brokers, topicConfig1.getTopic());
        assertEquals(3, assignedBrokers0.size());
        assertEquals(6, assignedBrokers1.size());
        assertLocalityBalance(assignedBrokers0);
        assertLocalityBalance(assignedBrokers1);

        Set<Broker> removed = removeBrokers(brokers, "a", 2);
        Set<Broker> removed0 = Sets.intersection(removed, assignedBrokers0);
        Set<Broker> removed1 = Sets.intersection(removed, assignedBrokers1);

        assertEquals(7, brokers.size());
        assertTrue(removed0.size() + removed1.size() > 0);

        long now = System.currentTimeMillis();
        while (System.currentTimeMillis() - now < expirationTime * 2) {
            brokers = strategy.balance(topics, brokers);
            assignedBrokers0 = getAssignedBrokersForTopic(brokers, topicConfig0.getTopic());
            assignedBrokers1 = getAssignedBrokersForTopic(brokers, topicConfig1.getTopic());
            assertEquals(3 - removed0.size(), assignedBrokers0.size());
            assertEquals(6 - removed1.size(), assignedBrokers1.size());
            Thread.sleep(50);
        }

        brokers.addAll(removed);

        brokers = strategy.balance(topics, brokers);
        assignedBrokers0 = getAssignedBrokersForTopic(brokers, topicConfig0.getTopic());
        assignedBrokers1 = getAssignedBrokersForTopic(brokers, topicConfig1.getTopic());
        assertEquals(3, assignedBrokers0.size());
        assertEquals(6, assignedBrokers1.size());
        assertLocalityBalance(assignedBrokers0);
        assertLocalityBalance(assignedBrokers1);
    }

    private static BalanceStrategy getExpirationBalanceStrategyWithFreeze(long expirationTime) {
        ExpirationPartitionBalanceStrategyWithAssignmentFreeze strategy = new ExpirationPartitionBalanceStrategyWithAssignmentFreeze();
        strategy.setDefaultExpirationTime(expirationTime); // 500ms
        return strategy;
    }

    private static BalanceStrategy getExpirationBalanceStrategy(long expirationTime) {
        ExpirationPartitionBalanceStrategy strategy = new ExpirationPartitionBalanceStrategy();
        strategy.setDefaultExpirationTime(expirationTime); // 500ms
        return strategy;
    }

    private static Set<Broker> getBrokers(int numBrokersA, int numBrokersB, int numBrokersC) throws Exception {
        Set<Broker> brokers = new HashSet<>();
        for (int i = 1; i <= numBrokersA + numBrokersB + numBrokersC; i++) {
            if (i % 3 == 1) {
                brokers.add(generateBroker(i, "a"));
            } else if (i % 3 == 2) {
                brokers.add(generateBroker(i, "b"));
            } else {
                brokers.add(generateBroker(i, "c"));
            }
        }
        return brokers;
    }

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

    private static Set<Broker> getAssignedBrokersForTopic(Set<Broker> brokers, String topic) {
        Set<Broker> assignedBrokers = new HashSet<>();
        for (Broker broker : brokers) {
            for (TopicAssignment topicAssignment : broker.getAssignedTopics()) {
                if (topicAssignment.getTopic().equals(topic)) {
                    assignedBrokers.add(broker);
                    break;
                }
            }
        }
        return assignedBrokers;
    }

    private static Set<Broker> removeBrokers(Set<Broker> brokers, String localityOfBrokersToRemove, int numBrokersToRemove) {
        Set<Broker> brokersToRemove = new HashSet<>();
        for (Broker broker : brokers) {
            if (broker.getLocality().equals("us-east-1" + localityOfBrokersToRemove)) {
                brokersToRemove.add(broker);
                if (brokersToRemove.size() == numBrokersToRemove) {
                    break;
                }
            }
        }
        brokers.removeAll(brokersToRemove);
        return brokersToRemove;
    }

    private static Set<Broker> removeBrokers(Set<Broker> brokers, String ipToRemove) {
        Set<Broker> brokersToRemove = new HashSet<>();
        for (Broker broker : brokers) {
            if (broker.getBrokerIP().equals(ipToRemove)) {
                brokersToRemove.add(broker);
            }
        }
        brokers.removeAll(brokersToRemove);
        return brokersToRemove;
    }

    private static void assertLocalityBalance(Set<Broker> brokers) {
        Map<String, Integer> localityCount = new HashMap<>();
        for (Broker broker: brokers) {
            localityCount.put(broker.getLocality(), localityCount.getOrDefault(broker.getLocality(), 0) + 1);
        }
        assertEquals(1, localityCount.values().stream().collect(Collectors.toSet()).size());
    }
}
