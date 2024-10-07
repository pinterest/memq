package com.pinterest.memq.core.clustering;

import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.TopicAssignment;
import com.pinterest.memq.commons.protocol.TopicConfig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExpirationPartitionBalanceStrategyWithAssignmentFreeze extends ExpirationPartitionBalanceStrategyWithErrorHandling {

    /**
     * Use the existing assignment and send alert.
     * Refresher the topic assignment timestamp.
     * @param topics
     * @param brokers
     * @return brokers
     */
    @Override
    protected Set<Broker> handleBalancerError(Set<TopicConfig> topics, Set<Broker> brokers) {
        System.out.println("[TEST] ExpirationPartitionBalanceStrategyWithAssignmentFreeze");
        this.sendAlert();
        List<Broker> brokerList = new ArrayList<>(brokers);
        for (Broker broker : brokerList) {
            for (TopicAssignment topicAssignment : broker.getAssignedTopics()) {
                topicAssignment.setAssignmentTimestamp(System.currentTimeMillis());
            }
        }
        return new HashSet<>(brokers);
    }
}
