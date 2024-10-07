package com.pinterest.memq.core.clustering;

import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.TopicConfig;

import java.util.HashSet;
import java.util.Set;

public class ExpirationPartitionBalanceStrategyWithAssignmentFreeze extends ExpirationPartitionBalanceStrategyWithErrorHandling {

    /**
     * Use the existing assignment and send alert
     * The assignment may in expired state but the assignment is not changed.
     * @param topics
     * @param brokers
     * @return brokers
     */
    @Override
    protected Set<Broker> handleBalancerError(Set<TopicConfig> topics, Set<Broker> brokers) {
        this.sendAlert();
        return new HashSet<>(brokers);
    }
}
