package com.pinterest.memq.core.clustering;

import com.pinterest.memq.commons.protocol.Broker;
import com.pinterest.memq.commons.protocol.TopicAssignment;
import com.pinterest.memq.commons.protocol.TopicConfig;
import com.pinterest.memq.core.config.MemqConfig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

public class ExpirationPartitionBalanceStrategyWithAssignmentFreeze extends ExpirationPartitionBalanceStrategyWithErrorHandling {

    private static final Logger logger =
        Logger.getLogger(ExpirationPartitionBalanceStrategyWithAssignmentFreeze.class.getName());

    public ExpirationPartitionBalanceStrategyWithAssignmentFreeze(MemqConfig memqConfig) {
        super(memqConfig);
    }

    /**
     * Use the existing assignment and send alert.
     * Refresher the topic assignment timestamp.
     * @param topics
     * @param brokers
     * @return brokers
     */
    @Override
    protected Set<Broker> handleBalancerError(Set<TopicConfig> topics, Set<Broker> brokers) {
        logger.info("Trigger assignment freeze and send alert. Current assignment: " + brokers);
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
