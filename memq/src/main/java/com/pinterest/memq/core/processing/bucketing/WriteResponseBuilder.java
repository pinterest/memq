/**
 * Copyright 2022 Pinterest, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.memq.core.processing.bucketing;

import com.pinterest.memq.commons.protocol.ResponseCodes;
import com.pinterest.memq.commons.protocol.WriteResponsePacket;
import com.pinterest.memq.core.eviction.EvictionManager;
import com.pinterest.memq.core.eviction.EvictionResult;
import com.pinterest.memq.core.slot.SlotManager;

import java.util.logging.Logger;

/**
 * Builds the v4-aware {@link WriteResponsePacket} returned to write requests.
 * <p>
 * v3 producers receive an empty packet (the v4 fields are not serialized at
 * protocolVersion &lt; 4 anyway). v4 producers receive their current slot
 * ownership and, if available, an eviction directive.
 * <p>
 * This logic is shared between two ack paths so v4 features work regardless
 * of {@code disableAcks}:
 * <ul>
 *   <li>{@link Batch} acks (the {@code disableAcks=false} path)</li>
 *   <li>{@link BucketingTopicProcessor#write} immediate ack (the
 *       {@code disableAcks=true} path)</li>
 * </ul>
 */
public final class WriteResponseBuilder {

  private static final Logger logger = Logger.getLogger(WriteResponseBuilder.class.getName());

  private WriteResponseBuilder() {
  }

  public static WriteResponsePacket build(String producerId,
                                          short clientProtocolVersion,
                                          short responseCode,
                                          EvictionManager em,
                                          SlotManager sm) {
    if (clientProtocolVersion < 4 || responseCode != ResponseCodes.OK) {
      return new WriteResponsePacket();
    }
    if (producerId == null || producerId.isEmpty()) {
      return new WriteResponsePacket();
    }

    if (em != null) {
      EvictionResult eviction = em.pollEviction(producerId);
      if (eviction != null && sm != null) {
        for (String topic : sm.getProducerTopics(producerId)) {
          sm.releaseProducerSlots(producerId, topic, eviction.getNumSlotsToEvict());
        }
        int remaining = sm.getTotalProducerSlots(producerId);
        logger.info("Eviction delivered to producer=" + producerId
            + " target=" + eviction.getTargetBrokerIp()
            + " slotsToEvict=" + eviction.getNumSlotsToEvict()
            + " remainingSlotsForProducer=" + remaining);
        return new WriteResponsePacket(eviction.getTargetBrokerIp(),
            eviction.getNumSlotsToEvict(), remaining);
      }
    }

    int slotsOwned = sm != null ? sm.getTotalProducerSlots(producerId) : 0;
    return new WriteResponsePacket(null, 0, slotsOwned);
  }
}
