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
package com.pinterest.memq.core.load;

import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.core.processing.TopicProcessor;

import io.netty.channel.ChannelHandlerContext;

import java.util.Properties;

public interface BalancingStrategy {
  void configure(Properties configs);
  LoadBalancer.Action evaluate(RequestPacket rawPacket, ChannelHandlerContext context, TopicProcessor topicProcessor);
  default void cleanup() {

  }
}
