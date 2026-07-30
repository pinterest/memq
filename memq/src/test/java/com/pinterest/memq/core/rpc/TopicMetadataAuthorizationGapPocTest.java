package com.pinterest.memq.core.rpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.NotAuthorizedException;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.commons.protocol.RequestPacket;
import com.pinterest.memq.commons.protocol.RequestType;
import com.pinterest.memq.commons.protocol.ResponsePacket;
import com.pinterest.memq.commons.protocol.TopicMetadata;
import com.pinterest.memq.commons.protocol.TopicMetadataRequestPacket;
import com.pinterest.memq.commons.protocol.TopicMetadataResponsePacket;
import com.pinterest.memq.core.MemqManager;
import com.pinterest.memq.core.clustering.MemqGovernor;
import com.pinterest.memq.core.security.Authorizer;

import io.netty.channel.ChannelHandlerContext;

/**
 * Regression test for the authorization gap in
 * {@link PacketSwitchingHandler#authorize}: TOPIC_METADATA requests used to
 * skip the operator-configured {@link Authorizer} entirely (it only handled
 * WRITE/READ), so a principal denied on every topic could still list all
 * topic metadata -- including internal broker host sets and storage handler
 * config -- via an empty-topic-list "list all" request. The fix applies the
 * same Authorizer per topic (as a READ) both for the list-all case (silently
 * filtered) and for explicitly-named topics (rejected with
 * NotAuthorizedException, matching WRITE/READ's existing behavior).
 */
public class TopicMetadataAuthorizationGapPocTest {

  private MemqGovernor governorWithOneSecretTopic() {
    Properties storageHandlerConfig = new Properties();
    storageHandlerConfig.setProperty("s3.bucket", "internal-prod-memq-bucket");
    TopicMetadata secretTopicMetadata = new TopicMetadata(
        "payments-critical-topic", "s3", storageHandlerConfig);

    Map<String, TopicMetadata> topicMetadataMap = new HashMap<>();
    topicMetadataMap.put("payments-critical-topic", secretTopicMetadata);

    MemqGovernor governor = mock(MemqGovernor.class);
    when(governor.getTopicMetadataMap()).thenReturn(topicMetadataMap);
    return governor;
  }

  @Test
  public void listAll_filtersOutTopicsCallerIsNotAuthorizedFor() throws Exception {
    Authorizer denyAllAuthorizer = mock(Authorizer.class);
    when(denyAllAuthorizer.authorize(any(), anyString(), anyString(), any()))
        .thenReturn(false);

    MemqGovernor governor = governorWithOneSecretTopic();
    MemqManager mgr = mock(MemqManager.class);
    PacketSwitchingHandler handler = new PacketSwitchingHandler(
        mgr, governor, denyAllAuthorizer, new MetricRegistry());

    Principal noGrantsPrincipal = () -> "attacker-with-zero-grants";
    RequestPacket metadataRequest = new RequestPacket(
        RequestType.PROTOCOL_VERSION, 1L, RequestType.TOPIC_METADATA,
        new TopicMetadataRequestPacket(Collections.emptyList()));

    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    handler.handle(ctx, metadataRequest, noGrantsPrincipal, "10.0.0.99");

    // The authorizer WAS consulted this time, per topic, as a READ.
    verify(denyAllAuthorizer)
        .authorize(eq(noGrantsPrincipal), eq("10.0.0.99"), eq("payments-critical-topic"),
            eq(RequestType.READ));

    ArgumentCaptor<ResponsePacket> responseCaptor = ArgumentCaptor.forClass(ResponsePacket.class);
    verify(ctx).writeAndFlush(responseCaptor.capture());
    TopicMetadataResponsePacket payload =
        (TopicMetadataResponsePacket) responseCaptor.getValue().getPacket();
    // Filtered out: the caller is authorized for nothing.
    assertTrue(payload.getMetadataList().isEmpty());
  }

  @Test
  public void explicitTopic_rejectsUnauthorizedCallerInsteadOfLeakingMetadata() throws Exception {
    Authorizer denyAllAuthorizer = mock(Authorizer.class);
    when(denyAllAuthorizer.authorize(any(), anyString(), anyString(), any()))
        .thenReturn(false);

    MemqGovernor governor = governorWithOneSecretTopic();
    MemqManager mgr = mock(MemqManager.class);
    PacketSwitchingHandler handler = new PacketSwitchingHandler(
        mgr, governor, denyAllAuthorizer, new MetricRegistry());

    Principal noGrantsPrincipal = () -> "attacker-with-zero-grants";
    RequestPacket metadataRequest = new RequestPacket(
        RequestType.PROTOCOL_VERSION, 1L, RequestType.TOPIC_METADATA,
        new TopicMetadataRequestPacket("payments-critical-topic"));

    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

    try {
      handler.handle(ctx, metadataRequest, noGrantsPrincipal, "10.0.0.99");
      throw new AssertionError("expected NotAuthorizedException");
    } catch (NotAuthorizedException expected) {
      // expected
    }

    verify(ctx, never()).writeAndFlush(any());
  }

  @Test
  public void listAll_stillReturnsTopicsCallerIsAuthorizedFor() throws Exception {
    Authorizer allowAllAuthorizer = mock(Authorizer.class);
    when(allowAllAuthorizer.authorize(any(), anyString(), anyString(), any()))
        .thenReturn(true);

    MemqGovernor governor = governorWithOneSecretTopic();
    MemqManager mgr = mock(MemqManager.class);
    PacketSwitchingHandler handler = new PacketSwitchingHandler(
        mgr, governor, allowAllAuthorizer, new MetricRegistry());

    Principal legitimatePrincipal = () -> "legit-owner";
    RequestPacket metadataRequest = new RequestPacket(
        RequestType.PROTOCOL_VERSION, 1L, RequestType.TOPIC_METADATA,
        new TopicMetadataRequestPacket(Collections.emptyList()));

    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    handler.handle(ctx, metadataRequest, legitimatePrincipal, "10.0.0.5");

    ArgumentCaptor<ResponsePacket> responseCaptor = ArgumentCaptor.forClass(ResponsePacket.class);
    verify(ctx).writeAndFlush(responseCaptor.capture());
    TopicMetadataResponsePacket payload =
        (TopicMetadataResponsePacket) responseCaptor.getValue().getPacket();
    assertEquals(1, payload.getMetadataList().size());
  }
}
