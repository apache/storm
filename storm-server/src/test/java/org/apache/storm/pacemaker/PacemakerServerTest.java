/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.pacemaker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.generated.HBMessage;
import org.apache.storm.generated.HBMessageData;
import org.apache.storm.generated.HBServerMessageType;
import org.apache.storm.messaging.netty.ControlMessage;
import org.apache.storm.messaging.netty.ISaslClient;
import org.apache.storm.messaging.netty.ISaslServer;
import org.apache.storm.messaging.netty.SaslMessageToken;
import org.apache.storm.messaging.netty.SaslStormClientHandler;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.pacemaker.codec.ThriftDecoder;
import org.apache.storm.pacemaker.codec.ThriftEncoder;
import org.apache.storm.pacemaker.codec.ThriftNettyServerCodec;
import org.apache.storm.pacemaker.codec.ThriftNettyServerCodec.AuthMethod;
import org.apache.storm.shade.io.netty.buffer.ByteBuf;
import org.apache.storm.shade.io.netty.buffer.Unpooled;
import org.apache.storm.shade.io.netty.channel.Channel;
import org.apache.storm.shade.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("deprecation")
public class PacemakerServerTest {

    private static final int MAX_LENGTH = 1024 * 1024;

    private static PacemakerServer server;

    private static Map<String, Object> config() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.PACEMAKER_PORT, 0);
        conf.put(Config.PACEMAKER_AUTH_METHOD, "NONE");
        conf.put(DaemonConfig.PACEMAKER_MAX_THREADS, 1);
        conf.put(Config.PACEMAKER_THRIFT_MESSAGE_SIZE_MAX, MAX_LENGTH);
        return conf;
    }

    private static ByteBuf frame(byte[] serialized) {
        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(serialized.length);
        buf.writeBytes(serialized);
        return buf;
    }

    private static ByteBuf frame(HBMessage message) {
        return frame(Utils.thriftSerialize(message));
    }

    private static ByteBuf controlFrame(ControlMessage controlMessage) {
        ByteBuf buf = Unpooled.buffer();
        controlMessage.write(buf);
        byte[] blob = new byte[buf.readableBytes()];
        buf.readBytes(blob);
        return frame(new HBMessage(HBServerMessageType.CONTROL_MESSAGE, HBMessageData.message_blob(blob)));
    }

    private static ByteBuf saslTokenFrame(short identifier, int declaredPayloadLen, byte[] payload) {
        ByteBuf buf = Unpooled.buffer();
        buf.writeShort(identifier);
        buf.writeInt(declaredPayloadLen);
        if (payload != null) {
            buf.writeBytes(payload);
        }
        byte[] blob = new byte[buf.readableBytes()];
        buf.readBytes(blob);
        return frame(new HBMessage(HBServerMessageType.SASL_MESSAGE_TOKEN, HBMessageData.message_blob(blob)));
    }

    private static HBMessage readResponse(EmbeddedChannel serverChannel) {
        EmbeddedChannel decoder = new EmbeddedChannel(new ThriftDecoder(MAX_LENGTH));
        Object out;
        while ((out = serverChannel.readOutbound()) != null) {
            decoder.writeInbound(out);
        }
        return decoder.readInbound();
    }

    private EmbeddedChannel pipeline() {
        return new EmbeddedChannel(new ThriftNettyServerCodec(server, config(), AuthMethod.NONE, MAX_LENGTH));
    }

    @BeforeAll
    public static void setUp() {
        server = new PacemakerServer(new Pacemaker(new ConcurrentHashMap<>(), new StormMetricsRegistry()), config());
    }

    @AfterAll
    public static void tearDown() {
        server.close();
    }

    @Test
    public void heartbeatRequestIsAnswered() {
        EmbeddedChannel channel = pipeline();
        HBMessage request = new HBMessage(HBServerMessageType.CREATE_PATH, HBMessageData.path("/path"));
        request.set_message_id(7);

        channel.writeInbound(frame(request));

        HBMessage response = readResponse(channel);
        assertEquals(HBServerMessageType.CREATE_PATH_RESPONSE, response.get_type());
        assertEquals(7, response.get_message_id());
        assertTrue(channel.isActive());
    }

    @Test
    public void controlFrameClosesOnlyThatConnection() {
        EmbeddedChannel other = pipeline();
        EmbeddedChannel channel = pipeline();

        channel.writeInbound(controlFrame(ControlMessage.CLOSE_MESSAGE));

        assertFalse(channel.isActive());
        assertNull(channel.readOutbound());

        other.writeInbound(frame(new HBMessage(HBServerMessageType.CREATE_PATH, HBMessageData.path("/path"))));
        assertEquals(HBServerMessageType.CREATE_PATH_RESPONSE, readResponse(other).get_type());
        assertTrue(other.isActive());
    }

    @Test
    public void oversizedSaslTokenClosesOnlyThatConnection() {
        EmbeddedChannel other = pipeline();
        EmbeddedChannel channel = pipeline();

        // A SASL token frame that declares a ~2GB payload previously OOM'd the decoder and terminated the daemon.
        channel.writeInbound(saslTokenFrame(SaslMessageToken.IDENTIFIER, Integer.MAX_VALUE, null));

        assertFalse(channel.isActive());
        assertNull(channel.readOutbound());

        other.writeInbound(frame(new HBMessage(HBServerMessageType.CREATE_PATH, HBMessageData.path("/path"))));
        assertEquals(HBServerMessageType.CREATE_PATH_RESPONSE, readResponse(other).get_type());
        assertTrue(other.isActive());
    }

    @Test
    public void saslFrameWithoutAuthenticationConfiguredClosesConnection() {
        // Passes the decoder, but no SASL handler is installed when the auth method is NONE.
        EmbeddedChannel channel = pipeline();

        channel.writeInbound(controlFrame(ControlMessage.SASL_TOKEN_MESSAGE_REQUEST));

        assertFalse(channel.isActive());
        assertNull(channel.readOutbound());
    }

    @Test
    public void receivedDropsUnexpectedMessageType() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel();

        server.received(ControlMessage.CLOSE_MESSAGE, "remote", channel);

        assertFalse(channel.isActive());
        assertNull(channel.readOutbound());
    }

    @Test
    public void undecodableFrameClosesConnection() {
        EmbeddedChannel channel = pipeline();

        channel.writeInbound(frame(new byte[]{ 0x7f, 0x7f, 0x7f, 0x7f }));

        assertFalse(channel.isActive());
    }

    @Test
    public void requestFailureClosesConnection() {
        EmbeddedChannel channel = pipeline();

        channel.writeInbound(frame(new HBMessage(HBServerMessageType.SEND_PULSE, null)));

        assertFalse(channel.isActive());
        assertNull(channel.readOutbound());
    }

    @Test
    public void digestHandshakeAuthenticatesChannel() throws Exception {
        ISaslServer saslServer = mock(ISaslServer.class);
        when(saslServer.name()).thenReturn("pacemaker_server");
        when(saslServer.secretKey()).thenReturn("secret");
        ISaslClient saslClient = mock(ISaslClient.class);
        when(saslClient.name()).thenReturn("pacemaker_server");
        when(saslClient.secretKey()).thenReturn("secret");

        EmbeddedChannel serverChannel = new EmbeddedChannel(
            new ThriftNettyServerCodec(saslServer, config(), AuthMethod.DIGEST, MAX_LENGTH));
        EmbeddedChannel clientChannel = new EmbeddedChannel(
            new ThriftEncoder(), new ThriftDecoder(MAX_LENGTH), new SaslStormClientHandler(saslClient));

        exchange(clientChannel, serverChannel);

        verify(saslServer).authenticated(serverChannel);
        verify(saslClient, atLeastOnce()).channelReady(any(Channel.class));
        assertNull(serverChannel.pipeline().get(ThriftNettyServerCodec.SASL_HANDLER));
        assertTrue(serverChannel.isActive());
        assertTrue(clientChannel.isActive());
        verify(saslServer, never()).received(any(), anyString(), any(Channel.class));

        HBMessage request = new HBMessage(HBServerMessageType.GET_PULSE, HBMessageData.path("/path"));
        serverChannel.writeInbound(frame(request));
        verify(saslServer).received(request, serverChannel.remoteAddress().toString(), serverChannel);

        // Control frames other than the handshake request are still refused once authenticated.
        serverChannel.writeInbound(controlFrame(ControlMessage.EOB_MESSAGE));
        assertFalse(serverChannel.isActive());
    }

    private static void exchange(EmbeddedChannel client, EmbeddedChannel server) {
        boolean progress = true;
        while (progress) {
            progress = forward(client, server) | forward(server, client);
        }
    }

    private static boolean forward(EmbeddedChannel from, EmbeddedChannel to) {
        List<Object> frames = new ArrayList<>();
        Object out;
        while ((out = from.readOutbound()) != null) {
            frames.add(out);
        }
        for (Object frame : frames) {
            to.writeInbound(frame);
        }
        return !frames.isEmpty();
    }
}
