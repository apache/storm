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

package org.apache.storm.messaging.netty;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.serialization.KryoValuesDeserializer;
import org.apache.storm.shade.io.netty.buffer.ByteBuf;
import org.apache.storm.shade.io.netty.buffer.Unpooled;
import org.apache.storm.shade.io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

public class MessageDecoderTest {

    private static final short TASK_ID = 1;
    private static final int HUGE_LENGTH = 0x7FFFFFFC;

    private static ByteBuf backPressureFrame() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeShort(BackPressureStatus.IDENTIFIER);
        byte[] payload = { 1, 2, 3, 4 };
        buf.writeInt(payload.length);
        buf.writeBytes(payload);
        return buf;
    }

    private static ByteBuf frameHeader(short code, int declaredLength) {
        ByteBuf buf = Unpooled.buffer();
        buf.writeShort(code);
        buf.writeInt(declaredLength);
        return buf;
    }

    private static ByteBuf taskMessageFrame(byte[] payload) {
        ByteBuf buf = frameHeader(TASK_ID, payload.length);
        buf.writeBytes(payload);
        return buf;
    }

    private static ByteBuf saslTokenFrame(byte[] token) {
        ByteBuf buf = frameHeader(SaslMessageToken.IDENTIFIER, token.length);
        buf.writeBytes(token);
        return buf;
    }

    private static EmbeddedChannel authenticatedChannel(KryoValuesDeserializer deser) {
        EmbeddedChannel channel = new EmbeddedChannel(new MessageDecoder(deser, true));
        SaslNettyServer saslNettyServer = mock(SaslNettyServer.class);
        when(saslNettyServer.isComplete()).thenReturn(true);
        channel.attr(SaslNettyServerState.SASL_NETTY_SERVER).set(saslNettyServer);
        return channel;
    }

    @Test
    public void backPressureFrameIsNotDeserializedBeforeAuthentication() {
        KryoValuesDeserializer deser = mock(KryoValuesDeserializer.class);
        EmbeddedChannel channel = new EmbeddedChannel(new MessageDecoder(deser, true));

        channel.writeInbound(backPressureFrame());

        assertNull(channel.readInbound());
        assertFalse(channel.isActive());
        verifyNoInteractions(deser);
    }

    @Test
    public void taskMessageFrameIsNotBufferedBeforeAuthentication() {
        KryoValuesDeserializer deser = mock(KryoValuesDeserializer.class);
        EmbeddedChannel channel = new EmbeddedChannel(new MessageDecoder(deser, true));

        channel.writeInbound(frameHeader(TASK_ID, HUGE_LENGTH));

        assertNull(channel.readInbound());
        assertFalse(channel.isActive());
    }

    @Test
    public void oversizedSaslTokenFrameIsNotBufferedBeforeAuthentication() {
        KryoValuesDeserializer deser = mock(KryoValuesDeserializer.class);
        EmbeddedChannel channel = new EmbeddedChannel(new MessageDecoder(deser, true));

        channel.writeInbound(frameHeader(SaslMessageToken.IDENTIFIER, HUGE_LENGTH));

        assertNull(channel.readInbound());
        assertFalse(channel.isActive());
    }

    @Test
    public void saslHandshakeFramesAreAcceptedBeforeAuthentication() {
        KryoValuesDeserializer deser = mock(KryoValuesDeserializer.class);
        EmbeddedChannel channel = new EmbeddedChannel(new MessageDecoder(deser, true));

        ByteBuf request = Unpooled.buffer();
        ControlMessage.SASL_TOKEN_MESSAGE_REQUEST.write(request);
        channel.writeInbound(request);
        assertSame(ControlMessage.SASL_TOKEN_MESSAGE_REQUEST, channel.readInbound());

        byte[] token = { 9, 8, 7 };
        channel.writeInbound(saslTokenFrame(token));

        SaslMessageToken decoded = channel.readInbound();
        assertArrayEquals(token, decoded.getSaslToken());
        assertTrue(channel.isActive());
    }

    @Test
    public void backPressureFrameIsDeserializedAfterAuthentication() {
        KryoValuesDeserializer deser = mock(KryoValuesDeserializer.class);
        BackPressureStatus status = new BackPressureStatus();
        when(deser.deserializeObject(any(byte[].class))).thenReturn(status);

        EmbeddedChannel channel = authenticatedChannel(deser);

        channel.writeInbound(backPressureFrame());

        assertSame(status, channel.readInbound());
        assertTrue(channel.isActive());
    }

    @Test
    public void taskMessageFrameIsDecodedAfterAuthentication() {
        KryoValuesDeserializer deser = mock(KryoValuesDeserializer.class);
        EmbeddedChannel channel = authenticatedChannel(deser);
        byte[] payload = { 4, 5, 6 };

        channel.writeInbound(taskMessageFrame(payload));

        List<Object> decoded = channel.readInbound();
        assertEquals(1, decoded.size());
        assertArrayEquals(payload, ((TaskMessage) decoded.get(0)).message());
        assertTrue(channel.isActive());
    }

    @Test
    public void framesAreDecodedWhenServerAuthenticationIsNotRequired() {
        KryoValuesDeserializer deser = mock(KryoValuesDeserializer.class);
        BackPressureStatus status = new BackPressureStatus();
        when(deser.deserializeObject(any(byte[].class))).thenReturn(status);

        EmbeddedChannel channel = new EmbeddedChannel(new MessageDecoder(deser, false));

        channel.writeInbound(backPressureFrame());

        assertSame(status, channel.readInbound());
        assertTrue(channel.isActive());
    }
}
