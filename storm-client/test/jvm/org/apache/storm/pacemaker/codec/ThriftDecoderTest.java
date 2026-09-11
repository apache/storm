/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.pacemaker.codec;

import org.apache.storm.generated.HBMessage;
import org.apache.storm.generated.HBMessageData;
import org.apache.storm.generated.HBServerMessageType;
import org.apache.storm.messaging.netty.ControlMessage;
import org.apache.storm.messaging.netty.SaslMessageToken;
import org.apache.storm.shade.io.netty.buffer.ByteBuf;
import org.apache.storm.shade.io.netty.buffer.Unpooled;
import org.apache.storm.shade.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.storm.shade.io.netty.handler.codec.DecoderException;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("deprecation")
public class ThriftDecoderTest {

    private static final int MAX_LENGTH = 1024 * 1024;

    static ByteBuf frame(HBMessage message) {
        byte[] serialized = Utils.thriftSerialize(message);
        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(serialized.length);
        buf.writeBytes(serialized);
        return buf;
    }

    static ByteBuf controlFrame(ControlMessage controlMessage) {
        ByteBuf blob = Unpooled.buffer();
        controlMessage.write(blob);
        byte[] bytes = new byte[blob.readableBytes()];
        blob.readBytes(bytes);
        return frame(new HBMessage(HBServerMessageType.CONTROL_MESSAGE, HBMessageData.message_blob(bytes)));
    }

    private static EmbeddedChannel serverChannel() {
        return new EmbeddedChannel(new ThriftDecoder(MAX_LENGTH, true));
    }

    @Test
    public void serverDropsUnexpectedControlFrames() {
        for (ControlMessage controlMessage : ControlMessage.values()) {
            if (controlMessage == ControlMessage.SASL_TOKEN_MESSAGE_REQUEST) {
                continue;
            }
            EmbeddedChannel channel = serverChannel();

            channel.writeInbound(controlFrame(controlMessage));

            assertNull(channel.readInbound(), controlMessage.name());
            assertFalse(channel.isActive(), controlMessage.name());
        }
    }

    @Test
    public void serverAcceptsSaslTokenMessageRequest() {
        EmbeddedChannel channel = serverChannel();

        channel.writeInbound(controlFrame(ControlMessage.SASL_TOKEN_MESSAGE_REQUEST));

        assertSame(ControlMessage.SASL_TOKEN_MESSAGE_REQUEST, channel.readInbound());
        assertTrue(channel.isActive());
    }

    @Test
    public void serverAcceptsSaslMessageToken() {
        EmbeddedChannel channel = serverChannel();
        byte[] token = { 1, 2, 3 };
        ByteBuf blob = Unpooled.buffer();
        new SaslMessageToken(token).write(blob);
        byte[] bytes = new byte[blob.readableBytes()];
        blob.readBytes(bytes);

        channel.writeInbound(frame(new HBMessage(HBServerMessageType.SASL_MESSAGE_TOKEN, HBMessageData.message_blob(bytes))));

        SaslMessageToken decoded = channel.readInbound();
        assertArrayEquals(token, decoded.getSaslToken());
        assertTrue(channel.isActive());
    }

    @Test
    public void serverPassesHeartbeatMessages() {
        EmbeddedChannel channel = serverChannel();
        HBMessage message = new HBMessage(HBServerMessageType.CREATE_PATH, HBMessageData.path("/path"));

        channel.writeInbound(frame(message));

        assertEquals(message, channel.readInbound());
        assertTrue(channel.isActive());
    }

    @Test
    public void serverDropsControlFrameWithUnknownCode() {
        EmbeddedChannel channel = serverChannel();

        channel.writeInbound(frame(new HBMessage(HBServerMessageType.CONTROL_MESSAGE,
                                                 HBMessageData.message_blob(new byte[]{ 0, 1 }))));

        assertNull(channel.readInbound());
        assertFalse(channel.isActive());
    }

    @Test
    public void serverDropsControlFrameWithoutPayload() {
        EmbeddedChannel channel = serverChannel();

        channel.writeInbound(frame(new HBMessage(HBServerMessageType.CONTROL_MESSAGE, null)));

        assertNull(channel.readInbound());
        assertFalse(channel.isActive());
    }

    @Test
    public void serverDropsControlFrameWithShortPayload() {
        EmbeddedChannel channel = serverChannel();

        channel.writeInbound(frame(new HBMessage(HBServerMessageType.CONTROL_MESSAGE,
                                                 HBMessageData.message_blob(new byte[]{ 1 }))));

        assertNull(channel.readInbound());
        assertFalse(channel.isActive());
    }

    @Test
    public void clientDecodesSaslCompleteRequest() {
        EmbeddedChannel channel = new EmbeddedChannel(new ThriftDecoder(MAX_LENGTH));

        channel.writeInbound(controlFrame(ControlMessage.SASL_COMPLETE_REQUEST));

        assertSame(ControlMessage.SASL_COMPLETE_REQUEST, channel.readInbound());
        assertTrue(channel.isActive());
    }

    @Test
    public void clientReportsMalformedControlFrame() {
        EmbeddedChannel channel = new EmbeddedChannel(new ThriftDecoder(MAX_LENGTH));

        assertThrows(DecoderException.class, () -> channel.writeInbound(
            frame(new HBMessage(HBServerMessageType.CONTROL_MESSAGE, null))));
        assertNull(channel.readInbound());
    }
}
