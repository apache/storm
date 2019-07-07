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

package org.apache.storm.pacemaker.codec;

import java.util.List;
import org.apache.storm.generated.HBMessage;
import org.apache.storm.generated.HBMessageData;
import org.apache.storm.generated.HBServerMessageType;
import org.apache.storm.messaging.netty.ControlMessage;
import org.apache.storm.messaging.netty.INettySerializable;
import org.apache.storm.messaging.netty.SaslMessageToken;
import org.apache.storm.shade.io.netty.buffer.ByteBuf;
import org.apache.storm.shade.io.netty.buffer.ByteBufAllocator;
import org.apache.storm.shade.io.netty.buffer.Unpooled;
import org.apache.storm.shade.io.netty.channel.ChannelHandlerContext;
import org.apache.storm.shade.io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftEncoder extends MessageToMessageEncoder<Object> {

    private static final Logger LOG = LoggerFactory
        .getLogger(ThriftEncoder.class);

    private HBMessage encodeNettySerializable(ByteBufAllocator alloc,
        INettySerializable nettyMessage, HBServerMessageType serverMessageType) {

        HBMessageData messageData = new HBMessageData();
        HBMessage m = new HBMessage();
        byte[] messageBuffer = new byte[nettyMessage.encodeLength()];
        ByteBuf wrappedBuffer = Unpooled.wrappedBuffer(messageBuffer);
        try {
            wrappedBuffer.resetWriterIndex();
            nettyMessage.write(wrappedBuffer);
            
            messageData.set_message_blob(messageBuffer);
            m.set_type(serverMessageType);
            m.set_data(messageData);
            return m;
        } finally {
            wrappedBuffer.release();
        }
    }

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Object msg, List<Object> out) throws Exception {
        if (msg == null) {
            return;
        }

        LOG.debug("Trying to encode: " + msg.getClass().toString() + " : " + msg.toString());

        HBMessage message;
        ByteBufAllocator alloc = channelHandlerContext.alloc();
        if (msg instanceof INettySerializable) {
            INettySerializable nettyMsg = (INettySerializable) msg;

            HBServerMessageType type;
            if (msg instanceof ControlMessage) {
                type = HBServerMessageType.CONTROL_MESSAGE;
            } else if (msg instanceof SaslMessageToken) {
                type = HBServerMessageType.SASL_MESSAGE_TOKEN;
            } else {
                LOG.error("Didn't recognise INettySerializable: " + nettyMsg.toString());
                throw new RuntimeException("Unrecognized INettySerializable.");
            }
            message = encodeNettySerializable(alloc, nettyMsg, type);
        } else {
            message = (HBMessage) msg;
        }

        try {
            byte[] serialized = Utils.thriftSerialize(message);
            ByteBuf ret = alloc.ioBuffer(serialized.length + 4);

            ret.writeInt(serialized.length);
            ret.writeBytes(serialized);

            out.add(ret);
        } catch (RuntimeException e) {
            LOG.error("Failed to serialize.", e);
            throw e;
        }
    }
}
