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

import java.io.IOException;
import java.util.List;
import org.apache.storm.generated.HBMessage;
import org.apache.storm.generated.HBServerMessageType;
import org.apache.storm.messaging.netty.ControlMessage;
import org.apache.storm.messaging.netty.SaslMessageToken;
import org.apache.storm.shade.io.netty.buffer.ByteBuf;
import org.apache.storm.shade.io.netty.channel.ChannelHandlerContext;
import org.apache.storm.shade.io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes length-prefixed thrift {@link HBMessage} frames of the Pacemaker protocol.
 *
 * @deprecated Pacemaker is deprecated and only kept for backward compatibility; it will be removed in a future release.
 *     Use the default heartbeat path instead: workers heartbeat to their supervisor, which reports them to Nimbus over
 *     Thrift, with the default ZooKeeper-based cluster state store ({@code org.apache.storm.cluster.ZKStateStorageFactory}).
 */
@Deprecated
public class ThriftDecoder extends ByteToMessageDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(ThriftDecoder.class);
    private static final int INTEGER_SIZE = 4;

    /**
     * The maximum length in bytes that a serialized thrift message is allowed to be.
     */
    private final int maxLength;

    /**
     * Whether this decoder sits in a Pacemaker server pipeline. A server only accepts the control message a client
     * sends to start the SASL handshake; any other control frame is dropped and the connection closed.
     */
    private final boolean serverSide;

    /**
     * Instantiate a ThriftDecoder that accepts serialized messages of at most maxLength bytes.
     */
    public ThriftDecoder(final int maxLengthBytes) {
        this(maxLengthBytes, false);
    }

    /**
     * Instantiate a ThriftDecoder that accepts serialized messages of at most maxLength bytes.
     *
     * @param maxLengthBytes the maximum length of a serialized thrift message
     * @param serverSide true if the decoder is used by a Pacemaker server, which restricts the control messages it accepts
     */
    public ThriftDecoder(final int maxLengthBytes, final boolean serverSide) {
        maxLength = maxLengthBytes;
        this.serverSide = serverSide;
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf buf, List<Object> out) throws Exception {
        long available = buf.readableBytes();
        if (available < INTEGER_SIZE) {
            return;
        }

        buf.markReaderIndex();

        int thriftLen = buf.readInt();
        if (thriftLen < 0 || thriftLen > maxLength) {
            throw new IOException("Thrift message of length " + Integer.toString(thriftLen)
                                  + " is greater than allowed " + maxLength
                                  + " or less than 0.");
        }

        available -= INTEGER_SIZE;

        if (available < thriftLen) {
            // We haven't received the entire object yet, return and wait for more bytes.
            buf.resetReaderIndex();
            return;
        }

        byte[] serialized = new byte[thriftLen];
        buf.readBytes(serialized, 0, thriftLen);
        HBMessage m = (HBMessage) Utils.thriftDeserialize(HBMessage.class, serialized);

        if (m.get_type() == HBServerMessageType.CONTROL_MESSAGE) {
            ControlMessage cm = readControlMessage(m);
            if (cm == null) {
                if (!serverSide) {
                    // Let the client handler see the failure so that it reconnects.
                    throw new IOException("Received a malformed control message");
                }
                dropAndClose(channelHandlerContext, buf, "a malformed control frame");
                return;
            }
            if (serverSide && cm != ControlMessage.SASL_TOKEN_MESSAGE_REQUEST) {
                dropAndClose(channelHandlerContext, buf, "an unexpected control frame " + cm);
                return;
            }
            out.add(cm);
        } else if (m.get_type() == HBServerMessageType.SASL_MESSAGE_TOKEN) {
            SaslMessageToken sm = readSaslMessageToken(m);
            if (sm == null) {
                if (!serverSide) {
                    // Let the client handler see the failure so that it reconnects.
                    throw new IOException("Received a malformed SASL token message");
                }
                dropAndClose(channelHandlerContext, buf, "a malformed SASL token frame");
                return;
            }
            out.add(sm);
        } else {
            out.add(m);
        }
    }

    private static ControlMessage readControlMessage(HBMessage m) {
        if (m.get_data() == null || !m.get_data().is_set_message_blob()) {
            return null;
        }
        byte[] blob = m.get_data().get_message_blob();
        if (blob == null || blob.length < 2) {
            return null;
        }
        return ControlMessage.read(blob);
    }

    private static SaslMessageToken readSaslMessageToken(HBMessage m) {
        if (m.get_data() == null || !m.get_data().is_set_message_blob()) {
            return null;
        }
        byte[] blob = m.get_data().get_message_blob();
        if (blob == null) {
            return null;
        }
        return SaslMessageToken.read(blob);
    }

    private static void dropAndClose(ChannelHandlerContext ctx, ByteBuf buf, String what) {
        LOG.warn("Channel {} sent {}; closing the connection", ctx.channel(), what);
        buf.skipBytes(buf.readableBytes());
        ctx.close();
    }
}
