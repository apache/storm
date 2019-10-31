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

import java.io.IOException;
import org.apache.storm.shade.io.netty.channel.Channel;
import org.apache.storm.shade.io.netty.channel.ChannelHandlerContext;
import org.apache.storm.shade.io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SaslStormServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory
        .getLogger(SaslStormServerHandler.class);
    private final ISaslServer server;
    /**
     * Used for client or server's token to send or receive from each other.
     */
    private byte[] token;
    private String topologyName;

    public SaslStormServerHandler(ISaslServer server) throws IOException {
        this.server = server;
        getSASLCredentials();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg == null) {
            return;
        }

        Channel channel = ctx.channel();

        if (msg instanceof ControlMessage
            && msg == ControlMessage.SASL_TOKEN_MESSAGE_REQUEST) {
            // initialize server-side SASL functionality, if we haven't yet
            // (in which case we are looking at the first SASL message from the
            // client).
            SaslNettyServer saslNettyServer = channel.attr(SaslNettyServerState.SASL_NETTY_SERVER).get();
            if (saslNettyServer == null) {
                LOG.debug("No saslNettyServer for " + channel
                          + " yet; creating now, with topology token: " + topologyName);
                try {
                    saslNettyServer = new SaslNettyServer(topologyName, token);
                    LOG.debug("SaslNettyServer for " + channel
                        + "created with topology token: " + topologyName);
                } catch (IOException e) {
                    LOG.error("Error occurred while creating saslNettyServer on server "
                              + channel.localAddress()
                              + " for client "
                              + channel.remoteAddress());
                    throw new IllegalStateException("Failed to set SaslNettyServerState.SASL_NETTY_SERVER");
                }

                channel.attr(SaslNettyServerState.SASL_NETTY_SERVER).set(saslNettyServer);
            } else {
                LOG.debug("Found existing saslNettyServer on server:"
                          + channel.localAddress() + " for client "
                          + channel.remoteAddress());
            }

            LOG.debug("processToken:  With nettyServer: " + saslNettyServer
                      + " and token length: " + token.length);

            SaslMessageToken saslTokenMessageRequest;
            saslTokenMessageRequest = new SaslMessageToken(
                saslNettyServer.response(new byte[0]));
            // Send response to client.
            channel.writeAndFlush(saslTokenMessageRequest, channel.voidPromise());
            // do not send upstream to other handlers: no further action needs
            // to be done for SASL_TOKEN_MESSAGE_REQUEST requests.
            return;
        }

        if (msg instanceof SaslMessageToken) {
            // initialize server-side SASL functionality, if we haven't yet
            // (in which case we are looking at the first SASL message from the
            // client).
            SaslNettyServer saslNettyServer = channel.attr(SaslNettyServerState.SASL_NETTY_SERVER).get();
            if (saslNettyServer == null) {
                throw new Exception("saslNettyServer was unexpectedly "
                                    + "null for channel: " + channel);
            }
            SaslMessageToken saslTokenMessageRequest = new SaslMessageToken(
                saslNettyServer.response(((SaslMessageToken) msg)
                                             .getSaslToken()));

            // Send response to client.
            channel.writeAndFlush(saslTokenMessageRequest, channel.voidPromise());

            if (saslNettyServer.isComplete()) {
                // If authentication of client is complete, we will also send a
                // SASL-Complete message to the client.
                LOG.debug("SASL authentication is complete for client with "
                          + "username: " + saslNettyServer.getUserName());
                channel.writeAndFlush(ControlMessage.SASL_COMPLETE_REQUEST, channel.voidPromise());
                LOG.debug("Removing SaslServerHandler from pipeline since SASL "
                          + "authentication is complete.");
                ctx.pipeline().remove(this);
                server.authenticated(channel);
            }
        } else {
            // Client should not be sending other-than-SASL messages before
            // SaslServerHandler has removed itself from the pipeline. Such
            // non-SASL requests will be denied by the Authorize channel handler
            // (the next handler upstream in the server pipeline) if SASL
            // authentication has not completed.
            LOG.warn("Sending upstream an unexpected non-SASL message :  "
                     + msg);
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    private void getSASLCredentials() throws IOException {
        String secretKey;
        topologyName = server.name();
        secretKey = server.secretKey();

        if (secretKey != null) {
            token = secretKey.getBytes();
        }

        LOG.debug("SASL credentials for storm topology {} is {}",
                  topologyName, secretKey);
    }
}
