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
import java.util.List;
import java.util.Map;
import org.apache.storm.shade.io.netty.channel.Channel;
import org.apache.storm.shade.io.netty.channel.ChannelHandlerContext;
import org.apache.storm.shade.io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosSaslServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory
        .getLogger(KerberosSaslServerHandler.class);
    private final ISaslServer server;
    /**
     * Used for client or server's token to send or receive from each other.
     */
    private final Map<String, Object> topoConf;
    private final String jaasSection;
    private final List<String> authorizedUsers;

    public KerberosSaslServerHandler(ISaslServer server, Map<String, Object> topoConf, String jaasSection,
                                     List<String> authorizedUsers) throws IOException {
        this.server = server;
        this.topoConf = topoConf;
        this.jaasSection = jaasSection;
        this.authorizedUsers = authorizedUsers;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg == null) {
            return;
        }

        Channel channel = ctx.channel();

        if (msg instanceof SaslMessageToken) {
            // initialize server-side SASL functionality, if we haven't yet
            // (in which case we are looking at the first SASL message from the
            // client).
            try {
                LOG.debug("Got SaslMessageToken!");

                KerberosSaslNettyServer saslNettyServer = channel.attr(KerberosSaslNettyServerState.KERBOROS_SASL_NETTY_SERVER).get();
                if (saslNettyServer == null) {
                    LOG.debug("No saslNettyServer for {}  yet; creating now, with topology token: ", channel);
                    try {
                        saslNettyServer = new KerberosSaslNettyServer(topoConf, jaasSection, authorizedUsers);
                        channel.attr(KerberosSaslNettyServerState.KERBOROS_SASL_NETTY_SERVER).set(saslNettyServer);
                    } catch (RuntimeException ioe) {
                        LOG.error("Error occurred while creating saslNettyServer on server {} for client {}",
                                  channel.localAddress(), channel.remoteAddress());
                        throw ioe;
                    }
                } else {
                    LOG.debug("Found existing saslNettyServer on server: {} for client {}",
                              channel.localAddress(), channel.remoteAddress());
                }

                byte[] responseBytes = saslNettyServer.response(((SaslMessageToken) msg)
                                                                    .getSaslToken());

                SaslMessageToken saslTokenMessageRequest = new SaslMessageToken(responseBytes);

                if (saslTokenMessageRequest.getSaslToken() == null) {
                    channel.writeAndFlush(ControlMessage.SASL_COMPLETE_REQUEST, channel.voidPromise());
                } else {
                    // Send response to client.
                    channel.writeAndFlush(saslTokenMessageRequest, channel.voidPromise());
                }

                if (saslNettyServer.isComplete()) {
                    // If authentication of client is complete, we will also send a
                    // SASL-Complete message to the client.
                    LOG.info("SASL authentication is complete for client with username: {}",
                             saslNettyServer.getUserName());
                    channel.writeAndFlush(ControlMessage.SASL_COMPLETE_REQUEST, channel.voidPromise());
                    LOG.debug("Removing SaslServerHandler from pipeline since SASL authentication is complete.");
                    ctx.pipeline().remove(this);
                    server.authenticated(channel);
                }
            } catch (Exception ex) {
                LOG.error("Failed to handle SaslMessageToken: ", ex);
                throw ex;
            }
        } else {
            // Client should not be sending other-than-SASL messages before
            // SaslServerHandler has removed itself from the pipeline. Such
            // non-SASL requests will be denied by the Authorize channel handler
            // (the next handler upstream in the server pipeline) if SASL
            // authentication has not completed.
            LOG.warn("Sending upstream an unexpected non-SASL message : {}",
                     msg);
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }

}
