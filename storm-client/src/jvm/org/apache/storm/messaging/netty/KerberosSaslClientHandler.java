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
import java.util.Map;
import org.apache.storm.shade.io.netty.channel.Channel;
import org.apache.storm.shade.io.netty.channel.ChannelHandlerContext;
import org.apache.storm.shade.io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosSaslClientHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory
        .getLogger(KerberosSaslClientHandler.class);
    private final long startTime;
    private final ISaslClient client;
    /**
     * Used for client or server's token to send or receive from each other.
     */
    private final Map<String, Object> topoConf;
    private final String jaasSection;
    private final String host;

    public KerberosSaslClientHandler(ISaslClient client, Map<String, Object> topoConf, String jaasSection, String host) throws
        IOException {
        this.client = client;
        this.topoConf = topoConf;
        this.jaasSection = jaasSection;
        this.host = host;
        startTime = System.currentTimeMillis();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // register the newly established channel
        Channel channel = ctx.channel();

        LOG.info("Connection established from {} to {}",
                 channel.localAddress(), channel.remoteAddress());

        try {
            KerberosSaslNettyClient saslNettyClient = channel.attr(KerberosSaslNettyClientState.KERBEROS_SASL_NETTY_CLIENT).get();

            if (saslNettyClient == null) {
                LOG.debug("Creating saslNettyClient now for channel: {}",
                          channel);
                saslNettyClient = new KerberosSaslNettyClient(topoConf, jaasSection, host);
                channel.attr(KerberosSaslNettyClientState.KERBEROS_SASL_NETTY_CLIENT).set(saslNettyClient);
            }
            LOG.debug("Going to initiate Kerberos negotiations.");
            byte[] initialChallenge = saslNettyClient.saslResponse(new SaslMessageToken(new byte[0]));
            LOG.debug("Sending initial challenge: {}", initialChallenge);
            channel.writeAndFlush(new SaslMessageToken(initialChallenge), channel.voidPromise());
        } catch (Exception e) {
            LOG.error("Failed to authenticate with server due to error: ",
                      e);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
        LOG.debug("send/recv time (ms): {}", (System.currentTimeMillis() - startTime));

        // examine the response message from server
        if (message instanceof ControlMessage) {
            handleControlMessage(ctx, (ControlMessage) message);
        } else if (message instanceof SaslMessageToken) {
            handleSaslMessageToken(ctx, (SaslMessageToken) message);
        } else {
            LOG.error("Unexpected message from server: {}", message);
        }
    }

    private KerberosSaslNettyClient getChannelSaslClient(Channel channel) throws Exception {
        // Generate SASL response to server using Channel-local SASL client.
        KerberosSaslNettyClient saslNettyClient = channel.attr(KerberosSaslNettyClientState.KERBEROS_SASL_NETTY_CLIENT).get();
        if (saslNettyClient == null) {
            throw new Exception("saslNettyClient was unexpectedly null for channel:" + channel);
        }
        return saslNettyClient;
    }

    private void handleControlMessage(ChannelHandlerContext ctx, ControlMessage controlMessage) throws Exception {
        Channel channel = ctx.channel();
        KerberosSaslNettyClient saslNettyClient = getChannelSaslClient(channel);
        if (controlMessage == ControlMessage.SASL_COMPLETE_REQUEST) {
            LOG.debug("Server has sent us the SaslComplete message. Allowing normal work to proceed.");

            if (!saslNettyClient.isComplete()) {
                String errorMessage =
                    "Server returned a Sasl-complete message, but as far as we can tell, we are not authenticated yet.";
                LOG.error(errorMessage);
                throw new Exception(errorMessage);
            }
            ctx.pipeline().remove(this);
            this.client.channelReady(channel);

            // We call fireChannelRead since the client is allowed to
            // perform this request. The client's request will now proceed
            // to the next pipeline component namely StormClientHandler.
            ctx.fireChannelRead(controlMessage);
        } else {
            LOG.warn("Unexpected control message: {}", controlMessage);
        }
    }

    private void handleSaslMessageToken(ChannelHandlerContext ctx, SaslMessageToken saslMessageToken) throws Exception {
        Channel channel = ctx.channel();
        KerberosSaslNettyClient saslNettyClient = getChannelSaslClient(channel);
        LOG.debug("Responding to server's token of length: {}", saslMessageToken.getSaslToken().length);

        // Generate SASL response (but we only actually send the response if
        // it's non-null.
        byte[] responseToServer = saslNettyClient.saslResponse(saslMessageToken);
        if (responseToServer == null) {
            // If we generate a null response, then authentication has completed
            // (if not, warn), and return without sending a response back to the
            // server.
            LOG.debug("Response to server is null: authentication should now be complete.");
            if (!saslNettyClient.isComplete()) {
                LOG.warn("Generated a null response, but authentication is not complete.");
                throw new Exception("Our reponse to the server is null, but as far as we can tell, we are not authenticated yet.");
            }
            this.client.channelReady(channel);
        } else {
            LOG.debug("Response to server token has length: {}",
                      responseToServer.length);
            // Construct a message containing the SASL response and send it to the server.
            SaslMessageToken saslResponse = new SaslMessageToken(responseToServer);
            channel.writeAndFlush(saslResponse, channel.voidPromise());
        }
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        LOG.debug("channelRegistered {}", ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        LOG.debug("channelUnregistered {}", ctx);

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.warn("{} exceptionCaught", ctx, cause);
        super.exceptionCaught(ctx, cause);
    }
}
