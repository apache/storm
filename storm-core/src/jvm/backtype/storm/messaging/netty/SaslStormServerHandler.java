/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.messaging.netty;

import java.io.IOException;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

public class SaslStormServerHandler extends ChannelInboundHandlerAdapter {

    Server server;
    /** Used for client or server's token to send or receive from each other. */
    private byte[] token;
    private String topologyName;

    private static final Logger LOG = LoggerFactory
            .getLogger(SaslStormServerHandler.class);

    public SaslStormServerHandler(Server server) throws IOException {
        this.server = server;
        getSASLCredentials();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg == null)
            return;

        Channel channel = ctx.channel();

        if (msg instanceof ControlMessage && ((ControlMessage) msg) == ControlMessage.SASL_TOKEN_MESSAGE_REQUEST) {
            // initialize server-side SASL functionality, if we haven't yet
            // (in which case we are looking at the first SASL message from the
            // client).
            SaslNettyServer saslNettyServer = ctx.attr(SaslNettyServerState.SAS_NETTY_SERVER).get();
            if (saslNettyServer == null) {
                LOG.debug("No saslNettyServer for " + channel
                        + " yet; creating now, with topology token: ");
                try {
                    saslNettyServer = new SaslNettyServer(topologyName, token);
                } catch (IOException ioe) {
                    LOG.error("Error occurred while creating saslNettyServer on server "
                            + channel.localAddress()
                            + " for client "
                            + channel.remoteAddress());
                    saslNettyServer = null;
                }

                ctx.attr(SaslNettyServerState.SAS_NETTY_SERVER).set(saslNettyServer);
            } else {
                LOG.debug("Found existing saslNettyServer on server:"
                        + channel.localAddress() + " for client "
                        + channel.remoteAddress());
            }

            LOG.debug("processToken:  With nettyServer: " + saslNettyServer
                    + " and token length: " + token.length);

            SaslMessageToken saslTokenMessageRequest = null;
            saslTokenMessageRequest = new SaslMessageToken(
                    saslNettyServer.response(new byte[0]));
            // Send response to client.
            channel.writeAndFlush(saslTokenMessageRequest);
            // do not send upstream to other handlers: no further action needs
            // to be done for SASL_TOKEN_MESSAGE_REQUEST requests.
            return;
        }

        if (msg instanceof SaslMessageToken) {
            // initialize server-side SASL functionality, if we haven't yet
            // (in which case we are looking at the first SASL message from the
            // client).
            SaslNettyServer saslNettyServer = ctx.attr(SaslNettyServerState.SAS_NETTY_SERVER).get();
            if (saslNettyServer == null) {
                if (saslNettyServer == null) {
                    throw new Exception("saslNettyServer was unexpectedly "
                            + "null for channel: " + channel);
                }
            }
            SaslMessageToken saslTokenMessageRequest = new SaslMessageToken(
                    saslNettyServer.response(((SaslMessageToken) msg)
                            .getSaslToken()));

            // Send response to client.
            channel.writeAndFlush(saslTokenMessageRequest);

            if (saslNettyServer.isComplete()) {
                // If authentication of client is complete, we will also send a
                // SASL-Complete message to the client.
                LOG.debug("SASL authentication is complete for client with "
                        + "username: " + saslNettyServer.getUserName());
                channel.writeAndFlush(ControlMessage.SASL_COMPLETE_REQUEST);
                LOG.debug("Removing SaslServerHandler from pipeline since SASL "
                        + "authentication is complete.");
                ctx.pipeline().remove(this);
            }
            return;
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
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        server.closeChannel(ctx.channel());
    }

    private void getSASLCredentials() throws IOException {
        topologyName = (String) this.server.storm_conf
                .get(Config.TOPOLOGY_NAME);
        String secretKey = SaslUtils.getSecretKey(this.server.storm_conf);
        if (secretKey != null) {
            token = secretKey.getBytes();
        }
        LOG.debug("SASL credentials for storm topology " + topologyName
                + " is " + secretKey);
    }
}
