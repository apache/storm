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

import java.net.ConnectException;
import org.apache.storm.generated.HBMessage;
import org.apache.storm.messaging.netty.ControlMessage;
import org.apache.storm.shade.io.netty.channel.Channel;
import org.apache.storm.shade.io.netty.channel.ChannelHandlerContext;
import org.apache.storm.shade.io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PacemakerClientHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(PacemakerClientHandler.class);

    private final PacemakerClient client;

    public PacemakerClientHandler(PacemakerClient client) {
        this.client = client;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // register the newly established channel
        Channel channel = ctx.channel();
        LOG.info("Connection established from {} to {}",
                 channel.localAddress(), channel.remoteAddress());
        client.channelReady(channel);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        LOG.debug("Got Message: {}", message.toString());

        if (message instanceof ControlMessage) {
            LOG.debug("Got control message: {}", message.toString());
            return;
        } else if (message instanceof HBMessage) {
            client.gotMessage((HBMessage) message);
        } else {
            LOG.warn("Got unexpected message: {} from server.", message);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof ConnectException) {
            LOG.warn("Connection to pacemaker failed. Trying to reconnect {}", cause.getMessage());
        } else {
            LOG.error("Exception occurred in Pacemaker: " + cause);
        }
        client.reconnect();
    }
}
