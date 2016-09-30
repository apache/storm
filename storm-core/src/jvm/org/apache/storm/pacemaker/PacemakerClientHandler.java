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
package org.apache.storm.pacemaker;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.storm.generated.HBMessage;
import org.apache.storm.messaging.netty.ControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PacemakerClientHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(PacemakerClientHandler.class);

    private PacemakerClient client;

    public PacemakerClientHandler(PacemakerClient client) {
        this.client = client;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // register the newly established channel
        Channel channel = ctx.channel();
        client.channelConnected(channel);

        LOG.info("Connection established from {} to {}", channel.localAddress(), channel.remoteAddress());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object evm) throws Exception {
        LOG.debug("Got Message: {}", evm.toString());

        if(evm instanceof ControlMessage) {
            LOG.debug("Got control message: {}", evm.toString());
        }
        else if(evm instanceof HBMessage) {
            client.gotMessage((HBMessage)evm);
        }
        else {
            LOG.warn("Got unexpected message: {} from server.", evm);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error("Connection to pacemaker failed", cause);
        client.reconnect();
    }
}
