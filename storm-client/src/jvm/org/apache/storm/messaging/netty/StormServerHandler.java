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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.storm.shade.io.netty.channel.Channel;
import org.apache.storm.shade.io.netty.channel.ChannelHandlerContext;
import org.apache.storm.shade.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormServerHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(StormServerHandler.class);
    private static final Set<Class<?>> ALLOWED_EXCEPTIONS = new HashSet<>(Arrays.asList(new Class<?>[]{ IOException.class }));
    private final IServer server;

    public StormServerHandler(IServer server) {
        this.server = server;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        server.channelActive(ctx.channel());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg == null) {
            return;
        }

        Channel channel = ctx.channel();
        try {
            server.received(msg, channel.remoteAddress().toString(), channel);
        } catch (InterruptedException e) {
            LOG.info("failed to enqueue a request message", e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        try {
            LOG.error("server errors in handling the request from {}", ctx.channel().remoteAddress().toString(), cause);
        } catch (Throwable err) {
            // Doing nothing (probably due to an oom issue) and hoping Utils.handleUncaughtException will handle it
        }
        try {
            Utils.handleUncaughtException(cause, ALLOWED_EXCEPTIONS, false);
            ctx.close();
        } catch (Error error) {
            LOG.info("Received error in netty thread.. terminating server...");
            Runtime.getRuntime().exit(1);
        }

    }
}
