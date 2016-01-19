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
package org.apache.storm.messaging.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class StormServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(StormServerHandler.class);
    IServer server;
    private AtomicInteger failure_count; 

    public StormServerHandler(IServer server) {
        this.server = server;
        failure_count = new AtomicInteger(0);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        server.channelConnected(ctx.channel());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      try {
        server.received(msg, ctx.channel().remoteAddress().toString(), ctx.channel());
      } catch (InterruptedException e1) {
        LOG.info("failed to enqueue a request message", e1);
        failure_count.incrementAndGet();
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("server errors in handling the request", cause);
        server.closeChannel(ctx.channel());
    }
}
