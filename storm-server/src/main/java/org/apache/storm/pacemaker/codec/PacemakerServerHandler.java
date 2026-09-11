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

import org.apache.storm.messaging.netty.IServer;
import org.apache.storm.messaging.netty.StormServerHandler;
import org.apache.storm.shade.io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pacemaker server handler. A failure while handling a request only affects the connection it arrived on: the
 * connection is closed and the Pacemaker server keeps serving its other clients. Errors are still handled by
 * {@link StormServerHandler}.
 */
public class PacemakerServerHandler extends StormServerHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PacemakerServerHandler.class);

    public PacemakerServerHandler(IServer server) {
        super(server);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (!(cause instanceof Exception)) {
            super.exceptionCaught(ctx, cause);
            return;
        }
        try {
            LOG.warn("Closing connection {} after failing to handle its request", ctx.channel(), cause);
        } finally {
            ctx.close();
        }
    }
}
