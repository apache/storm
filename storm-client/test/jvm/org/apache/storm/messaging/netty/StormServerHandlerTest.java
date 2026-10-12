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
import java.net.InetSocketAddress;
import org.apache.storm.messaging.FatalDeserializationException;
import org.apache.storm.shade.io.netty.channel.Channel;
import org.apache.storm.shade.io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StormServerHandlerTest {

    private Runnable terminator;
    private ChannelHandlerContext ctx;
    private StormServerHandler handler;

    @BeforeEach
    public void setUp() {
        terminator = mock(Runnable.class);
        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("localhost", 6700));
        ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        handler = new StormServerHandler(mock(IServer.class), terminator);
    }

    @Test
    public void testIoExceptionClosesConnection() {
        handler.exceptionCaught(ctx, new RuntimeException("Failed to decompress", new IOException("corrupt frame")));

        verify(ctx).close();
        verify(terminator, never()).run();
    }

    @Test
    public void testFatalDeserializationWrappingIoExceptionTerminates() {
        handler.exceptionCaught(ctx, new FatalDeserializationException("strict",
            new RuntimeException("Failed to decompress", new IOException("corrupt frame"))));

        verify(terminator).run();
    }

    @Test
    public void testOtherFailureTerminates() {
        handler.exceptionCaught(ctx, new IllegalStateException("unexpected"));

        verify(terminator).run();
        verify(ctx, never()).close();
    }
}
