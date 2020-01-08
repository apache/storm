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

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import javax.security.auth.login.Configuration;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.generated.HBMessage;
import org.apache.storm.messaging.netty.ISaslServer;
import org.apache.storm.messaging.netty.NettyRenameThreadFactory;
import org.apache.storm.pacemaker.codec.ThriftNettyServerCodec;
import org.apache.storm.security.auth.ClientAuthUtils;
import org.apache.storm.shade.io.netty.bootstrap.ServerBootstrap;
import org.apache.storm.shade.io.netty.buffer.PooledByteBufAllocator;
import org.apache.storm.shade.io.netty.channel.Channel;
import org.apache.storm.shade.io.netty.channel.ChannelFuture;
import org.apache.storm.shade.io.netty.channel.ChannelOption;
import org.apache.storm.shade.io.netty.channel.EventLoopGroup;
import org.apache.storm.shade.io.netty.channel.WriteBufferWaterMark;
import org.apache.storm.shade.io.netty.channel.group.ChannelGroup;
import org.apache.storm.shade.io.netty.channel.group.DefaultChannelGroup;
import org.apache.storm.shade.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.storm.shade.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.storm.shade.io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PacemakerServer implements ISaslServer {

    private static final int FIVE_MB_IN_BYTES = 5 * 1024 * 1024;

    private static final Logger LOG = LoggerFactory.getLogger(PacemakerServer.class);

    private final IServerMessageHandler handler;
    private String secret;
    private final String topologyName;
    private volatile ChannelGroup allChannels = new DefaultChannelGroup("storm-server", GlobalEventExecutor.INSTANCE);
    private final ChannelGroup authenticatedChannels = new DefaultChannelGroup("authenticated-pacemaker-channels",
            GlobalEventExecutor.INSTANCE);
    private final ThriftNettyServerCodec.AuthMethod authMethod;
    private final EventLoopGroup bossEventLoopGroup;
    private final EventLoopGroup workerEventLoopGroup;

    PacemakerServer(IServerMessageHandler handler, Map<String, Object> config) {
        int port = (int) config.get(Config.PACEMAKER_PORT);
        this.handler = handler;
        this.topologyName = "pacemaker_server";

        String auth = (String) config.get(Config.PACEMAKER_AUTH_METHOD);
        switch (auth) {

            case "DIGEST":
                authMethod = ThriftNettyServerCodec.AuthMethod.DIGEST;
                this.secret = ClientAuthUtils.makeDigestPayload(config, ClientAuthUtils.LOGIN_CONTEXT_PACEMAKER_DIGEST);
                if (this.secret == null) {
                    LOG.error("Can't start pacemaker server without digest secret.");
                    throw new RuntimeException("Can't start pacemaker server without digest secret.");
                }
                break;

            case "KERBEROS":
                authMethod = ThriftNettyServerCodec.AuthMethod.KERBEROS;
                break;

            case "NONE":
                authMethod = ThriftNettyServerCodec.AuthMethod.NONE;
                break;

            default:
                LOG.error("Can't start pacemaker server without proper PACEMAKER_AUTH_METHOD.");
                throw new RuntimeException("Can't start pacemaker server without proper PACEMAKER_AUTH_METHOD.");
        }

        ThreadFactory bossFactory = new NettyRenameThreadFactory("server-boss");
        ThreadFactory workerFactory = new NettyRenameThreadFactory("server-worker");
        this.bossEventLoopGroup = new NioEventLoopGroup(1, bossFactory);
        // 0 means DEFAULT_EVENT_LOOP_THREADS
        int maxWorkers = (int) config.get(DaemonConfig.PACEMAKER_MAX_THREADS);
        // https://github.com/netty/netty/blob/netty-4.1.24.Final/transport/src/main/java/io/netty/channel/MultithreadEventLoopGroup.java#L40
        this.workerEventLoopGroup = new NioEventLoopGroup(maxWorkers > 0 ? maxWorkers : 0, workerFactory);

        LOG.info("Create Netty Server " + name() + ", buffer_size: " + FIVE_MB_IN_BYTES + ", maxWorkers: " + maxWorkers);

        int thriftMessageMaxSize = (Integer) config.get(Config.PACEMAKER_THRIFT_MESSAGE_SIZE_MAX);
        ServerBootstrap bootstrap = new ServerBootstrap()
            .group(bossEventLoopGroup, workerEventLoopGroup)
            .channel(NioServerSocketChannel.class)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_SNDBUF, FIVE_MB_IN_BYTES)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(8 * 1024, 32 * 1024))
            .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .childHandler(new ThriftNettyServerCodec(this, config, authMethod, thriftMessageMaxSize));

        try {
            ChannelFuture channelFuture = bootstrap.bind(new InetSocketAddress(port)).sync();
            allChannels.add(channelFuture.channel());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        LOG.info("Bound server to port: {}", Integer.toString(port));
    }

    /** Implementing IServer. **/
    @Override
    public void channelActive(Channel c) {
        allChannels.add(c);
    }

    public void cleanPipeline(Channel channel) {
        boolean authenticated = authenticatedChannels.contains(channel);
        if (!authenticated) {
            if (channel.pipeline().get(ThriftNettyServerCodec.SASL_HANDLER) != null) {
                channel.pipeline().remove(ThriftNettyServerCodec.SASL_HANDLER);
            } else if (channel.pipeline().get(ThriftNettyServerCodec.KERBEROS_HANDLER) != null) {
                channel.pipeline().remove(ThriftNettyServerCodec.KERBEROS_HANDLER);
            }
        }
    }

    @Override
    public void received(Object mesg, String remote, Channel channel) throws InterruptedException {
        cleanPipeline(channel);

        boolean authenticated = (authMethod == ThriftNettyServerCodec.AuthMethod.NONE) || authenticatedChannels.contains(channel);
        HBMessage m = (HBMessage) mesg;
        LOG.debug("received message. Passing to handler. {} : {} : {}",
                  handler.toString(), m.toString(), channel.toString());
        HBMessage response = handler.handleMessage(m, authenticated);
        if (response != null) {
            LOG.debug("Got Response from handler: {}", response);
            channel.writeAndFlush(response, channel.voidPromise());
        } else {
            LOG.info("Got null response from handler handling message: {}", m);
        }
    }

    @Override
    public String name() {
        return topologyName;
    }

    @Override
    public String secretKey() {
        return secret;
    }

    @Override
    public void authenticated(Channel c) {
        LOG.debug("Pacemaker server authenticated channel: {}", c.toString());
        authenticatedChannels.add(c);
    }
}
