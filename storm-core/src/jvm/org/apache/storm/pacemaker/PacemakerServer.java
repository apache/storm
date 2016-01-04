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

import backtype.storm.Config;
import backtype.storm.generated.HBMessage;
import backtype.storm.messaging.netty.ISaslServer;
import backtype.storm.messaging.netty.NettyRenameThreadFactory;
import backtype.storm.security.auth.AuthUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.storm.pacemaker.codec.ThriftNettyServerCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadFactory;

class PacemakerServer implements ISaslServer {

    private static final int FIVE_MB_IN_BYTES = 5 * 1024 * 1024;

    private static final Logger LOG = LoggerFactory.getLogger(PacemakerServer.class);

    private IServerMessageHandler handler;
    private String secret;
    private String topologyName;
    private volatile ChannelGroup allChannels = new DefaultChannelGroup("storm-server", GlobalEventExecutor.INSTANCE);
    private ConcurrentSkipListSet<Channel> authenticated_channels = new ConcurrentSkipListSet<>();
    private ThriftNettyServerCodec.AuthMethod authMethod;

    public PacemakerServer(IServerMessageHandler handler, Map config){
        int maxWorkers = (int)config.get(Config.PACEMAKER_MAX_THREADS);
        int port = (int) config.get(Config.PACEMAKER_PORT);
        this.handler = handler;
        this.topologyName = "pacemaker_server";

        String auth = (String)config.get(Config.PACEMAKER_AUTH_METHOD);
        switch(auth) {

        case "DIGEST":
            Configuration login_conf = AuthUtils.GetConfiguration(config);
            authMethod = ThriftNettyServerCodec.AuthMethod.DIGEST;
            this.secret = AuthUtils.makeDigestPayload(login_conf, AuthUtils.LOGIN_CONTEXT_PACEMAKER_DIGEST);
            if(this.secret == null) {
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

        // 0 means DEFAULT_EVENT_LOOP_THREADS
        EventLoopGroup bossEventLoopGroup = new NioEventLoopGroup(0, bossFactory);
        EventLoopGroup workerEventLoopGroup;
        if (maxWorkers > 0) {
            workerEventLoopGroup = new NioEventLoopGroup(maxWorkers, workerFactory);
        } else {
            workerEventLoopGroup = new NioEventLoopGroup(0, workerFactory);
        }

        LOG.info("Create Netty Server " + name() + ", buffer_size: " + FIVE_MB_IN_BYTES + ", maxWorkers: " + maxWorkers);

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossEventLoopGroup, workerEventLoopGroup)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_RCVBUF, FIVE_MB_IN_BYTES)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ThriftNettyServerCodec(this, config, authMethod));

        // Bind and start to accept incoming connections.
        try {
            ChannelFuture f = bootstrap.bind(new InetSocketAddress(port)).sync();
            allChannels.add(f.channel());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /** Implementing IServer. **/
    public void channelConnected(Channel c) {
        allChannels.add(c);
    }

    public void cleanPipeline(Channel channel) {
        boolean authenticated = authenticated_channels.contains(channel);
        if(!authenticated) {
            if(channel.pipeline().get(ThriftNettyServerCodec.SASL_HANDLER) != null) {
                channel.pipeline().remove(ThriftNettyServerCodec.SASL_HANDLER);
            }
            else if(channel.pipeline().get(ThriftNettyServerCodec.KERBEROS_HANDLER) != null) {
                channel.pipeline().remove(ThriftNettyServerCodec.KERBEROS_HANDLER);
            }
        }
    }

    public void received(Object mesg, String remote, Channel channel) throws InterruptedException {
        cleanPipeline(channel);

        boolean authenticated = (authMethod == ThriftNettyServerCodec.AuthMethod.NONE) || authenticated_channels.contains(channel);
        HBMessage m = (HBMessage)mesg;
        LOG.debug("received message. Passing to handler. {} : {} : {}",
                  handler.toString(), m.toString(), channel.toString());
        HBMessage response = handler.handleMessage(m, authenticated);
        LOG.debug("Got Response from handler: {}", response.toString());
        channel.write(response);
    }

    public void closeChannel(Channel c) {
        c.close().awaitUninterruptibly();
        allChannels.remove(c);
        authenticated_channels.remove(c);
    }

    public String name() {
        return topologyName;
    }

    public String secretKey() {
        return secret;
    }

    public void authenticated(Channel c) {
        LOG.debug("Pacemaker server authenticated channel: {}", c.toString());
        authenticated_channels.add(c);
    }
}
