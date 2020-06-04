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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.auth.login.Configuration;
import org.apache.storm.Config;
import org.apache.storm.generated.HBMessage;
import org.apache.storm.messaging.netty.ISaslClient;
import org.apache.storm.messaging.netty.NettyRenameThreadFactory;
import org.apache.storm.pacemaker.codec.ThriftNettyClientCodec;
import org.apache.storm.security.auth.ClientAuthUtils;
import org.apache.storm.shade.io.netty.bootstrap.Bootstrap;
import org.apache.storm.shade.io.netty.buffer.PooledByteBufAllocator;
import org.apache.storm.shade.io.netty.channel.Channel;
import org.apache.storm.shade.io.netty.channel.ChannelOption;
import org.apache.storm.shade.io.netty.channel.EventLoopGroup;
import org.apache.storm.shade.io.netty.channel.WriteBufferWaterMark;
import org.apache.storm.shade.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.storm.shade.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.storm.utils.StormBoundedExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PacemakerClient implements ISaslClient {

    private static final Logger LOG = LoggerFactory.getLogger(PacemakerClient.class);
    private static Timer timer = new Timer(true);
    private final Bootstrap bootstrap;
    private final EventLoopGroup workerEventLoopGroup;
    private String clientName;
    private String secret;
    private AtomicBoolean ready;
    private AtomicBoolean shutdown;
    private AtomicReference<Channel> channelRef;
    private InetSocketAddress remoteAddr;
    private int maxPending = 100;
    private HBMessage[] messages;
    private LinkedBlockingQueue<Integer> availableMessageSlots;
    private ThriftNettyClientCodec.AuthMethod authMethod;
    private static final int maxRetries = 10;
    private String host;

    private StormBoundedExponentialBackoffRetry backoff = new StormBoundedExponentialBackoffRetry(100, 5000, 20);
    private int retryTimes = 0;

    public PacemakerClient(Map<String, Object> config, String host) {
        this.host = host;
        clientName = (String) config.get(Config.TOPOLOGY_NAME);
        if (clientName == null) {
            clientName = "pacemaker-client";
        }

        String auth = (String) config.get(Config.PACEMAKER_AUTH_METHOD);

        switch (auth) {

            case "DIGEST":
                authMethod = ThriftNettyClientCodec.AuthMethod.DIGEST;
                secret = ClientAuthUtils.makeDigestPayload(config, ClientAuthUtils.LOGIN_CONTEXT_PACEMAKER_DIGEST);
                if (secret == null) {
                    LOG.error("Can't start pacemaker server without digest secret.");
                    throw new RuntimeException("Can't start pacemaker server without digest secret.");
                }
                break;

            case "KERBEROS":
                authMethod = ThriftNettyClientCodec.AuthMethod.KERBEROS;
                break;

            case "NONE":
                authMethod = ThriftNettyClientCodec.AuthMethod.NONE;
                break;

            default:
                authMethod = ThriftNettyClientCodec.AuthMethod.NONE;
                LOG.warn("Invalid auth scheme: '{}'. Falling back to 'NONE'", auth);
                break;
        }

        ready = new AtomicBoolean(false);
        shutdown = new AtomicBoolean(false);
        channelRef = new AtomicReference<>(null);
        setupMessaging();

        ThreadFactory workerFactory = new NettyRenameThreadFactory(this.host + "-pm");
        // 0 means DEFAULT_EVENT_LOOP_THREADS
        // https://github.com/netty/netty/blob/netty-4.1.24.Final/transport/src/main/java/io/netty/channel/MultithreadEventLoopGroup.java#L40
        int maxWorkers = (int) config.get(Config.PACEMAKER_CLIENT_MAX_THREADS);
        this.workerEventLoopGroup = new NioEventLoopGroup(maxWorkers > 0 ? maxWorkers : 0, workerFactory);
        int thriftMessageMaxSize = (Integer) config.get(Config.PACEMAKER_THRIFT_MESSAGE_SIZE_MAX);
        bootstrap = new Bootstrap()
            .group(workerEventLoopGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_SNDBUF, 5242880)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(8 * 1024, 32 * 1024))
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .handler(new ThriftNettyClientCodec(this, config, authMethod, host, thriftMessageMaxSize));

        int port = (int) config.get(Config.PACEMAKER_PORT);
        remoteAddr = new InetSocketAddress(host, port);
        bootstrap.connect(remoteAddr);
    }

    private void setupMessaging() {
        messages = new HBMessage[maxPending];
        availableMessageSlots = new LinkedBlockingQueue<Integer>();
        for (int i = 0; i < maxPending; i++) {
            availableMessageSlots.add(i);
        }
    }

    @Override
    public synchronized void channelReady(Channel channel) {
        Channel oldChannel = channelRef.get();
        if (oldChannel != null) {
            LOG.debug("Closing oldChannel is connected: {}", oldChannel.toString());
            close_channel();
        }

        channelRef.set(channel);
        retryTimes = 0;
        LOG.debug("Channel is ready: {}", channel.toString());
        ready.set(true);
        this.notifyAll();
    }

    @Override
    public String name() {
        return clientName;
    }

    @Override
    public String secretKey() {
        return secret;
    }

    public HBMessage send(HBMessage m) throws PacemakerConnectionException, InterruptedException {
        LOG.debug("Sending pacemaker message to {}: {}", host, m);

        int next = availableMessageSlots.take();
        synchronized (m) {
            m.set_message_id(next);
            messages[next] = m;
            LOG.debug("Put message in slot: {} for {}", Integer.toString(next), host);
            int retry = maxRetries;
            while (true) {
                try {
                    waitUntilReady();
                    Channel channel = channelRef.get();
                    if (channel != null) {
                        channel.writeAndFlush(m, channel.voidPromise());
                        m.wait(1000);
                    }
                    if (messages[next] != m && messages[next] != null) {
                        // messages[next] == null can happen if we lost the connection and subsequently reconnected or timed out.
                        HBMessage ret = messages[next];
                        messages[next] = null;
                        LOG.debug("Got Response: {}", ret);
                        return ret;
                    }
                } catch (PacemakerConnectionException e) {
                    if (retry <= 0) {
                        throw e;
                    }
                    LOG.error("Error attempting to write to a channel to host {} - {}", host, e.getMessage());
                }
                if (retry <= 0) {
                    throw new PacemakerConnectionException("couldn't get response after " + maxRetries + " attempts.");
                }
                retry--;
                LOG.warn("Not getting response or getting null response. Making {} more attempts for {}.", retry, host);
            }
        }
    }

    private void waitUntilReady() throws PacemakerConnectionException, InterruptedException {
        // Wait for 'ready' (channel connected and maybe authentication)
        if (!ready.get() || channelRef.get() == null) {
            synchronized (this) {
                if (!ready.get()) {
                    LOG.debug("Waiting for netty channel to be ready.");
                    this.wait(1000);
                    if (!ready.get() || channelRef.get() == null) {
                        throw new PacemakerConnectionException("Timed out waiting for channel ready.");
                    }
                }
            }
        }
    }

    public void gotMessage(HBMessage m) {
        int messageId = m.get_message_id();
        if (messageId >= 0 && messageId < maxPending) {

            LOG.debug("Pacemaker client got message: {}", m.toString());
            HBMessage request = messages[messageId];

            if (request == null) {
                LOG.debug("No message for slot: {}", Integer.toString(messageId));
            } else {
                synchronized (request) {
                    messages[messageId] = m;
                    request.notifyAll();
                    availableMessageSlots.add(messageId);
                }
            }
        } else {
            LOG.error("Got Message with bad id: {}", m.toString());
        }
    }

    public void reconnect() {
        final PacemakerClient client = this;
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                client.doReconnect();
            }
        }, backoff.getSleepTimeMs(retryTimes++, 0));
        ready.set(false);
        setupMessaging();
    }

    public synchronized void doReconnect() {
        LOG.info("reconnecting to {}", host);
        close_channel();
        if (!shutdown.get()) {
            bootstrap.connect(remoteAddr);
        }
    }

    public void shutdown() {
        shutdown.set(true);
        workerEventLoopGroup.shutdownGracefully().awaitUninterruptibly();
    }

    private synchronized void close_channel() {
        if (channelRef.get() != null) {
            channelRef.get().close();
            LOG.debug("channel {} closed", remoteAddr);
            channelRef.set(null);
        }
    }

    public void close() {
        close_channel();
    }
}
