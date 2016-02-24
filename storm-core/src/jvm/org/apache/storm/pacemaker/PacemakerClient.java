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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.storm.Config;
import org.apache.storm.generated.HBMessage;
import org.apache.storm.messaging.netty.ISaslClient;
import org.apache.storm.messaging.netty.NettyRenameThreadFactory;
import org.apache.storm.pacemaker.codec.ThriftNettyClientCodec;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.utils.StormBoundedExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class PacemakerClient implements ISaslClient {

    private static final Logger LOG = LoggerFactory.getLogger(PacemakerClient.class);

    private String topo_name;
    private String secret;
    private boolean ready = false;
    private final Bootstrap bootstrap;
    private AtomicReference<Channel> channelRef;
    private AtomicBoolean closing;
    private InetSocketAddress remote_addr;
    private int maxPending = 100;
    private HBMessage messages[];
    private LinkedBlockingQueue<Integer> availableMessageSlots;
    private ThriftNettyClientCodec.AuthMethod authMethod;

    private StormBoundedExponentialBackoffRetry backoff = new StormBoundedExponentialBackoffRetry(100, 5000, 20);
    private int retryTimes = 0;

    //the constructor is invoked by pacemaker-state-factory-test
    public PacemakerClient() {
        bootstrap = new Bootstrap();
    }
    public PacemakerClient(Map config) {

        String host = (String)config.get(Config.PACEMAKER_HOST);
        int port = (int)config.get(Config.PACEMAKER_PORT);
        int maxWorkers = (int)config.get(Config.PACEMAKER_CLIENT_MAX_THREADS);

        topo_name = (String)config.get(Config.TOPOLOGY_NAME);
        if(topo_name == null) {
            topo_name = "pacemaker-client";
        }

        String auth = (String)config.get(Config.PACEMAKER_AUTH_METHOD);
        ThriftNettyClientCodec.AuthMethod authMethod;

        switch(auth) {

        case "DIGEST":
            Configuration login_conf = AuthUtils.GetConfiguration(config);
            authMethod = ThriftNettyClientCodec.AuthMethod.DIGEST;
            secret = AuthUtils.makeDigestPayload(login_conf, AuthUtils.LOGIN_CONTEXT_PACEMAKER_DIGEST);
            if(secret == null) {
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

        closing = new AtomicBoolean(false);
        channelRef = new AtomicReference<Channel>(null);
        setupMessaging();

        ThreadFactory workerFactory = new NettyRenameThreadFactory("client-worker");

        EventLoopGroup workerEventLoopGroup;
        if (maxWorkers > 0) {
            workerEventLoopGroup = new NioEventLoopGroup(maxWorkers, workerFactory);
        } else {
            // 0 means DEFAULT_EVENT_LOOP_THREADS
            workerEventLoopGroup = new NioEventLoopGroup(0, workerFactory);
        }

        bootstrap = new Bootstrap()
                .group(workerEventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_SNDBUF, 5242880)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)
                .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .handler(new ThriftNettyClientCodec(this, config, authMethod));

        remote_addr = new InetSocketAddress(host, port);
        bootstrap.connect(remote_addr);
    }

    private void setupMessaging() {
        messages = new HBMessage[maxPending];
        availableMessageSlots = new LinkedBlockingQueue<Integer>();
        for(int i = 0; i < maxPending; i++) {
            availableMessageSlots.add(i);
        }
    }

    public synchronized void channelConnected(Channel channel) {
        Channel oldChannel = channelRef.get();
        if (oldChannel != null) {
            LOG.debug("Closing oldChannel is connected: {}", oldChannel.toString());
            close_channel();
        }

        LOG.debug("Channel is connected: {}", channel.toString());
        channelRef.set(channel);

        //If we're not going to authenticate, we can begin sending.
        if(authMethod == ThriftNettyClientCodec.AuthMethod.NONE) {
            ready = true;
            this.notifyAll();
        }
        retryTimes = 0;
    }

    public synchronized void channelReady() {
        LOG.debug("Channel is ready.");
        ready = true;
        this.notifyAll();
    }

    public String name() {
        return topo_name;
    }

    public String secretKey() {
        return secret;
    }
    public HBMessage  checkCaptured() {return null;}
    public HBMessage send(HBMessage m) {
        waitUntilReady();
        LOG.debug("Sending message: {}", m.toString());
        try {

            int next = availableMessageSlots.take();
            synchronized (m) {
                m.set_message_id(next);
                messages[next] = m;
                LOG.debug("Put message in slot: {}", Integer.toString(next));
                do {
                    Channel channel = channelRef.get();
                    if(channel == null )
                    {
                        reconnect();
                        waitUntilReady();
                    }
                    channelRef.get().write(m);
                    m.wait(1000);
                } while (messages[next] == m);
            }

            HBMessage ret = messages[next];
            if(ret == null) {
                // This can happen if we lost the connection and subsequently reconnected or timed out.
                send(m);
            }
            messages[next] = null;
            LOG.debug("Got Response: {}", ret);
            return ret;
        }
        catch (InterruptedException e) {
            LOG.error("PacemakerClient send interrupted: ", e);
            throw new RuntimeException(e);
        }
    }

    private void waitUntilReady() {
        // Wait for 'ready' (channel connected and maybe authentication)
        if(!ready || channelRef.get() == null) {
            synchronized(this) {
                if(!ready) {
                    LOG.debug("Waiting for netty channel to be ready.");
                    try {
                        this.wait(1000);
                        if(!ready || channelRef.get() == null) {
                            throw new RuntimeException("Timed out waiting for channel ready.");
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    public void gotMessage(HBMessage m) {
        int message_id = m.get_message_id();
        if(message_id >=0 && message_id < maxPending) {

            LOG.debug("Pacemaker client got message: {}", m.toString());
            HBMessage request = messages[message_id];

            if(request == null) {
                LOG.debug("No message for slot: {}", Integer.toString(message_id));
            }
            else {
                synchronized(request) {
                    messages[message_id] = m;
                    request.notifyAll();
                    availableMessageSlots.add(message_id);
                }
            }
        }
        else {
            LOG.error("Got Message with bad id: {}", m.toString());
        }
    }

    public void reconnect() {
        final PacemakerClient client = this;
        Timer t = new Timer(true);
        t.schedule(new TimerTask() {
                public void run() {
                    client.doReconnect();
                }
            },
            backoff.getSleepTimeMs(retryTimes++, 0));
        ready = false;
        setupMessaging();
    }

    public synchronized void doReconnect() {
        close_channel();
        if(closing.get()) return;
        bootstrap.connect(remote_addr);
    }

    synchronized void close_channel() {
        if (channelRef.get() != null) {
            channelRef.get().close();
            LOG.debug("channel {} closed", remote_addr);
            channelRef.set(null);
        }
    }

    public void close() {
        close_channel();
    }
}
