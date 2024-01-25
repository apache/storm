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

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.storm.Config;
import org.apache.storm.grouping.Load;
import org.apache.storm.messaging.ConnectionWithStatus;
import org.apache.storm.messaging.IConnectionCallback;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IStatefulObject;
import org.apache.storm.serialization.KryoValuesDeserializer;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.apache.storm.shade.io.netty.bootstrap.ServerBootstrap;
import org.apache.storm.shade.io.netty.buffer.PooledByteBufAllocator;
import org.apache.storm.shade.io.netty.channel.Channel;
import org.apache.storm.shade.io.netty.channel.ChannelFuture;
import org.apache.storm.shade.io.netty.channel.ChannelOption;
import org.apache.storm.shade.io.netty.channel.EventLoopGroup;
import org.apache.storm.shade.io.netty.channel.group.ChannelGroup;
import org.apache.storm.shade.io.netty.channel.group.DefaultChannelGroup;
import org.apache.storm.shade.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.storm.shade.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.storm.shade.io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Server extends ConnectionWithStatus implements IStatefulObject, ISaslServer {

    public static final int LOAD_METRICS_TASK_ID = -1;
    
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);
    private final EventLoopGroup bossEventLoopGroup;
    private final EventLoopGroup workerEventLoopGroup;
    private final ServerBootstrap bootstrap;
    private final ConcurrentHashMap<String, AtomicInteger> messagesEnqueued = new ConcurrentHashMap<>();
    private final AtomicInteger messagesDequeued = new AtomicInteger(0);
    private final int boundPort;
    private final Map<String, Object> topoConf;
    private final int port;
    private final ChannelGroup allChannels = new DefaultChannelGroup("storm-server", GlobalEventExecutor.INSTANCE);
    private final KryoValuesSerializer ser;
    private final IConnectionCallback cb;
    private final Supplier<Object> newConnectionResponse;
    private volatile boolean closing = false;
    private final boolean isNettyAuthRequired;

    /**
     * Starts Netty at the given port.
     * @param topoConf The topology config
     * @param port The port to start Netty at
     * @param cb The callback to deliver incoming messages to
     * @param newConnectionResponse The response to send to clients when they connect. Can be null. If authentication
     *                              is required, the message will be sent after authentication is complete.
     */
    Server(Map<String, Object> topoConf, int port, IConnectionCallback cb, Supplier<Object> newConnectionResponse) {
        this.topoConf = topoConf;
        this.isNettyAuthRequired = (Boolean) topoConf.get(Config.STORM_MESSAGING_NETTY_AUTHENTICATION);
        this.port = port;
        ser = new KryoValuesSerializer(topoConf);
        this.cb = cb;
        this.newConnectionResponse = newConnectionResponse;

        // Configure the server.
        int bufferSize = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
        int maxWorkers = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS));

        ThreadFactory bossFactory = new NettyRenameThreadFactory(netty_name() + "-boss");
        ThreadFactory workerFactory = new NettyRenameThreadFactory(netty_name() + "-worker");

        bossEventLoopGroup = new NioEventLoopGroup(1, bossFactory);
        // 0 means DEFAULT_EVENT_LOOP_THREADS
        // https://github.com/netty/netty/blob/netty-4.1.24.Final/transport/src/main/java/io/netty/channel/MultithreadEventLoopGroup.java#L40
        this.workerEventLoopGroup = new NioEventLoopGroup(maxWorkers > 0 ? maxWorkers : 0, workerFactory);

        LOG.info("Create Netty Server " + netty_name() + ", buffer_size: " + bufferSize + ", maxWorkers: " + maxWorkers);

        int backlog = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_SOCKET_BACKLOG), 500);
        bootstrap = new ServerBootstrap()
            .group(bossEventLoopGroup, workerEventLoopGroup)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_REUSEADDR, true)
            .option(ChannelOption.SO_BACKLOG, backlog)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_RCVBUF, bufferSize)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .childHandler(new StormServerPipelineFactory(topoConf, this));

        // Bind and start to accept incoming connections.
        try {
            ChannelFuture bindFuture = bootstrap.bind(new InetSocketAddress(port)).sync();
            Channel channel = bindFuture.channel();
            boundPort = ((InetSocketAddress) channel.localAddress()).getPort();
            allChannels.add(channel);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void addReceiveCount(String from, int amount) {
        //This is possibly lossy in the case where a value is deleted
        // because it has received no messages over the metrics collection
        // period and new messages are starting to come in.  This is
        // because I don't want the overhead of a synchronize just to have
        // the metric be absolutely perfect.
        AtomicInteger i = messagesEnqueued.get(from);
        if (i == null) {
            i = new AtomicInteger(amount);
            AtomicInteger prev = messagesEnqueued.putIfAbsent(from, i);
            if (prev != null) {
                prev.addAndGet(amount);
            }
        } else {
            i.addAndGet(amount);
        }
    }

    /**
     * enqueue a received message.
     */
    protected void enqueue(List<TaskMessage> msgs, String from) throws InterruptedException {
        if (null == msgs || msgs.isEmpty() || closing) {
            return;
        }
        addReceiveCount(from, msgs.size());
        cb.recv(msgs);
    }

    @Override
    public int getPort() {
        return boundPort;
    }

    /**
     * close all channels, and release resources.
     */
    @Override
    public void close() {
        allChannels.close().awaitUninterruptibly();
        workerEventLoopGroup.shutdownGracefully().awaitUninterruptibly();
        bossEventLoopGroup.shutdownGracefully().awaitUninterruptibly();
    }

    @Override
    public void sendLoadMetrics(Map<Integer, Double> taskToLoad) {
        MessageBatch mb = new MessageBatch(1);
        synchronized (ser) {
            mb.add(new TaskMessage(LOAD_METRICS_TASK_ID, ser.serialize(Collections.singletonList((Object) taskToLoad))));
        }
        allChannels.writeAndFlush(mb);
    }

    // this method expected to be thread safe
    @Override
    public void sendBackPressureStatus(BackPressureStatus bpStatus) {
        LOG.info("Sending BackPressure status update to connected workers. BPStatus = {}", bpStatus);
        allChannels.writeAndFlush(bpStatus);
    }

    @Override
    public Map<Integer, Load> getLoad(Collection<Integer> tasks) {
        throw new RuntimeException("Server connection cannot get load");
    }

    @Override
    public void send(Iterator<TaskMessage> msgs) {
        throw new UnsupportedOperationException("Server connection should not send any messages");
    }

    public final String netty_name() {
        return "Netty-server-localhost-" + port;
    }

    @Override
    public Status status() {
        if (closing) {
            return Status.Closed;
        } else if (!connectionEstablished(allChannels)) {
            return Status.Connecting;
        } else {
            return Status.Ready;
        }
    }

    private boolean connectionEstablished(Channel channel) {
        return channel != null && channel.isActive();
    }

    private boolean connectionEstablished(ChannelGroup allChannels) {
        boolean allEstablished = true;
        for (Channel channel : allChannels) {
            if (!(connectionEstablished(channel))) {
                allEstablished = false;
                break;
            }
        }
        return allEstablished;
    }

    @Override
    public Object getState() {
        LOG.debug("Getting metrics for server on port {}", port);
        HashMap<String, Object> ret = new HashMap<>();
        ret.put("dequeuedMessages", messagesDequeued.getAndSet(0));
        HashMap<String, Integer> enqueued = new HashMap<>();
        Iterator<Map.Entry<String, AtomicInteger>> it = messagesEnqueued.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, AtomicInteger> ent = it.next();
            //Yes we can delete something that is not 0 because of races, but that is OK for metrics
            AtomicInteger i = ent.getValue();
            if (i.get() == 0) {
                it.remove();
            } else {
                enqueued.put(ent.getKey(), i.getAndSet(0));
            }
        }
        ret.put("enqueued", enqueued);

        // Report messageSizes metric, if enabled (non-null).
        if (cb instanceof IMetric) {
            Object metrics = ((IMetric) cb).getValueAndReset();
            if (metrics instanceof Map) {
                ret.put("messageBytes", metrics);
            }
        }

        return ret;
    }

    /**
     * Implementing IServer.
     **/
    @Override
    public void channelActive(Channel c) {
        if (!isNettyAuthRequired) {
            //if authentication is not required, treat it as authenticated.
            authenticated(c);
        }
        allChannels.add(c);
    }

    @Override
    public void received(Object message, String remote, Channel channel) throws InterruptedException {
        List<TaskMessage> msgs;

        try {
            msgs = (List<TaskMessage>) message;
        } catch (ClassCastException e) {
            LOG.error("Worker netty server received message other than the expected class List<TaskMessage> from remote: {}. Ignored.",
                remote, e);
            return;
        }

        enqueue(msgs, remote);
    }

    @Override
    public String name() {
        return (String) topoConf.get(Config.TOPOLOGY_NAME);
    }

    @Override
    public String secretKey() {
        return SaslUtils.getSecretKey(topoConf);
    }

    @Override
    public void authenticated(Channel c) {
        if (isNettyAuthRequired) {
            LOG.debug("The channel {} is active and authenticated", c);
        } else {
            LOG.debug("The channel {} is active", c);
        }
        if (newConnectionResponse != null) {
            c.writeAndFlush(newConnectionResponse.get(), c.voidPromise());
        }
    }

    @Override
    public String toString() {
        return String.format("Netty server listening on port %s", port);
    }
}
