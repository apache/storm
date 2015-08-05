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
package com.alibaba.jstorm.message.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.metric.Histogram;
import com.alibaba.jstorm.common.metric.Meter;
import com.alibaba.jstorm.common.metric.QueueGauge;
import com.alibaba.jstorm.metric.JStormHealthCheck;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;
import com.codahale.metrics.health.HealthCheck;

class NettyClient implements IConnection {
    private static final Logger LOG = LoggerFactory
            .getLogger(NettyClient.class);
    protected String name;

    protected final int max_retries;
    protected final int base_sleep_ms;
    protected final int max_sleep_ms;
    protected final long timeoutMs;
    protected final int MAX_SEND_PENDING;

    protected AtomicInteger retries;

    protected AtomicReference<Channel> channelRef;
    protected ClientBootstrap bootstrap;
    protected final InetSocketAddress remote_addr;
    protected final ChannelFactory factory;

    protected final int buffer_size;
    protected final AtomicBoolean being_closed;

    protected AtomicLong pendings;
    protected int messageBatchSize;
    protected AtomicReference<MessageBatch> messageBatchRef;

    protected ScheduledExecutorService scheduler;

    protected String address;
    // doesn't use timer, due to competition
    protected Histogram sendTimer;
    protected Histogram batchSizeHistogram;
    protected Meter     sendSpeed;

    protected ReconnectRunnable reconnector;
    protected ChannelFactory clientChannelFactory;

    protected Set<Channel> closingChannel;

    protected AtomicBoolean isConnecting = new AtomicBoolean(false);
    
    protected NettyConnection nettyConnection;
    
    protected Map stormConf;
    
    protected boolean connectMyself;

    protected Object channelClosing = new Object();

    @SuppressWarnings("rawtypes")
    NettyClient(Map storm_conf, ChannelFactory factory,
            ScheduledExecutorService scheduler, String host, int port,
            ReconnectRunnable reconnector) {
        this.stormConf = storm_conf;
        this.factory = factory;
        this.scheduler = scheduler;
        this.reconnector = reconnector;

        retries = new AtomicInteger(0);
        channelRef = new AtomicReference<Channel>(null);
        being_closed = new AtomicBoolean(false);
        pendings = new AtomicLong(0);
        
        nettyConnection = new NettyConnection();
        nettyConnection.setClientPort(NetWorkUtils.ip(), 
                ConfigExtension.getLocalWorkerPort(storm_conf));
        nettyConnection.setServerPort(host, port);

        // Configure
        buffer_size =
                Utils.getInt(storm_conf
                        .get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
        max_retries =
                Math.min(30, Utils.getInt(storm_conf
                        .get(Config.STORM_MESSAGING_NETTY_MAX_RETRIES)));
        base_sleep_ms =
                Utils.getInt(storm_conf
                        .get(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS));
        max_sleep_ms =
                Utils.getInt(storm_conf
                        .get(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS));

        timeoutMs = ConfigExtension.getNettyPendingBufferTimeout(storm_conf);
        MAX_SEND_PENDING =
                (int) ConfigExtension.getNettyMaxSendPending(storm_conf);

        this.messageBatchSize =
                Utils.getInt(
                        storm_conf.get(Config.STORM_NETTY_MESSAGE_BATCH_SIZE),
                        262144);
        messageBatchRef = new AtomicReference<MessageBatch>();

        // Start the connection attempt.
        remote_addr = new InetSocketAddress(host, port);
        name = remote_addr.toString();
        connectMyself = isConnectMyself(stormConf, host, port);

        address = JStormServerUtils.getName(host, port);
        
        if (connectMyself == false) {
            registerMetrics();
        }
        closingChannel = new HashSet<Channel>();
    }
    
    public void registerMetrics() {
        sendTimer =
                JStormMetrics.registerWorkerHistogram(
                        MetricDef.NETTY_CLI_SEND_TIME, nettyConnection.toString());
        batchSizeHistogram =
                JStormMetrics.registerWorkerHistogram(
                        MetricDef.NETTY_CLI_BATCH_SIZE, nettyConnection.toString());
        sendSpeed = JStormMetrics.registerWorkerMeter(MetricDef.NETTY_CLI_SEND_SPEED, 
                nettyConnection.toString());

        CacheGaugeHealthCheck cacheGauge =
                new CacheGaugeHealthCheck(messageBatchRef,
                        MetricDef.NETTY_CLI_CACHE_SIZE + ":" + nettyConnection.toString());
        JStormMetrics.registerWorkerGauge(cacheGauge,
                MetricDef.NETTY_CLI_CACHE_SIZE, nettyConnection.toString());
        JStormHealthCheck.registerWorkerHealthCheck(
                MetricDef.NETTY_CLI_CACHE_SIZE + ":" + nettyConnection.toString(), cacheGauge);

        JStormMetrics.registerWorkerGauge(
                new com.codahale.metrics.Gauge<Double>() {

                    @Override
                    public Double getValue() {
                        return ((Long) pendings.get()).doubleValue();
                    }
                }, MetricDef.NETTY_CLI_SEND_PENDING, nettyConnection.toString());
        
        JStormHealthCheck.registerWorkerHealthCheck(
                MetricDef.NETTY_CLI_CONNECTION + ":" + nettyConnection.toString(), 
                new HealthCheck() {
                    HealthCheck.Result healthy = HealthCheck.Result.healthy();
                    HealthCheck.Result unhealthy = HealthCheck.Result.unhealthy
                            ("NettyConnection " + nettyConnection.toString() + " is broken.");
                    @Override
                    protected Result check() throws Exception {
                        // TODO Auto-generated method stub
                        if (isChannelReady() == null) {
                            return unhealthy;
                        }else {
                            return healthy;
                        }
                    }
                    
                });
    }

    public void start() {
        bootstrap = new ClientBootstrap(clientChannelFactory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("reuserAddress", true);
        bootstrap.setOption("sendBufferSize", buffer_size);
        bootstrap.setOption("keepAlive", true);

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new StormClientPipelineFactory(this, stormConf));
        reconnect();
    }
    
    public boolean isConnectMyself(Map conf, String host, int port) {
        String localIp = NetWorkUtils.ip();
        String remoteIp = NetWorkUtils.host2Ip(host);
        int localPort = ConfigExtension.getLocalWorkerPort(conf);
        
        if (localPort == port && 
                localIp.equals(remoteIp)) {
            return true;
        }
        
        return false;
    }

    /**
     * The function can't be synchronized, otherwise it will be deadlock
     * 
     */
    public void doReconnect() {
        if (channelRef.get() != null) {

            // if (channelRef.get().isWritable()) {
            // LOG.info("already exist a writable channel, give up reconnect, {}",
            // channelRef.get());
            // return;
            // }
            return;
        }

        if (isClosed() == true) {
            return;
        }

        if (isConnecting.getAndSet(true)) {
            LOG.info("Connect twice {}", name());
            return;
        }

        long sleepMs = getSleepTimeMs();
        LOG.info("Reconnect ... [{}], {}, sleep {}ms", retries.get(), name,
                sleepMs);
        ChannelFuture future = bootstrap.connect(remote_addr);
        future.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future)
                    throws Exception {
                isConnecting.set(false);
                Channel channel = future.getChannel();
                if (future.isSuccess()) {
                    // do something else
                    LOG.info("Connection established, channel = :{}", channel);
                    setChannel(channel);
                    // handleResponse();
                } else {
                    LOG.info(
                            "Failed to reconnect ... [{}], {}, channel = {}, cause = {}",
                            retries.get(), name, channel, future.getCause());
                    reconnect();
                }
            }
        });
        JStormUtils.sleepMs(sleepMs);

        return;

    }

    public void reconnect() {
        reconnector.pushEvent(this);
    }

    /**
     * # of milliseconds to wait per exponential back-off policy
     */
    private int getSleepTimeMs() {

        int sleepMs = base_sleep_ms * retries.incrementAndGet();
        if (sleepMs > 1000) {
            sleepMs = 1000;
        }
        return sleepMs;
    }

    /**
     * Enqueue a task message to be sent to server
     */
    @Override
    public void send(List<TaskMessage> messages) {
        LOG.warn("Should be overload");
    }

    @Override
    public void send(TaskMessage message) {
        LOG.warn("Should be overload");
    }
    
    Channel isChannelReady() {
        Channel channel = channelRef.get();
        if (channel == null) {
            return null;
        }

        // improve performance skill check
        if (channel.isWritable() == false) {
            return null;
        }

        return channel;
    }

    protected synchronized void flushRequest(Channel channel,
            final MessageBatch requests) {
        if (requests == null || requests.isEmpty())
            return;

        Double batchSize = Double.valueOf(requests.getEncoded_length());
        batchSizeHistogram.update(batchSize);
        pendings.incrementAndGet();
        sendSpeed.update(batchSize);
        ChannelFuture future = channel.write(requests);
        future.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future)
                    throws Exception {

                pendings.decrementAndGet();
                if (!future.isSuccess()) {
                    Channel channel = future.getChannel();
                    if (isClosed() == false) {
                        LOG.info("Failed to send requests to " + name + ": "
                                + channel.toString() + ":", future.getCause());
                    }

                    if (null != channel) {

                        exceptionChannel(channel);
                    }
                } else {
                    // LOG.debug("{} request(s) sent", requests.size());
                }
            }
        });
    }
    
    public void unregisterMetrics() {
        JStormMetrics.unregisterWorkerMetric(MetricDef.NETTY_CLI_SEND_TIME,
                nettyConnection.toString());
        JStormMetrics.unregisterWorkerMetric(MetricDef.NETTY_CLI_BATCH_SIZE,
                nettyConnection.toString());
        JStormMetrics.unregisterWorkerMetric(MetricDef.NETTY_CLI_SEND_PENDING,
                nettyConnection.toString());
        JStormMetrics.unregisterWorkerMetric(MetricDef.NETTY_CLI_CACHE_SIZE,
                nettyConnection.toString());
        JStormMetrics.unregisterWorkerMetric(MetricDef.NETTY_CLI_SEND_SPEED, 
                nettyConnection.toString());

        JStormHealthCheck
                .unregisterWorkerHealthCheck(MetricDef.NETTY_CLI_CACHE_SIZE
                        + ":" + nettyConnection.toString());
        
        JStormHealthCheck.unregisterWorkerHealthCheck(
                MetricDef.NETTY_CLI_CONNECTION + ":" + nettyConnection.toString()); 
    }

    /**
     * gracefully close this client.
     * 
     * We will send all existing requests, and then invoke close_n_release()
     * method
     */
    public void close() {
        LOG.info("Close netty connection to {}", name());
        if (being_closed.compareAndSet(false, true) == false) {
            LOG.info("Netty client has been closed.");
            return;
        }

        if (connectMyself == false) {
            unregisterMetrics();
        }

        Channel channel = channelRef.get();
        if (channel == null) {
            LOG.info("Channel {} has been closed before", name());
            return;
        }

        if (channel.isWritable()) {
            MessageBatch toBeFlushed = messageBatchRef.getAndSet(null);
            flushRequest(channel, toBeFlushed);
        }

        // wait for pendings to exit
        final long timeoutMilliSeconds = 10 * 1000;
        final long start = System.currentTimeMillis();

        LOG.info("Waiting for pending batchs to be sent with " + name()
                + "..., timeout: {}ms, pendings: {}", timeoutMilliSeconds,
                pendings.get());

        while (pendings.get() != 0) {
            try {
                long delta = System.currentTimeMillis() - start;
                if (delta > timeoutMilliSeconds) {
                    LOG.error(
                            "Timeout when sending pending batchs with {}..., there are still {} pending batchs not sent",
                            name(), pendings.get());
                    break;
                }
                Thread.sleep(1000); // sleep 1s
            } catch (InterruptedException e) {
                break;
            }
        }

        close_n_release();

    }

    /**
     * close_n_release() is invoked after all messages have been sent.
     */
    void close_n_release() {
        if (channelRef.get() != null) {
            setChannel(null);
        }

    }

    /**
     * Avoid channel double close
     * 
     * @param channel
     */
    void closeChannel(final Channel channel) {
        synchronized (channelClosing) {
            if (closingChannel.contains(channel)) {
                LOG.info(channel.toString() + " is already closed");
                return;
            }

            closingChannel.add(channel);
        }

        LOG.debug(channel.toString() + " begin to closed");
        ChannelFuture closeFuture = channel.close();
        closeFuture.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future)
                    throws Exception {

                synchronized (channelClosing) {
                    closingChannel.remove(channel);
                }
                LOG.debug(channel.toString() + " finish closed");
            }
        });
    }

    void disconnectChannel(Channel channel) {
        if (isClosed()) {
            return;
        }

        if (channel == channelRef.get()) {
            setChannel(null);
            reconnect();
        } else {
            closeChannel(channel);
        }

    }

    void exceptionChannel(Channel channel) {
        if (channel == channelRef.get()) {
            setChannel(null);
        } else {
            closeChannel(channel);
        }
    }

    void setChannel(Channel newChannel) {
        final Channel oldChannel = channelRef.getAndSet(newChannel);

        if (newChannel != null) {
            retries.set(0);
        }

        final String oldLocalAddres =
                (oldChannel == null) ? "null" : oldChannel.getLocalAddress()
                        .toString();
        String newLocalAddress =
                (newChannel == null) ? "null" : newChannel.getLocalAddress()
                        .toString();
        LOG.info("Use new channel {} replace old channel {}", newLocalAddress,
                oldLocalAddres);

        // avoid one netty client use too much connection, close old one
        if (oldChannel != newChannel && oldChannel != null) {

            closeChannel(oldChannel);
            LOG.info("Successfully close old channel " + oldLocalAddres);
            // scheduler.schedule(new Runnable() {
            //
            // @Override
            // public void run() {
            //
            // }
            // }, 10, TimeUnit.SECONDS);

            // @@@ todo
            // pendings.set(0);
        }
    }

    @Override
    public boolean isClosed() {
        return being_closed.get();
    }

    public AtomicBoolean getBeing_closed() {
        return being_closed;
    }

    public int getBuffer_size() {
        return buffer_size;
    }

    public SocketAddress getRemoteAddr() {
        return remote_addr;
    }

    public String name() {
        return name;
    }

    public void handleResponse() {
        LOG.warn("Should be overload");
    }

    @Override
    public Object recv(Integer taskId, int flags) {
        throw new UnsupportedOperationException(
                "recvTask: Client connection should not receive any messages");
    }

    @Override
    public void registerQueue(Integer taskId, DisruptorQueue recvQueu) {
        throw new UnsupportedOperationException(
                "recvTask: Client connection should not receive any messages");
    }

    @Override
    public void enqueue(TaskMessage message) {
        throw new UnsupportedOperationException(
                "recvTask: Client connection should not receive any messages");
    }

    public static class CacheGaugeHealthCheck extends HealthCheck implements
            com.codahale.metrics.Gauge<Double> {

        AtomicReference<MessageBatch> messageBatchRef;
        String name;
        Result healthy;

        public CacheGaugeHealthCheck(
                AtomicReference<MessageBatch> messageBatchRef, String name) {
            this.messageBatchRef = messageBatchRef;
            this.name = name;
            this.healthy = HealthCheck.Result.healthy();
        }

        @Override
        public Double getValue() {
            // TODO Auto-generated method stub
            MessageBatch messageBatch = messageBatchRef.get();
            if (messageBatch == null) {
                return 0.0;
            } else {
                Double ret = (double) messageBatch.getEncoded_length();
                return ret;
            }

        }

        @Override
        protected Result check() throws Exception {
            // TODO Auto-generated method stub
            Double size = getValue();
            if (size > 8 * JStormUtils.SIZE_1_M) {
                return HealthCheck.Result.unhealthy(name
                        + QueueGauge.QUEUE_IS_FULL);
            } else {
                return healthy;
            }
        }

    }
}
