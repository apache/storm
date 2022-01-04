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

import static org.apache.storm.shade.com.google.common.base.Preconditions.checkState;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.grouping.Load;
import org.apache.storm.messaging.ConnectionWithStatus;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.policy.IWaitStrategy;
import org.apache.storm.policy.IWaitStrategy.WaitSituation;
import org.apache.storm.policy.WaitStrategyProgressive;
import org.apache.storm.shade.io.netty.bootstrap.Bootstrap;
import org.apache.storm.shade.io.netty.buffer.PooledByteBufAllocator;
import org.apache.storm.shade.io.netty.channel.Channel;
import org.apache.storm.shade.io.netty.channel.ChannelFuture;
import org.apache.storm.shade.io.netty.channel.ChannelFutureListener;
import org.apache.storm.shade.io.netty.channel.ChannelOption;
import org.apache.storm.shade.io.netty.channel.EventLoopGroup;
import org.apache.storm.shade.io.netty.channel.WriteBufferWaterMark;
import org.apache.storm.shade.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.storm.shade.io.netty.util.HashedWheelTimer;
import org.apache.storm.shade.io.netty.util.Timeout;
import org.apache.storm.shade.io.netty.util.TimerTask;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ReflectionUtils;
import org.apache.storm.utils.StormBoundedExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Netty client for sending task messages to a remote destination (Netty server).
 *
 * <p>Implementation details:
 *
 * <p>Sending messages, i.e. writing to the channel, is performed asynchronously. Messages are sent in batches to optimize for network
 * throughput at the expense of network latency.  The message batch size is configurable. Connecting and reconnecting are performed
 * asynchronously. Note: The current implementation drops any messages that are being enqueued for sending if the connection to the remote
 * destination is currently unavailable.
 */
public class Client extends ConnectionWithStatus implements ISaslClient {
    private static final long PENDING_MESSAGES_FLUSH_TIMEOUT_MS = 600000L;
    private static final long PENDING_MESSAGES_FLUSH_INTERVAL_MS = 1000L;
    /**
     * Periodically checks for connected channel in order to avoid loss of messages.
     */
    private static final long CHANNEL_ALIVE_INTERVAL_MS = 30000L;

    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    private static final String PREFIX = "Netty-Client-";
    private static final long NO_DELAY_MS = 0L;
    private static final Timer TIMER = new Timer("Netty-ChannelAlive-Timer", true);
    protected final String dstAddressPrefixedName;
    private final Map<String, Object> topoConf;
    private final StormBoundedExponentialBackoffRetry retryPolicy;
    private final EventLoopGroup eventLoopGroup;
    private final Bootstrap bootstrap;
    private final InetSocketAddress dstAddress;

    /**
     * The channel used for all write operations from this client to the remote destination.
     */
    private final AtomicReference<Channel> channelRef = new AtomicReference<>();
    /**
     * Total number of connection attempts.
     */
    private final AtomicInteger totalConnectionAttempts = new AtomicInteger(0);
    /**
     * Number of connection attempts since the last disconnect.
     */
    private final AtomicInteger connectionAttempts = new AtomicInteger(0);
    /**
     * Number of messages successfully sent to the remote destination.
     */
    private final AtomicInteger messagesSent = new AtomicInteger(0);
    /**
     * Number of messages that could not be sent to the remote destination.
     */
    private final AtomicInteger messagesLost = new AtomicInteger(0);
    /**
     * Number of messages buffered in memory.
     */
    private final AtomicLong pendingMessages = new AtomicLong(0);
    /**
     * Whether the SASL channel is ready.
     */
    private final AtomicBoolean saslChannelReady = new AtomicBoolean(false);
    private final HashedWheelTimer scheduler;
    private final MessageBuffer batcher;
    // wait strategy when the netty channel is not writable
    private final IWaitStrategy waitStrategy;
    private volatile Map<Integer, Double> serverLoad = null;
    /**
     * This flag is set to true if and only if a client instance is being closed.
     */
    private volatile boolean closing = false;
    StormMetricRegistry metricRegistry;
    private Set<Metric> metrics = new HashSet<>();

    Client(Map<String, Object> topoConf, AtomicBoolean[] remoteBpStatus,
        EventLoopGroup eventLoopGroup, HashedWheelTimer scheduler, String host,
           int port, StormMetricRegistry metricRegistry) {
        this.topoConf = topoConf;
        closing = false;
        this.scheduler = scheduler;
        int bufferSize = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
        int lowWatermark = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_BUFFER_LOW_WATERMARK));
        int highWatermark = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_BUFFER_HIGH_WATERMARK));
        // if SASL authentication is disabled, saslChannelReady is initialized as true; otherwise false
        saslChannelReady.set(!ObjectReader.getBoolean(topoConf.get(Config.STORM_MESSAGING_NETTY_AUTHENTICATION), false));
        LOG.info("Creating Netty Client, connecting to {}:{}, bufferSize: {}, lowWatermark: {}, highWatermark: {}",
                 host, port, bufferSize, lowWatermark, highWatermark);

        int minWaitMs = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS));
        int maxWaitMs = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS));
        retryPolicy = new StormBoundedExponentialBackoffRetry(minWaitMs, maxWaitMs, -1);

        // Initiate connection to remote destination
        this.eventLoopGroup = eventLoopGroup;
        // Initiate connection to remote destination
        bootstrap = new Bootstrap()
            .group(this.eventLoopGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_SNDBUF, bufferSize)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(lowWatermark, highWatermark))
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .handler(new StormClientPipelineFactory(this, remoteBpStatus, topoConf));
        dstAddress = new InetSocketAddress(host, port);
        dstAddressPrefixedName = prefixedName(dstAddress);
        launchChannelAliveThread();
        scheduleConnect(NO_DELAY_MS);
        int messageBatchSize = ObjectReader.getInt(topoConf.get(Config.STORM_NETTY_MESSAGE_BATCH_SIZE), 262144);
        batcher = new MessageBuffer(messageBatchSize);
        String clazz = (String) topoConf.get(Config.TOPOLOGY_BACKPRESSURE_WAIT_STRATEGY);
        if (clazz == null) {
            waitStrategy = new WaitStrategyProgressive();
        } else {
            waitStrategy = ReflectionUtils.newInstance(clazz);
        }
        waitStrategy.prepare(topoConf, WaitSituation.BACK_PRESSURE_WAIT);
        this.metricRegistry = metricRegistry;

        // it's possible to be passed a null metric registry if users are using their own IContext implementation.
        boolean reportMetrics =  this.metricRegistry != null
                && ObjectReader.getBoolean(topoConf.get(Config.TOPOLOGY_ENABLE_SEND_ICONNECTION_METRICS), true);

        if (reportMetrics) {
            Gauge<Integer> reconnects = new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    return totalConnectionAttempts.get();
                }
            };
            metricRegistry.gauge("__send-iconnection-reconnects-" + host + ":" + port, reconnects,
                    Constants.SYSTEM_COMPONENT_ID, (int) Constants.SYSTEM_TASK_ID);
            metrics.add(reconnects);

            Gauge<Integer> sent = new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    return messagesSent.get();
                }
            };
            metricRegistry.gauge("__send-iconnection-sent-" + host + ":" + port, sent,
                    Constants.SYSTEM_COMPONENT_ID, (int) Constants.SYSTEM_TASK_ID);
            metrics.add(sent);

            Gauge<Long> pending = new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return pendingMessages.get();
                }
            };
            metricRegistry.gauge("__send-iconnection-pending-" + host + ":" + port, pending,
                    Constants.SYSTEM_COMPONENT_ID, (int) Constants.SYSTEM_TASK_ID);
            metrics.add(pending);

            Gauge<Integer> lostOnSend = new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    return messagesLost.get();
                }
            };
            metricRegistry.gauge("__send-iconnection-lostOnSend-" + host + ":" + port, lostOnSend,
                    Constants.SYSTEM_COMPONENT_ID, (int) Constants.SYSTEM_TASK_ID);
            metrics.add(lostOnSend);
        }
    }

    /**
     * This thread helps us to check for channel connection periodically. This is performed just to know whether the destination address is
     * alive or attempts to refresh connections if not alive. This solution is better than what we have now in case of a bad channel.
     */
    private void launchChannelAliveThread() {
        // netty TimerTask is already defined and hence a fully
        // qualified name
        TIMER.schedule(new java.util.TimerTask() {
            @Override
            public void run() {
                try {
                    LOG.debug("running timer task, address {}", dstAddress);
                    if (closing) {
                        this.cancel();
                        return;
                    }
                    getConnectedChannel();
                } catch (Exception exp) {
                    LOG.error("channel connection error {}", exp);
                }
            }
        }, 0, CHANNEL_ALIVE_INTERVAL_MS);
    }

    private String prefixedName(InetSocketAddress dstAddress) {
        if (null != dstAddress) {
            return PREFIX + dstAddress.toString();
        }
        return "";
    }

    /**
     * Enqueue a task message to be sent to server.
     */
    private void scheduleConnect(long delayMs) {
        scheduler.newTimeout(new Connect(dstAddress), delayMs, TimeUnit.MILLISECONDS);
    }

    private boolean reconnectingAllowed() {
        return !closing;
    }

    private boolean connectionEstablished(Channel channel) {
        // The connection is ready once the channel is active.
        // See:
        // - http://netty.io/wiki/new-and-noteworthy-in-4.0.html#wiki-h4-19
        return channel != null && channel.isActive();
    }

    /**
     * Note:  Storm will check via this method whether a worker can be activated safely during the initial startup of a topology.  The
     * worker will only be activated once all of the its connections are ready.
     */
    @Override
    public Status status() {
        if (closing) {
            return Status.Closed;
        } else if (!connectionEstablished(channelRef.get())) {
            return Status.Connecting;
        } else {
            if (saslChannelReady.get()) {
                return Status.Ready;
            } else {
                return Status.Connecting; // need to wait until sasl channel is also ready
            }
        }
    }

    @Override
    public void sendLoadMetrics(Map<Integer, Double> taskToLoad) {
        throw new RuntimeException("Client connection should not send load metrics");
    }

    @Override
    public void sendBackPressureStatus(BackPressureStatus bpStatus) {
        throw new RuntimeException("Client connection should not send BackPressure status");
    }

    /**
     * Enqueue task messages to be sent to the remote destination (cf. `host` and `port`).
     */
    @Override
    public void send(Iterator<TaskMessage> msgs) {
        if (closing) {
            int numMessages = iteratorSize(msgs);
            LOG.error("Dropping {} messages because the Netty client to {} is being closed", numMessages,
                      dstAddressPrefixedName);
            return;
        }

        if (!hasMessages(msgs)) {
            return;
        }

        Channel channel = getConnectedChannel();
        if (channel == null) {
            /*
             * Connection is unavailable. We will drop pending messages and let at-least-once message replay kick in.
             *
             * Another option would be to buffer the messages in memory.  But this option has the risk of causing OOM errors,
             * especially for topologies that disable message acking because we don't know whether the connection recovery will
             * succeed  or not, and how long the recovery will take.
             */
            dropMessages(msgs);
            return;
        }
        try {
            while (msgs.hasNext()) {
                TaskMessage message = msgs.next();
                MessageBatch batch = batcher.add(message);
                if (batch != null) {
                    writeMessage(channel, batch);
                }
            }
            MessageBatch batch = batcher.drain();
            if (batch != null) {
                writeMessage(channel, batch);
            }
        } catch (IOException e) {
            LOG.warn("Exception when sending message to remote worker.", e);
            dropMessages(msgs);
        }
    }

    private void writeMessage(Channel channel, MessageBatch batch) throws IOException {
        try {
            int idleCounter = 0;
            while (!channel.isWritable()) {
                if (idleCounter == 0) { // check avoids multiple log msgs when in a idle loop
                    LOG.debug("Experiencing Back Pressure from Netty. Entering BackPressure Wait");
                }
                if (!channel.isActive()) {
                    throw new IOException("Connection disconnected");
                }
                idleCounter = waitStrategy.idle(idleCounter);
            }
            flushMessages(channel, batch);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Channel getConnectedChannel() {
        Channel channel = channelRef.get();
        if (connectionEstablished(channel)) {
            return channel;
        } else {
            // Closing the channel and reconnecting should be done before handling the messages.
            boolean reconnectScheduled = closeChannelAndReconnect(channel);
            if (reconnectScheduled) {
                // Log the connection error only once
                LOG.error("connection to {} is unavailable", dstAddressPrefixedName);
            }
            return null;
        }
    }

    public InetSocketAddress getDstAddress() {
        return dstAddress;
    }

    private boolean hasMessages(Iterator<TaskMessage> msgs) {
        return msgs != null && msgs.hasNext();
    }

    private void dropMessages(Iterator<TaskMessage> msgs) {
        // We consume the iterator by traversing and thus "emptying" it.
        int msgCount = iteratorSize(msgs);
        messagesLost.getAndAdd(msgCount);
        LOG.info("Dropping {} messages", msgCount);
    }

    private int iteratorSize(Iterator<TaskMessage> msgs) {
        int size = 0;
        if (msgs != null) {
            while (msgs.hasNext()) {
                size++;
                msgs.next();
            }
        }
        return size;
    }

    /**
     * Asynchronously writes the message batch to the channel.
     *
     * <p>If the write operation fails, then we will close the channel and trigger a reconnect.
     */
    private void flushMessages(final Channel channel, final MessageBatch batch) {
        if (null == batch || batch.isEmpty()) {
            return;
        }

        final int numMessages = batch.size();
        LOG.debug("writing {} messages to channel {}", batch.size(), channel.toString());
        pendingMessages.addAndGet(numMessages);

        ChannelFuture future = channel.writeAndFlush(batch);
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                pendingMessages.addAndGet(0 - numMessages);
                if (future.isSuccess()) {
                    LOG.debug("sent {} messages to {}", numMessages, dstAddressPrefixedName);
                    messagesSent.getAndAdd(batch.size());
                } else {
                    LOG.error("failed to send {} messages to {}: {}", numMessages, dstAddressPrefixedName,
                              future.cause());
                    closeChannelAndReconnect(future.channel());
                    messagesLost.getAndAdd(numMessages);
                }
            }

        });
    }

    /**
     * Schedule a reconnect if we closed a non-null channel, and acquired the right to provide a replacement by successfully setting a null
     * to the channel field.
     *
     * @param channel the channel to close
     * @return if the call scheduled a re-connect task
     */
    private boolean closeChannelAndReconnect(Channel channel) {
        if (channel != null) {
            channel.close();
            if (channelRef.compareAndSet(channel, null)) {
                scheduleConnect(NO_DELAY_MS);
                return true;
            }
        }
        return false;
    }

    @Override
    public int getPort() {
        return dstAddress.getPort();
    }

    /**
     * Gracefully close this client.
     */
    @Override
    public void close() {
        if (!closing) {
            LOG.info("closing Netty Client {}", dstAddressPrefixedName);
            // Set closing to true to prevent any further reconnection attempts.
            closing = true;
            waitForPendingMessagesToBeSent();
            closeChannel();

            // stop tracking metrics for this client
            if (this.metricRegistry != null) {
                this.metricRegistry.deregister(this.metrics);
            }
        }
    }

    private void waitForPendingMessagesToBeSent() {
        LOG.info("waiting up to {} ms to send {} pending messages to {}",
                 PENDING_MESSAGES_FLUSH_TIMEOUT_MS, pendingMessages.get(), dstAddressPrefixedName);
        long totalPendingMsgs = pendingMessages.get();
        long startMs = System.currentTimeMillis();
        while (pendingMessages.get() != 0) {
            try {
                long deltaMs = System.currentTimeMillis() - startMs;
                if (deltaMs > PENDING_MESSAGES_FLUSH_TIMEOUT_MS) {
                    LOG.error("failed to send all pending messages to {} within timeout, {} of {} messages were not "
                        + "sent", dstAddressPrefixedName, pendingMessages.get(), totalPendingMsgs);
                    break;
                }
                Thread.sleep(PENDING_MESSAGES_FLUSH_INTERVAL_MS);
            } catch (InterruptedException e) {
                break;
            }
        }

    }

    private void closeChannel() {
        Channel channel = channelRef.get();
        if (channel != null) {
            channel.close();
            LOG.debug("channel to {} closed", dstAddressPrefixedName);
        }
    }

    void setLoadMetrics(Map<Integer, Double> taskToLoad) {
        this.serverLoad = taskToLoad;
    }

    @Override
    public Map<Integer, Load> getLoad(Collection<Integer> tasks) {
        Map<Integer, Double> loadCache = serverLoad;
        Map<Integer, Load> ret = new HashMap<>();
        if (loadCache != null) {
            double clientLoad = Math.min(pendingMessages.get(), 1024) / 1024.0;
            for (Integer task : tasks) {
                Double found = loadCache.get(task);
                if (found != null) {
                    ret.put(task, new Load(true, found, clientLoad));
                }
            }
        }
        return ret;
    }

    public Map<String, Object> getConfig() {
        return topoConf;
    }

    @Override
    public void channelReady(Channel channel) {
        saslChannelReady.set(true);
    }

    @Override
    public String name() {
        return (String) topoConf.get(Config.TOPOLOGY_NAME);
    }

    @Override
    public String secretKey() {
        return SaslUtils.getSecretKey(topoConf);
    }

    private String srcAddressName() {
        String name = null;
        Channel channel = channelRef.get();
        if (channel != null) {
            SocketAddress address = channel.localAddress();
            if (address != null) {
                name = address.toString();
            }
        }
        return name;
    }

    @Override
    public String toString() {
        return String.format("Netty client for connecting to %s", dstAddressPrefixedName);
    }

    /**
     * Asynchronously establishes a Netty connection to the remote address This task runs on a single thread shared among all clients, and
     * thus should not perform operations that block.
     */
    private class Connect implements TimerTask {

        private final InetSocketAddress address;

        Connect(InetSocketAddress address) {
            this.address = address;
        }

        private void reschedule(Throwable t) {
            String baseMsg = String.format("connection attempt %s to %s failed", connectionAttempts,
                                           dstAddressPrefixedName);
            String failureMsg = (t == null) ? baseMsg : baseMsg + ": " + t.toString();
            LOG.error(failureMsg);
            long nextDelayMs = retryPolicy.getSleepTimeMs(connectionAttempts.get(), 0);
            scheduleConnect(nextDelayMs);
        }


        @Override
        public void run(Timeout timeout) throws Exception {
            if (reconnectingAllowed()) {
                final int connectionAttempt = connectionAttempts.getAndIncrement();
                totalConnectionAttempts.getAndIncrement();

                LOG.debug("connecting to {} [attempt {}]", address.toString(), connectionAttempt);
                ChannelFuture future = bootstrap.connect(address);
                future.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        // This call returns immediately
                        Channel newChannel = future.channel();

                        if (future.isSuccess() && connectionEstablished(newChannel)) {
                            boolean setChannel = channelRef.compareAndSet(null, newChannel);
                            checkState(setChannel);
                            LOG.debug("successfully connected to {}, {} [attempt {}]", address.toString(), newChannel.toString(),
                                      connectionAttempt);
                            if (messagesLost.get() > 0) {
                                LOG.warn("Re-connection to {} was successful but {} messages has been lost so far", address.toString(),
                                         messagesLost.get());
                            }
                        } else {
                            Throwable cause = future.cause();
                            reschedule(cause);
                            if (newChannel != null) {
                                newChannel.close();
                            }
                        }
                    }
                });
            } else {
                close();
                throw new RuntimeException("Giving up to scheduleConnect to " + dstAddressPrefixedName + " after "
                    + connectionAttempts + " failed attempts. " + messagesLost.get() + " messages were lost");

            }
        }
    }
}
