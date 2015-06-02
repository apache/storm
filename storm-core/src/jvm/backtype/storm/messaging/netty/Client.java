/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.messaging.netty;

import backtype.storm.Config;
import backtype.storm.messaging.ConnectionWithStatus;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.metric.api.IStatefulObject;
import backtype.storm.utils.StormBoundedExponentialBackoffRetry;
import backtype.storm.utils.Utils;
import com.google.common.base.Throwables;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;

/**
 * A Netty client for sending task messages to a remote destination (Netty server).
 *
 * Implementation details:
 *
 * - Sending messages, i.e. writing to the channel, is performed asynchronously.
 * - Messages are sent in batches to optimize for network throughput at the expense of network latency.  The message
 *   batch size is configurable.
 * - Connecting and reconnecting are performed asynchronously.
 *     - Note: The current implementation drops any messages that are being enqueued for sending if the connection to
 *       the remote destination is currently unavailable.
 */
public class Client extends ConnectionWithStatus implements IStatefulObject {

    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    private static final String PREFIX = "Netty-Client-";
    private static final long NO_DELAY_MS = 0L;

    private final StormBoundedExponentialBackoffRetry retryPolicy;
    private final ClientBootstrap bootstrap;
    private final InetSocketAddress dstAddress;
    protected final String dstAddressPrefixedName;

    /**
     * The channel used for all write operations from this client to the remote destination.
     */
    private final AtomicReference<Channel> channelRef = new AtomicReference<Channel>();


    /**
     * Maximum number of reconnection attempts we will perform after a disconnect before giving up.
     */
    private final int maxReconnectionAttempts;

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
     * This flag is set to true if and only if a client instance is being closed.
     */
    private volatile boolean closing = false;

    /**
     * How many messages should be batched together before sending them to the remote destination.
     *
     * Messages are batched to optimize network throughput at the expense of latency.
     */
    private final int messageBatchSize;

    private final ScheduledExecutorService scheduler;

    private final Object pendingMessageLock = new Object();
    private MessageBatch pendingMessage;
    private Timeout pendingFlush;

    @SuppressWarnings("rawtypes")
    Client(Map stormConf, ChannelFactory factory, ScheduledExecutorService scheduler, String host, int port) {
        closing = false;
        this.scheduler = scheduler;
        int bufferSize = Utils.getInt(stormConf.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
        LOG.info("creating Netty Client, connecting to {}:{}, bufferSize: {}", host, port, bufferSize);
        messageBatchSize = Utils.getInt(stormConf.get(Config.STORM_NETTY_MESSAGE_BATCH_SIZE), 262144);

        maxReconnectionAttempts = Utils.getInt(stormConf.get(Config.STORM_MESSAGING_NETTY_MAX_RETRIES));
        int minWaitMs = Utils.getInt(stormConf.get(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS));
        int maxWaitMs = Utils.getInt(stormConf.get(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS));
        retryPolicy = new StormBoundedExponentialBackoffRetry(minWaitMs, maxWaitMs, maxReconnectionAttempts);

        // Initiate connection to remote destination
        bootstrap = createClientBootstrap(factory, bufferSize);
        dstAddress = new InetSocketAddress(host, port);
        dstAddressPrefixedName = prefixedName(dstAddress);
        scheduleConnect(NO_DELAY_MS);

        // Dummy values to avoid null checks
        pendingMessage = new MessageBatch(messageBatchSize);
        scheduler.scheduleWithFixedDelay(new Flush(), 10, 10, TimeUnit.MILLISECONDS);
    }

    private ClientBootstrap createClientBootstrap(ChannelFactory factory, int bufferSize) {
        ClientBootstrap bootstrap = new ClientBootstrap(factory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("sendBufferSize", bufferSize);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setPipelineFactory(new StormClientPipelineFactory(this));
        return bootstrap;
    }

    private String prefixedName(InetSocketAddress dstAddress) {
        if (null != dstAddress) {
            return PREFIX + dstAddress.toString();
        }
        return "";
    }

    /**
     * We will retry connection with exponential back-off policy
     */
    private void scheduleConnect(long delayMs) {
        scheduler.schedule(new Connect(dstAddress), delayMs, TimeUnit.MILLISECONDS);
    }

    private boolean reconnectingAllowed() {
        return !closing && connectionAttempts.get() <= (maxReconnectionAttempts + 1);
    }

    private boolean connectionEstablished(Channel channel) {
        // Because we are using TCP (which is a connection-oriented transport unlike UDP), a connection is only fully
        // established iff the channel is connected.  That is, a TCP-based channel must be in the CONNECTED state before
        // anything can be read or written to the channel.
        //
        // See:
        // - http://netty.io/3.9/api/org/jboss/netty/channel/ChannelEvent.html
        // - http://stackoverflow.com/questions/13356622/what-are-the-netty-channel-state-transitions
        return channel != null && channel.isConnected();
    }

    /**
     * Note:  Storm will check via this method whether a worker can be activated safely during the initial startup of a
     * topology.  The worker will only be activated once all of the its connections are ready.
     */
    @Override
    public Status status() {
        if (closing) {
            return Status.Closed;
        } else if (!connectionEstablished(channelRef.get())) {
            return Status.Connecting;
        } else {
            return Status.Ready;
        }
    }

    /**
     * Receiving messages is not supported by a client.
     *
     * @throws java.lang.UnsupportedOperationException whenever this method is being called.
     */
    @Override
    public Iterator<TaskMessage> recv(int flags, int clientId) {
        throw new UnsupportedOperationException("Client connection should not receive any messages");
    }

    @Override
    public void send(int taskId, byte[] payload) {
        TaskMessage msg = new TaskMessage(taskId, payload);
        List<TaskMessage> wrapper = new ArrayList<TaskMessage>(1);
        wrapper.add(msg);
        send(wrapper.iterator());
    }

    /**
     * Enqueue task messages to be sent to the remote destination (cf. `host` and `port`).
     */
    @Override
    public void send(Iterator<TaskMessage> msgs) {
        if (closing) {
            int numMessages = iteratorSize(msgs);
            LOG.error("discarding {} messages because the Netty client to {} is being closed", numMessages,
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

        MessageBatch replacement = new MessageBatch(messageBatchSize);
        MessageBatch previous;
        synchronized (pendingMessageLock) {
            // pendingMessage is never null
            previous = pendingMessage;
            pendingMessage = replacement;

            // We are flushing the pending messages, therefore we can cancel the current pending flush
            // The cancel is idempotent
            pendingFlush.cancel();
        }

        // Collect messages into batches (to optimize network throughput)
        Batches batches = createBatches(previous, msgs);

        // Then flush the batches that are full
        flushMessages(channel, batches.fullBatches);

        if (batches.unfilled.isEmpty()) {
            // All messages ended up neatly into batches; there are no unfilled MessageBatch
            return;
        }

        if (channel.isWritable()) {
            // Netty's internal buffer is not full. We should write the unfilled MessageBatch immediately
            // to reduce latency
            flushMessages(channel, batches.unfilled);
        } else {
            // We have an unfilled MessageBatch, but Netty's internal buffer is full, meaning that we have time.
            // In this situation, waiting for more messages before handing it to Netty yields better throughput
            queueUp(channel, batches.unfilled);
        }
    }

    private void queueUp(Channel channel, MessageBatch unfilled) {
        Batches batches;
        synchronized (pendingMessageLock) {
            batches = createBatches(pendingMessage, unfilled.getMsgs().iterator());
            // We have a MessageBatch that isn't full yet, so we will wait for more messages.
            pendingMessage = batches.unfilled;
        }

        // MessageBatches that were filled are immediately handed to Netty
        flushMessages(channel, batches.fullBatches);

    }


    private static class Batches {
        final List<MessageBatch> fullBatches;
        final MessageBatch unfilled;

        private Batches(List<MessageBatch> fullBatches, MessageBatch unfilled) {
            this.fullBatches = fullBatches;
            this.unfilled = unfilled;
        }
    }

    private Batches createBatches(MessageBatch previous, Iterator<TaskMessage> msgs){
        List<MessageBatch> ret = new ArrayList<MessageBatch>();
        while (msgs.hasNext()) {
            TaskMessage message = msgs.next();
            previous.add(message);
            if (previous.isFull()) {
                ret.add(previous);
                previous = new MessageBatch(messageBatchSize);
            }
        }

        return new Batches(ret, previous);
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

    private boolean hasMessages(Iterator<TaskMessage> msgs) {
        return msgs != null && msgs.hasNext();
    }


    private void dropMessages(Iterator<TaskMessage> msgs) {
        // We consume the iterator by traversing and thus "emptying" it.
        int msgCount = iteratorSize(msgs);
        messagesLost.getAndAdd(msgCount);
    }

    private void dropMessages(MessageBatch msgs) {
        messagesLost.getAndAdd(msgs.size());
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

    private void flushMessages(Channel channel, List<MessageBatch> batches) {
        for (MessageBatch batch : batches) {
            flushMessages(channel, batch);
        }
    }


    /**
     * Asynchronously writes the message batch to the channel.
     *
     * If the write operation fails, then we will close the channel and trigger a reconnect.
     */
    private void flushMessages(Channel channel, final MessageBatch batch) {
        final int numMessages = batch.size();
        LOG.debug("writing {} messages to channel {}", batch.size(), channel.toString());
        pendingMessages.addAndGet(numMessages);

        ChannelFuture future = channel.write(batch);
        future.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future) throws Exception {
                pendingMessages.addAndGet(0 - numMessages);
                if (future.isSuccess()) {
                    LOG.debug("sent {} messages to {}", numMessages, dstAddressPrefixedName);
                    messagesSent.getAndAdd(batch.size());
                } else {
                    LOG.error("failed to send {} messages to {}: {}", numMessages, dstAddressPrefixedName,
                            future.getCause());
                    closeChannelAndReconnect(future.getChannel());
                    messagesLost.getAndAdd(numMessages);
                }
            }

        });
    }

    /**
     * Schedule a reconnect if we closed a non-null channel, and acquired the right to
     * provide a replacement by successfully setting a null to the channel field
     * @param channel
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

    /**
     * Gracefully close this client.
     */
    @Override
    public void close() {
        if (!closing) {
            LOG.info("closing Netty Client {}", dstAddressPrefixedName);
            // Set closing to true to prevent any further reconnection attempts.
            closing = true;

            closeChannel();
        }
    }


    private void closeChannel() {
        Channel channel = channelRef.get();
        if (channel != null) {
            channel.close();
            LOG.debug("channel to {} closed", dstAddressPrefixedName);
        }
    }

    @Override
    public Object getState() {
        LOG.info("Getting metrics for client connection to {}", dstAddressPrefixedName);
        HashMap<String, Object> ret = new HashMap<String, Object>();
        ret.put("reconnects", totalConnectionAttempts.getAndSet(0));
        ret.put("sent", messagesSent.getAndSet(0));
        ret.put("pending", pendingMessages.get());
        ret.put("lostOnSend", messagesLost.getAndSet(0));
        ret.put("dest", dstAddress.toString());
        String src = srcAddressName();
        if (src != null) {
            ret.put("src", src);
        }
        return ret;
    }

    private String srcAddressName() {
        String name = null;
        Channel channel = channelRef.get();
        if (channel != null) {
            SocketAddress address = channel.getLocalAddress();
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
     * Asynchronously flushes pending messages to the remote address, if they have not been
     * flushed by other means.
     * This task runs on a single thread shared among all clients, and thus
     * should not perform operations that block or are expensive.
     */
    private class Flush implements Runnable {
        @Override
        public void run() {
            try {
                Channel channel = getConnectedChannel();
                if (channel == null || !channel.isWritable()) {
                    // Connection not available or buffer is full, no point in flushing
                    return;
                } else {
                    // Connection is available and there is room in Netty's buffer
                    MessageBatch toSend;
                    synchronized (pendingMessageLock) {
                        if(pendingMessage.isEmpty()){
                            // Nothing to flush
                            return;
                        } else {
                            toSend = pendingMessage;
                            pendingMessage = new MessageBatch(messageBatchSize);
                        }
                    }
                    checkState(!toSend.isFull(), "Filled batches should never be in pendingMessage field");

                    flushMessages(channel, toSend);
                }
            }catch (Throwable e){
                LOG.error("Uncaught throwable", e);
                throw Throwables.propagate(e);
            }
        }
    }

    /**
     * Asynchronously establishes a Netty connection to the remote address
     * This task runs on a single thread shared among all clients, and thus
     * should not perform operations that block.
     */
    private class Connect implements Runnable {

        private final InetSocketAddress address;

        public Connect(InetSocketAddress address) {
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
        public void run() {
            try {
                if (reconnectingAllowed()) {
                    final int connectionAttempt = connectionAttempts.getAndIncrement();
                    totalConnectionAttempts.getAndIncrement();

                    LOG.debug("connecting to {} [attempt {}]", address.toString(), connectionAttempt);
                    ChannelFuture future = bootstrap.connect(address);
                    future.addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            // This call returns immediately
                            Channel newChannel = future.getChannel();

                            if (future.isSuccess() && connectionEstablished(newChannel)) {
                                boolean setChannel = channelRef.compareAndSet(null, newChannel);
                                checkState(setChannel);
                                LOG.debug("successfully connected to {}, {} [attempt {}]", address.toString(), newChannel.toString(),
                                        connectionAttempt);
                                if (messagesLost.get() > 0) {
                                    LOG.warn("Re-connection to {} was successful but {} messages has been lost so far", address.toString(), messagesLost.get());
                                }
                            } else {
                                Throwable cause = future.getCause();
                                reschedule(cause);
                                if (newChannel != null) {
                                    newChannel.close();
                                }
                            }
                        }
                    });
                } else {
                    close();
                    throw new RuntimeException("Giving up to scheduleConnect to " + dstAddressPrefixedName + " after " +
                            connectionAttempts + " failed attempts. " + messagesLost.get() + " messages were lost");

                }
            }catch (Throwable e){
                LOG.error("Uncaught throwable", e);
                throw Throwables.propagate(e);
            }
        }
    }

}
