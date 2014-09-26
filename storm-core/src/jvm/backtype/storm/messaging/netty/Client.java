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
package backtype.storm.messaging.netty;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.StormBoundedExponentialBackoffRetry;
import backtype.storm.utils.TimeCacheMap;
import backtype.storm.utils.Utils;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class Client implements IConnection {
    private static enum ClientState {
        NEW,
        CONNECTED,
        DISCONNECTED,
        CLOSED,
    }

    private class CachedTaskMessageHandler implements TimeCacheMap.ExpiredCallback<Long, ArrayList<TaskMessage>> {
        @Override
        public void expire(Long key, ArrayList<TaskMessage> val) {
            onTaskMessageTimeout(val);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    private static final String PREFIX = "Netty-Client-";

    // configure
    private final int max_retries;
    private final int base_sleep_ms;
    private final int max_sleep_ms;
    private final int buffer_size;
    private final int messageBatchSize;
    private final int flushCheckInterval;
    private final int messageTimeoutSecs;

    // netty
    private AtomicReference<Channel> channelRef;
    private final ClientBootstrap bootstrap;
    private InetSocketAddress remote_addr;
    private final ChannelFactory factory;

    // local
    private ClientState clientState;
    private AtomicLong pendings;
    private AtomicInteger curConnectionRetryTimes;
    private AtomicLong bufferMessageSize;
    private AtomicLong nextTaskMessageIndex;
    private AtomicLong curTaskMessageIndex;
    private AtomicLong flushCheckTimer;
    private long curMessageBatchSize;
    private Runnable connector;
    private Runnable flusher;
    private ChannelFutureListener channelFutureListener;
    private ScheduledExecutorService scheduler;
    private ClientFactory clientFactory;
    MessageBatch messageBatch = null;

    private final StormBoundedExponentialBackoffRetry retryPolicy;
    private TimeCacheMap<Long, ArrayList<TaskMessage>> cachedTaskMessages;

    @SuppressWarnings("rawtypes")
    Client(Map storm_conf, ChannelFactory factory, ScheduledExecutorService scheduler,
            ClientFactory clientFactory, String host, int port) {
        this.factory = factory;
        this.scheduler = scheduler;
        this.clientFactory = clientFactory;

        // Configure
        buffer_size = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_SEND_RECV_BUFFER_SIZE));
        max_retries = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MAX_RETRIES));
        base_sleep_ms = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS));
        max_sleep_ms = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS));

        messageBatchSize = Utils.getInt(storm_conf.get(Config.STORM_NETTY_MESSAGE_BATCH_SIZE), 262144); // default 256K
        flushCheckInterval = Utils.getInt(storm_conf.get(Config.STORM_NETTY_FLUSH_CHECK_INTERVAL_MS), 10); // default 10 ms
        messageTimeoutSecs = Utils.getInt(storm_conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));

        LOG.info("New Netty Client, connect to " + host + ", " + port
                + ", config: " + ", send_buffer_size: " + buffer_size);

        // netty
        channelRef = new AtomicReference<Channel>(null);
        bootstrap = new ClientBootstrap(factory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("sendBufferSize", buffer_size);
        bootstrap.setOption("keepAlive", true);
        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new StormClientPipelineFactory(this));
        remote_addr = new InetSocketAddress(host, port);

        clientState = ClientState.NEW;
        pendings = new AtomicLong(0);
        curConnectionRetryTimes = new AtomicInteger(0);
        bufferMessageSize = new AtomicLong(0);
        nextTaskMessageIndex = new AtomicLong(0);
        curTaskMessageIndex = new AtomicLong(0);
        flushCheckTimer = new AtomicLong(Long.MAX_VALUE);
        curMessageBatchSize = 0;

        cachedTaskMessages = new TimeCacheMap<Long, ArrayList<TaskMessage>>(messageTimeoutSecs, new CachedTaskMessageHandler());
        retryPolicy = new StormBoundedExponentialBackoffRetry(base_sleep_ms, max_sleep_ms, max_retries);
//        flushScheduler = Executors.newScheduledThreadPool(1, new NettyRenameThreadFactory("client-flush-service"));

        flusher = new Runnable() {
            @Override
            public void run() {
                try {
                    flushWithSchedule();
                } catch (Exception e) {
                    LOG.error("flush catch exception:", e);
                }
            }
        };
        connector = new Runnable() {
            @Override
            public void run() {
                try {
                    connect();
                } catch (Exception e) {
                    LOG.error("connect catch execption: " , e);
                }
            }
        };

        channelFutureListener = new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                onFlushFinished(channelFuture);
            }
        };

        // setup the connection async now
        scheduler.execute(connector);
    }

    @Override
    public void send(ArrayList<TaskMessage> taskMessages) {
        if (clientState == ClientState.CLOSED) {
            LOG.warn("Client : " + remote_addr.toString() + "has closed, send abort");
            return;
        }

        long taskMessageSize = 0;
        for (TaskMessage taskMessage : taskMessages) {
            taskMessageSize += taskMessage.length();
        }

        // this will block until there enough space to buffer the new arrival TaskMessages;
        clientFactory.onMessageAdd(taskMessageSize);
        cacheMessage(taskMessages);
    }

    private synchronized void cacheMessage(ArrayList<TaskMessage> taskMessages) {
        cachedTaskMessages.put(nextTaskMessageIndex.getAndIncrement(), taskMessages);
        // inform flush right now;
        scheduler.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    flush();
                } catch (Exception e) {
                    LOG.error("flush catch exception:", e);
                }
            }
        });
    }

    /**
     * connect to remote server
     */
    private synchronized void connect() {
        if (clientState == ClientState.CLOSED) {
            LOG.warn("Client : " + remote_addr.toString() + "has closed, connect abort");
            return;
        }

        assert (clientState == ClientState.NEW || clientState == ClientState.DISCONNECTED);
        if (clientState == ClientState.CONNECTED) {
            LOG.warn("Client : " + remote_addr.toString() + "has connected, connect abort");
            return;
        }
        Channel channel = channelRef.get();
        assert (channel == null);

        if (curConnectionRetryTimes.get() > max_retries) {
            LOG.warn("Remote address is not reachable. We will close this client ");
            close();
            return;
        }

        LOG.info("Reconnect started for {}... [{}]", name(), curConnectionRetryTimes);
        LOG.debug("connection started...");

        ChannelFuture future = bootstrap.connect(remote_addr);
        future.awaitUninterruptibly();
        Channel current = future.getChannel();
        if (!future.isSuccess()) {
            if (null != current) {
                current.close();
            }
            curConnectionRetryTimes.getAndIncrement();
            clientState = ClientState.DISCONNECTED;
            int sleep_time = retryPolicy.getSleepTimeMs(curConnectionRetryTimes.get(), 0);
            LOG.info("we will try to connect : " + remote_addr.toString() + " in " + sleep_time + "ms");
            scheduler.schedule(connector, sleep_time, TimeUnit.MILLISECONDS);
        } else {
            LOG.info("connection established to a remote host " + name());
            channelRef.set(current);
            curConnectionRetryTimes.set(0);
            clientState = ClientState.CONNECTED;
            scheduler.execute(flusher);
        }
    }

    private synchronized void flushWithSchedule() {
        flush();
        scheduler.schedule(flusher, flushCheckInterval, TimeUnit.MILLISECONDS);
    }

    private synchronized void flush() {
        if (clientState != ClientState.CONNECTED) {
            LOG.warn("Client : " + remote_addr.toString() + "has not connected, abort flush");
            return;
        }

        Channel channel = channelRef.get();
        assert(channel != null);
        if (!channel.isConnected()) {
            LOG.warn("channel not connected, we will reconnect to : " + remote_addr.toString());
            scheduler.execute(connector);
            return;
        }

        while (curTaskMessageIndex.get() < nextTaskMessageIndex.get()) {
            // right now channel is not writable, we will try later;
            if (!channel.isWritable()) {
                break;
            }

            ArrayList<TaskMessage> taskMessages = cachedTaskMessages.get(curTaskMessageIndex.getAndIncrement());
            // this taskMessages may has timeout and deleted by cachedTaskMessages;
            if (taskMessages == null) {
                continue;
            }

            for (TaskMessage taskMessage : taskMessages) {
                if (null == messageBatch) {
                    messageBatch = new MessageBatch(messageBatchSize);
                }

                messageBatch.add(taskMessage);
                curMessageBatchSize += taskMessage.length();
                if (messageBatch.isFull()) {
                    clientFactory.onMessageSendFinished(curMessageBatchSize);
                    curMessageBatchSize = 0;
                    MessageBatch toBeFlushed = messageBatch;
                    flushRequest(channel, toBeFlushed);
                    messageBatch = null;
                }
            }
        }

        if (null != messageBatch && !messageBatch.isEmpty()) {
            if (channel.isWritable()) {
                clientFactory.onMessageSendFinished(curMessageBatchSize);
                curMessageBatchSize = 0;
                // Flush as fast as we can to reduce the latency
                MessageBatch toBeFlushed = messageBatch;
                messageBatch = null;
                flushRequest(channel, toBeFlushed);
            }
        }
    }

    private synchronized void onFlushFinished(ChannelFuture channelFuture) {
        pendings.decrementAndGet();
        if (!channelFuture.isSuccess()) {
            LOG.info("failed to send requests to " + remote_addr.toString() + ": ", channelFuture.getCause());

            Channel channel = channelFuture.getChannel();
            if (null != channel) {
                channel.close();
                channelRef.compareAndSet(channel, null);
            }
            if (clientState != ClientState.CLOSED) {
                clientState = ClientState.DISCONNECTED;
                scheduler.execute(connector);
            }
        }
    }

    private synchronized void onTaskMessageTimeout(ArrayList<TaskMessage> taskMessages) {
        long taskMessageSize = 0;
        for (TaskMessage taskMessage : taskMessages) {
            taskMessageSize += taskMessage.length();
        }
        LOG.warn("message timeout before send out, we will drop " + taskMessageSize + " Byte messages");
        clientFactory.onMessageSendFinished(taskMessageSize);
    }

    public String name() {
        if (null != remote_addr) {
            return PREFIX + remote_addr.toString();
        }
        return "";
    }

    /**
     * gracefully close this client.
     * 
     * We will send all existing requests, and then invoke close_n_release()
     * method
     */
    public synchronized void close() {
        if (clientState != ClientState.CLOSED) {
            clientState = ClientState.CLOSED;
            LOG.info("Closing Netty Client " + name());

            long taskMessageSize = 0;
            for (long index = curTaskMessageIndex.get(); index < nextTaskMessageIndex.get(); ++index) {
                ArrayList<TaskMessage> taskMessages = cachedTaskMessages.get(index);
                if (taskMessages != null) {
                    for (TaskMessage taskMessage : taskMessages) {
                        taskMessageSize += taskMessage.length();
                    }
                }
            }
            clientFactory.onMessageSendFinished(taskMessageSize);
            cachedTaskMessages.cleanup();
            if (null != messageBatch && !messageBatch.isEmpty()) {
                MessageBatch toBeFlushed = messageBatch;
                Channel channel = channelRef.get();
                if (channel != null) {
                    flushRequest(channel, toBeFlushed);
                }
                messageBatch = null;
            }
        
            //wait for pendings to exit
            final long timeoutMilliSeconds = 600 * 1000; //600 seconds
            final long start = System.currentTimeMillis();
            
            LOG.info("Waiting for pending batchs to be sent with "+ name() + "..., timeout: {}ms, pendings: {}", timeoutMilliSeconds, pendings.get());
            
            while(pendings.get() != 0) {
                try {
                    long delta = System.currentTimeMillis() - start;
                    if (delta > timeoutMilliSeconds) {
                        LOG.error("Timeout when sending pending batchs with {}..., there are still {} pending batchs not sent", name(), pendings.get());
                        break;
                    }
                    Thread.sleep(1000); //sleep 1s
                } catch (InterruptedException e) {
                    break;
                } 
            }
            
            close_n_release();
        }
    }

    /**
     * close_n_release() is invoked after all messages have been sent.
     */
    private void close_n_release() {
        if (channelRef.get() != null) {
            channelRef.get().close();
            LOG.debug("channel {} closed",remote_addr);
        }
    }

    @Override
    public Iterator<TaskMessage> recv(int flags, int clientId) {
        throw new RuntimeException("Client connection should not receive any messages");
    }

    @Override
    public void send(int taskId, byte[] payload) {
        TaskMessage msg = new TaskMessage(taskId, payload);
        ArrayList<TaskMessage> wrapper = new ArrayList<TaskMessage>(1);
        wrapper.add(msg);
        send(wrapper);
    }

    private void flushRequest(Channel channel, final MessageBatch requests) {
        if (requests == null)
            return;

        pendings.incrementAndGet();
        ChannelFuture future = channel.write(requests);
        future.addListener(channelFutureListener);
    }
}

