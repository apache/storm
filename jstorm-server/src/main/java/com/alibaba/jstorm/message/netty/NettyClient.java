package com.alibaba.jstorm.message.netty;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.WaitStrategy;

class NettyClient implements IConnection, EventHandler {
    private static final Logger         LOG    = LoggerFactory.getLogger(NettyClient.class);
    private final int                   max_retries;
    private final int                   base_sleep_ms;
    private final int                   max_sleep_ms;
    private LinkedBlockingQueue<Object> blockingQueue;                                      //entry should either be TaskMessage or ControlMessage
    private final DisruptorQueue        disruptorQueue;
    private final boolean               useDisruptor;
    private AtomicReference<Channel>    channelRef;
    private final ClientBootstrap       bootstrap;
    private InetSocketAddress           remote_addr;
    private final String                target_Server;
    private AtomicInteger               retries;
    //private final Random                random = new Random();
    private final ChannelFactory        factory;
    
    private final int                   buffer_size;
    private final AtomicBoolean         being_closed;
    // if batch set true, at first performance is pretty good, 
    // but after a while it is very pool
    private final boolean               batchHighLevel = false;

    @SuppressWarnings("rawtypes")
    NettyClient(Map storm_conf, String host, int port) {
        retries = new AtomicInteger(0);
        channelRef = new AtomicReference<Channel>(null);
        being_closed = new AtomicBoolean(false);
        // Configure 
        buffer_size = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
        max_retries = Math.min(30,
            Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MAX_RETRIES)));
        base_sleep_ms = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS));
        max_sleep_ms = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS));
        useDisruptor = ConfigExtension.isNettyEnableDisruptor(storm_conf);

        int                   queue_size; 
        
        if (batchHighLevel == true) {
            queue_size = 16;
            scheduExec = Executors.newSingleThreadScheduledExecutor();
            scheduExec.scheduleAtFixedRate(new Flush(), 1, 1,
                    TimeUnit.SECONDS);
        }else {
            queue_size = Utils.getInt(storm_conf.get(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE));
        }
        WaitStrategy          waitStrategy;
        waitStrategy = (WaitStrategy) Utils.newInstance((String) storm_conf
            .get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));

        disruptorQueue = new DisruptorQueue(new SingleThreadedClaimStrategy(queue_size),
            waitStrategy);

        blockingQueue = new LinkedBlockingQueue<Object>();   

        int maxWorkers = Utils.getInt(storm_conf
            .get(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS));

        // worker thread number is 1
        factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool(), 1);

        bootstrap = new ClientBootstrap(factory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("sendBufferSize", buffer_size);
        bootstrap.setOption("keepAlive", true);

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new StormClientPipelineFactory(this));

        // Start the connection attempt.
        remote_addr = new InetSocketAddress(host, port);
        bootstrap.connect(remote_addr);

        target_Server = host + ":" + port;
        LOG.info("Begin to connect {}, useDisrutpor: {}", target_Server, useDisruptor);

    }

    /**
     * We will retry connection with exponential back-off policy
     */
    void reconnect() {
        try {
//            int tried_count = retries.incrementAndGet();
//            if (tried_count <= max_retries) {
//                Thread.sleep(getSleepTimeMs());
//                LOG.info("Reconnect ... [{}], {}", tried_count, target_Server);
//                bootstrap.connect(remote_addr);
//            } else {
//                LOG.warn("Remote address is not reachable. We will close this client {}.",
//                    target_Server);
//                close();
//            }
            if (isClosed() == false) {
                int tried_count = retries.incrementAndGet();
                Thread.sleep(getSleepTimeMs());
                LOG.info("Reconnect ... [{}], {}", tried_count, target_Server);
                bootstrap.connect(remote_addr);
            }
            
        } catch (InterruptedException e) {
            if (isClosed() == false) {
                LOG.warn("connection failed", e);
            }
        }
    }

    /**
     * # of milliseconds to wait per exponential back-off policy
     */
    private int getSleepTimeMs() {
//        int backoff = 1 << Math.max(1, retries.get());
//        int sleepMs = base_sleep_ms * Math.max(1, random.nextInt(backoff));
//        if (sleepMs > max_sleep_ms)
//            sleepMs = max_sleep_ms;
//        if (sleepMs < base_sleep_ms)
//            sleepMs = base_sleep_ms;
        
        return base_sleep_ms * retries.get();
    }

    /**
     * Enqueue a task message to be sent to server 
     */
    @Override
    public void send(int task, byte[] message) {
        //throw exception if the client is being closed
        if (isClosed()) {
            throw new RuntimeException(
                "Client is being closed, and does not take requests any more");
        }

        try {
            push(new TaskMessage(task, message));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void push(Object obj) {
        if (useDisruptor) {
            if (batchHighLevel) {
                pushBatch((TaskMessage)obj);
            }else {
                disruptorQueue.publish(obj);
            }
            
        } else {
            blockingQueue.offer(obj);
        }
    }

    private boolean tryPush(Object obj) {
        if (useDisruptor) {
            try {
                disruptorQueue.tryPublish(obj);
            } catch (InsufficientCapacityException e) {
                return false;
            }
            return true;
        } else {
            return blockingQueue.offer(obj);
        }
    }

    private MessageBatch takeFromBlockQ() throws InterruptedException {
        //1st message
        MessageBatch batch = new MessageBatch(buffer_size);
        Object msg = blockingQueue.take();
        batch.add(msg);

        //we will discard any message after CLOSE
        if (msg == ControlMessage.CLOSE_MESSAGE)
            return batch;

        while (!batch.isFull()) {
            //peek the next message
            msg = blockingQueue.peek();
            //no more messages
            if (msg == null)
                break;

            //we will discard any message after CLOSE
            if (msg == ControlMessage.CLOSE_MESSAGE) {
                blockingQueue.take();
                batch.add(msg);
                break;
            }

            //try to add this msg into batch
            if (!batch.tryAdd((TaskMessage) msg))
                break;

            //remove this message
            blockingQueue.take();
        }

        return batch;
    }

    private ConcurrentLinkedQueue<Object>       msgCacheList = new ConcurrentLinkedQueue<Object>();
    private ConcurrentLinkedQueue<MessageBatch> readyBatchs  = new ConcurrentLinkedQueue<MessageBatch>();

    @Override
    public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {
        if (event == null) {
            // set endOfBatch as true
            endOfBatch = true;
        } else {
            msgCacheList.add(event);
        }

        if (!endOfBatch) {
            return;
        }

        MessageBatch batch = new MessageBatch(buffer_size);
        while (true) {
            Object _event = msgCacheList.poll();
            if (_event == null) {
                break;
            }
            //discard any message after CLOSE
            if (_event == ControlMessage.CLOSE_MESSAGE) {
                batch.add(_event);
                break;
            }

            TaskMessage message = (TaskMessage) _event;
            batch.add(message);
            
            if (batch.isFull() == true) {
                //rebuild a messageBatch because of asynchronous send
                readyBatchs.offer(batch);
                batch = new MessageBatch(buffer_size);
            }
        }

        if (batch.isEmpty() == false) {
            readyBatchs.offer(batch);
        }
        

    }
    
    private MessageBatch currentBatch = new MessageBatch((int)(5 * JStormUtils.SIZE_1_M));
    private AtomicBoolean flush = new AtomicBoolean(false);
    private ScheduledExecutorService        scheduExec = null;
    
    class Flush implements Runnable {

        @Override
        public void run() {
            flush.set(true);
        }
        
    }
    
    private boolean checkFlush() {
        return currentBatch.isFull();
//        boolean isFlush = flush.get();
//        
//        if (isFlush == false) {
//            return currentBatch.isFull();
//        }else {
//            // if disruptorQueue is empty, return true,
//            // else still batch
//            return disruptorQueue.population() == 0;
//        }
    }
    void pushBatch(TaskMessage message) {

        currentBatch.add(message);
        if ( checkFlush() == true) {
            //rebuild a messageBatch because of asynchronous send
            flush.set(false);
            
            disruptorQueue.publish(currentBatch);
            
            currentBatch = new MessageBatch(buffer_size);
        }
    }

    MessageBatch taskFromeDisruptorQ() throws Exception {
        if (batchHighLevel) {
            //flush.set(true);
            MessageBatch retBatch = (MessageBatch) disruptorQueue.take();

            return retBatch;
        } else {
            MessageBatch retBatch = readyBatchs.poll();
            if (retBatch == null) {
                disruptorQueue.consumeBatchWhenAvailable(this);
                retBatch = readyBatchs.poll();
            }
            return retBatch;
        }

    }

    /**
     * Take all enqueued messages from queue
     * @return
     * @throws InterruptedException
     */
    MessageBatch takeMessages() throws Exception {
        if (useDisruptor) {
            return taskFromeDisruptorQ();
        } else {
            return takeFromBlockQ();
        }
    }

    /**
     * gracefully close this client.
     * 
     * We will send all existing requests, and then invoke close_n_release() method
     */
    public synchronized void close() {

        if (!isClosed()) {
            being_closed.set(true);
            //enqueue a CLOSE message so that shutdown() will be invoked 
            //            if (!tryPush(ControlMessage.CLOSE_MESSAGE)) {
            //            	LOG.warn("client can't send CLOSE message because of queue is full");
            //            	
            //            } 

            close_n_release();

        }

    }

    /**
     * close_n_release() is invoked after all messages have been sent.
     */
    void close_n_release() {
        if (channelRef.get() != null)
            channelRef.get().close().awaitUninterruptibly();

        //we need to release resources 
        new Thread(new Runnable() {
            @Override
            public void run() {
                if (useDisruptor) {
                    disruptorQueue.clear();
                }
                factory.releaseExternalResources();
                
                LOG.info("Successfully close connection to {}", target_Server);
            }
        }).start();
    }

    public byte[] recv(int flags) {
        throw new UnsupportedOperationException("Client connection should not receive any messages");
    }

    void setChannel(Channel channel) {
        channelRef.set(channel);
        //reset retries   
        if (channel != null)
            retries.set(0);
    }

    @Override
    public boolean isClosed() {
        return being_closed.get();
    }

    public int getBuffer_size() {
        return buffer_size;
    }

    public String getTarget_Server() {
        return target_Server;
    }

}
