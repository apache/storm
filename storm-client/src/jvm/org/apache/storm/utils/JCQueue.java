/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.storm.utils;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.storm.metric.api.IStatefulObject;
import org.apache.storm.metric.internal.RateTracker;
import org.apache.storm.metrics2.JcMetrics;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.policy.IWaitStrategy;
import org.apache.storm.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.storm.shade.org.jctools.queues.MessagePassingQueue;
import org.apache.storm.shade.org.jctools.queues.MpscArrayQueue;
import org.apache.storm.shade.org.jctools.queues.MpscUnboundedArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JCQueue implements IStatefulObject, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(JCQueue.class);
    private static final String PREFIX = "jc-";
    private static final ScheduledThreadPoolExecutor METRICS_REPORTER_EXECUTOR =
        new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(PREFIX + "metrics-reporter")
                .build());
    private final ExitCondition continueRunning = () -> true;
    private final JcMetrics jcMetrics;
    private final MpscArrayQueue<Object> recvQueue;
    // only holds msgs from other workers (via WorkerTransfer), when recvQueue is full
    private final MpscUnboundedArrayQueue<Object> overflowQ;
    private final int overflowLimit; // ensures... overflowCount <= overflowLimit. if set to 0, disables overflow.
    private final int producerBatchSz;
    private final DirectInserter directInserter = new DirectInserter(this);
    private final ThreadLocal<BatchInserter> thdLocalBatcher = new ThreadLocal<BatchInserter>(); // ensure 1 instance per producer thd.
    private final JCQueue.QueueMetrics metrics;
    private final IWaitStrategy backPressureWaitStrategy;
    private final String queueName;

    public JCQueue(String queueName, int size, int overflowLimit, int producerBatchSz, IWaitStrategy backPressureWaitStrategy,
                   String topologyId, String componentId, Integer taskId, int port) {
        this.queueName = queueName;
        this.overflowLimit = overflowLimit;
        this.recvQueue = new MpscArrayQueue<>(size);
        this.overflowQ = new MpscUnboundedArrayQueue<>(size);

        this.metrics = new JCQueue.QueueMetrics();
        this.jcMetrics = StormMetricRegistry.jcMetrics(queueName, topologyId, componentId, taskId, port);

        //The batch size can be no larger than half the full recvQueue size, to avoid contention issues.
        this.producerBatchSz = Math.max(1, Math.min(producerBatchSz, size / 2));
        this.backPressureWaitStrategy = backPressureWaitStrategy;

        if (!METRICS_REPORTER_EXECUTOR.isShutdown()) {
            METRICS_REPORTER_EXECUTOR.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    jcMetrics.set(metrics);
                }
            }, 15, 15, TimeUnit.SECONDS);
        }
    }

    public String getName() {
        return queueName;
    }

    @Override
    public void close() {
        //No need to block, the task run by the executor is safe to run even after metrics are closed
        METRICS_REPORTER_EXECUTOR.shutdown();
        metrics.close();
    }

    /**
     * Non blocking. Returns immediately if Q is empty. Returns number of elements consumed from Q
     */
    public int consume(JCQueue.Consumer consumer) {
        return consume(consumer, continueRunning);
    }

    /**
     * Non blocking. Returns immediately if Q is empty. Runs till Q is empty OR exitCond.keepRunning() return false. Returns number of
     * elements consumed from Q
     */
    public int consume(JCQueue.Consumer consumer, ExitCondition exitCond) {
        try {
            return consumeImpl(consumer, exitCond);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public int size() {
        return recvQueue.size() + overflowQ.size();
    }

    /**
     * Non blocking. Returns immediately if Q is empty. Returns number of elements consumed from Q
     *
     * @param consumer
     * @param exitCond
     */
    private int consumeImpl(Consumer consumer, ExitCondition exitCond) throws InterruptedException {
        int drainCount = 0;
        while (exitCond.keepRunning()) {
            Object tuple = recvQueue.poll();
            if (tuple == null) {
                break;
            }
            consumer.accept(tuple);
            ++drainCount;
        }

        int overflowDrainCount = 0;
        int limit = overflowQ.size();
        while (exitCond.keepRunning() && (overflowDrainCount < limit)) { // 2nd cond prevents staying stuck with consuming overflow
            Object tuple = overflowQ.poll();
            ++overflowDrainCount;
            consumer.accept(tuple);
        }
        int total = drainCount + overflowDrainCount;
        if (total > 0) {
            consumer.flush();
        }
        return total;
    }

    // Non Blocking. returns true/false indicating success/failure. Fails if full.
    private boolean tryPublishInternal(Object obj) {
        if (recvQueue.offer(obj)) {
            metrics.notifyArrivals(1);
            return true;
        }
        return false;
    }

    // Non Blocking. returns count of how many inserts succeeded
    private int tryPublishInternal(ArrayList<Object> objs) {
        MessagePassingQueue.Supplier<Object> supplier =
            new MessagePassingQueue.Supplier<Object>() {
                int i = 0;

                @Override
                public Object get() {
                    return objs.get(i++);
                }
            };
        int count = recvQueue.fill(supplier, objs.size());
        metrics.notifyArrivals(count);
        return count;
    }

    private Inserter getInserter() {
        Inserter inserter;
        if (producerBatchSz > 1) {
            inserter = thdLocalBatcher.get();
            if (inserter == null) {
                BatchInserter b = new BatchInserter(this, producerBatchSz);
                inserter = b;
                thdLocalBatcher.set(b);
            }
        } else {
            inserter = directInserter;
        }
        return inserter;
    }

    /**
     * Blocking call. Retries till it can successfully publish the obj. Can be interrupted via Thread.interrupt().
     */
    public void publish(Object obj) throws InterruptedException {
        Inserter inserter = getInserter();
        inserter.publish(obj);
    }

    /**
     * Non-blocking call, returns false if full.
     **/
    public boolean tryPublish(Object obj) {
        Inserter inserter = getInserter();
        return inserter.tryPublish(obj);
    }

    /**
     * Non-blocking call. Bypasses any batching that may be enabled on the recvQueue. Intended for sending flush/metrics tuples
     */
    public boolean tryPublishDirect(Object obj) {
        return tryPublishInternal(obj);
    }

    /**
     * Un-batched write to overflowQ. Should only be called by WorkerTransfer returns false if overflowLimit has reached
     */
    public boolean tryPublishToOverflow(Object obj) {
        if (overflowLimit > 0 && overflowQ.size() >= overflowLimit) {
            return false;
        }
        overflowQ.add(obj);
        return true;
    }

    public void recordMsgDrop() {
        getMetrics().notifyDroppedMsg();
    }

    public boolean isEmptyOverflow() {
        return overflowQ.isEmpty();
    }

    public int getOverflowCount() {
        return overflowQ.size();
    }

    public int getQueuedCount() {
        return recvQueue.size();
    }

    /**
     * if(batchSz>1)  : Blocking call. Does not return until at least 1 element is drained or Thread.interrupt() is received if(batchSz==1)
     * : NO-OP. Returns immediately. doesnt throw.
     */
    public void flush() throws InterruptedException {
        Inserter inserter = getInserter();
        inserter.flush();
    }

    /**
     * if(batchSz>1)  : Non-Blocking call. Tries to flush as many as it can. Returns true if flushed at least 1. if(batchSz==1) : This is a
     * NO-OP. Returns true immediately.
     */
    public boolean tryFlush() {
        Inserter inserter = getInserter();
        return inserter.tryFlush();
    }

    @Override
    public Object getState() {
        return metrics.getState();
    }

    //This method enables the metrics to be accessed from outside of the JCQueue class
    public JCQueue.QueueMetrics getMetrics() {
        return metrics;
    }

    private interface Inserter {
        // blocking call that can be interrupted using Thread.interrupt()
        void publish(Object obj) throws InterruptedException;

        boolean tryPublish(Object obj);

        void flush() throws InterruptedException;

        boolean tryFlush();
    }

    public interface Consumer extends MessagePassingQueue.Consumer<Object> {
        void accept(Object event);

        void flush() throws InterruptedException;
    }

    public interface ExitCondition {
        boolean keepRunning();
    }

    /* Thread safe. Same instance can be used across multiple threads */
    private static class DirectInserter implements Inserter {
        private JCQueue q;

        public DirectInserter(JCQueue q) {
            this.q = q;
        }

        /**
         * Blocking call, that can be interrupted via Thread.interrupt
         */
        @Override
        public void publish(Object obj) throws InterruptedException {
            boolean inserted = q.tryPublishInternal(obj);
            int idleCount = 0;
            while (!inserted) {
                q.metrics.notifyInsertFailure();
                if (idleCount == 0) { // check avoids multiple log msgs when in a idle loop
                    LOG.debug("Experiencing Back Pressure on recvQueue: '{}'. Entering BackPressure Wait", q.getName());
                }

                idleCount = q.backPressureWaitStrategy.idle(idleCount);
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                inserted = q.tryPublishInternal(obj);
            }

        }

        /**
         * Non-Blocking call. return value indicates success/failure
         */
        @Override
        public boolean tryPublish(Object obj) {
            boolean inserted = q.tryPublishInternal(obj);
            if (!inserted) {
                q.metrics.notifyInsertFailure();
                return false;
            }
            return true;
        }

        @Override
        public void flush() throws InterruptedException {
        }

        @Override
        public boolean tryFlush() {
            return true;
        }
    } // class DirectInserter

    /* Not thread safe. Have one instance per producer thread or synchronize externally */
    private static class BatchInserter implements Inserter {
        private final int batchSz;
        private JCQueue q;
        private ArrayList<Object> currentBatch;

        public BatchInserter(JCQueue q, int batchSz) {
            this.q = q;
            this.batchSz = batchSz;
            this.currentBatch = new ArrayList<>(batchSz + 1);
        }

        /**
         * Blocking call - retires till element is successfully added.
         */
        @Override
        public void publish(Object obj) throws InterruptedException {
            currentBatch.add(obj);
            if (currentBatch.size() >= batchSz) {
                flush();
            }
        }

        /**
         * Non-Blocking call. return value indicates success/failure
         */
        @Override
        public boolean tryPublish(Object obj) {
            if (currentBatch.size() >= batchSz) {
                if (!tryFlush()) {
                    return false;
                }
            }
            currentBatch.add(obj);
            return true;
        }

        /**
         * Blocking call - Does not return until at least 1 element is drained or Thread.interrupt() is received. Uses backpressure wait
         * strategy.
         */
        @Override
        public void flush() throws InterruptedException {
            if (currentBatch.isEmpty()) {
                return;
            }
            int publishCount = q.tryPublishInternal(currentBatch);
            int retryCount = 0;
            while (publishCount == 0) { // retry till at least 1 element is drained
                q.metrics.notifyInsertFailure();
                if (retryCount == 0) { // check avoids multiple log msgs when in a idle loop
                    LOG.debug("Experiencing Back Pressure when flushing batch to Q: {}. Entering BackPressure Wait.", q.getName());
                }
                retryCount = q.backPressureWaitStrategy.idle(retryCount);
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                publishCount = q.tryPublishInternal(currentBatch);
            }
            currentBatch.subList(0, publishCount).clear();
        }

        /**
         * Non blocking call. tries to flush as many as possible. Returns true if at least one from non-empty currentBatch was flushed or if
         * currentBatch is empty. Returns false otherwise
         */
        @Override
        public boolean tryFlush() {
            if (currentBatch.isEmpty()) {
                return true;
            }
            int publishCount = q.tryPublishInternal(currentBatch);
            if (publishCount == 0) {
                q.metrics.notifyInsertFailure();
                return false;
            } else {
                currentBatch.subList(0, publishCount).clear();
                return true;
            }
        }
    } // class BatchInserter

    /**
     * This inner class provides methods to access the metrics of the JCQueue.
     */
    public class QueueMetrics implements Closeable {
        private final RateTracker arrivalsTracker = new RateTracker(10000, 10);
        private final RateTracker insertFailuresTracker = new RateTracker(10000, 10);
        private final AtomicLong droppedMessages = new AtomicLong(0);

        public long population() {
            return recvQueue.size();
        }

        public long capacity() {
            return recvQueue.capacity();
        }

        public Object getState() {
            Map<String, Object> state = new HashMap<>();

            final double arrivalRateInSecs = arrivalsTracker.reportRate();

            long tuplePop = population();

            // Assume the recvQueue is stable, in which the arrival rate is equal to the consumption rate.
            // If this assumption does not hold, the calculation of sojourn time should also consider
            // departure rate according to Queuing Theory.
            final double sojournTime = tuplePop / Math.max(arrivalRateInSecs, 0.00001) * 1000.0;

            long cap = capacity();
            float pctFull = (1.0F * tuplePop / cap);

            state.put("capacity", cap);
            state.put("pct_full", pctFull);
            state.put("population", tuplePop);

            state.put("arrival_rate_secs", arrivalRateInSecs);
            state.put("sojourn_time_ms", sojournTime); //element sojourn time in milliseconds
            state.put("insert_failures", insertFailuresTracker.reportRate());
            state.put("dropped_messages", droppedMessages);
            state.put("overflow", overflowQ.size());
            return state;
        }

        public void notifyArrivals(long counts) {
            arrivalsTracker.notify(counts);
        }

        public void notifyInsertFailure() {
            insertFailuresTracker.notify(1);
        }

        public void notifyDroppedMsg() {
            droppedMessages.incrementAndGet();
        }

        @Override
        public void close() {
            arrivalsTracker.close();
            insertFailuresTracker.close();
        }

    }
}