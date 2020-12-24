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
import java.util.List;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.policy.IWaitStrategy;
import org.apache.storm.shade.org.jctools.queues.MessagePassingQueue;
import org.apache.storm.shade.org.jctools.queues.MpscArrayQueue;
import org.apache.storm.shade.org.jctools.queues.MpscUnboundedArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JCQueue implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(JCQueue.class);
    private final ExitCondition continueRunning = () -> true;
    private final List<JCQueueMetrics> jcqMetrics = new ArrayList<>();
    private final MpscArrayQueue<Object> recvQueue;
    // only holds msgs from other workers (via WorkerTransfer), when recvQueue is full
    private final MpscUnboundedArrayQueue<Object> overflowQ;
    private final int overflowLimit; // ensures... overflowCount <= overflowLimit. if set to 0, disables overflow limiting.
    private final int producerBatchSz;
    private final DirectInserter directInserter = new DirectInserter(this);
    private final ThreadLocal<BatchInserter> thdLocalBatcher = new ThreadLocal<BatchInserter>(); // ensure 1 instance per producer thd.
    private final IWaitStrategy backPressureWaitStrategy;
    private final String queueName;

    public JCQueue(String queueName, String metricNamePrefix, int size, int overflowLimit, int producerBatchSz,
                   IWaitStrategy backPressureWaitStrategy, String topologyId, String componentId, List<Integer> taskIds,
                   int port, StormMetricRegistry metricRegistry) {
        this.queueName = queueName;
        this.overflowLimit = overflowLimit;
        this.recvQueue = new MpscArrayQueue<>(size);
        this.overflowQ = new MpscUnboundedArrayQueue<>(size);

        for (Integer taskId : taskIds) {
            this.jcqMetrics.add(new JCQueueMetrics(metricNamePrefix, topologyId, componentId, taskId, port,
                    metricRegistry, recvQueue, overflowQ));
        }

        //The batch size can be no larger than half the full recvQueue size, to avoid contention issues.
        this.producerBatchSz = Math.max(1, Math.min(producerBatchSz, size / 2));
        this.backPressureWaitStrategy = backPressureWaitStrategy;
    }

    public String getQueueName() {
        return queueName;
    }

    @Override
    public void close() {
        for (JCQueueMetrics jcQueueMetric : jcqMetrics) {
            jcQueueMetric.close();
        }
    }

    /**
     * Non blocking. Returns immediately if Q is empty. Returns number of elements consumed from Q.
     */
    public int consume(JCQueue.Consumer consumer) {
        return consume(consumer, continueRunning);
    }

    /**
     * Non blocking. Returns immediately if Q is empty. Runs till Q is empty OR exitCond.keepRunning() return false. Returns number of
     * elements consumed from Q.
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

    public double getQueueLoad() {
        return ((double) recvQueue.size()) / recvQueue.capacity();
    }

    /**
     * Non blocking. Returns immediately if Q is empty. Returns number of elements consumed from Q.
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
            for (JCQueueMetrics jcQueueMetric : jcqMetrics) {
                jcQueueMetric.notifyArrivals(1);
            }
            return true;
        }
        return false;
    }

    // Non Blocking. returns count of how many inserts succeeded
    private int tryPublishInternal(ArrayList<Object> objs) {
        MessagePassingQueue.Supplier<Object> supplier =
            new MessagePassingQueue.Supplier<Object>() {
                int counter = 0;

                @Override
                public Object get() {
                    return objs.get(counter++);
                }
            };
        int count = recvQueue.fill(supplier, objs.size());
        for (JCQueueMetrics jcQueueMetric : jcqMetrics) {
            jcQueueMetric.notifyArrivals(count);
        }
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
        for (JCQueueMetrics jcQueueMetric : jcqMetrics) {
            jcQueueMetric.notifyDroppedMsg();
        }
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
        private JCQueue queue;

        DirectInserter(JCQueue queue) {
            this.queue = queue;
        }

        /**
         * Blocking call, that can be interrupted via Thread.interrupt
         */
        @Override
        public void publish(Object obj) throws InterruptedException {
            boolean inserted = queue.tryPublishInternal(obj);
            int idleCount = 0;
            while (!inserted) {
                for (JCQueueMetrics jcQueueMetric : queue.jcqMetrics) {
                    jcQueueMetric.notifyInsertFailure();
                }
                if (idleCount == 0) { // check avoids multiple log msgs when in a idle loop
                    LOG.debug("Experiencing Back Pressure on recvQueue: '{}'. Entering BackPressure Wait",
                        queue.getQueueName());
                }

                idleCount = queue.backPressureWaitStrategy.idle(idleCount);
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                inserted = queue.tryPublishInternal(obj);
            }

        }

        /**
         * Non-Blocking call. return value indicates success/failure
         */
        @Override
        public boolean tryPublish(Object obj) {
            boolean inserted = queue.tryPublishInternal(obj);
            if (!inserted) {
                for (JCQueueMetrics jcQueueMetric : queue.jcqMetrics) {
                    jcQueueMetric.notifyInsertFailure();
                }
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
        private JCQueue queue;
        private ArrayList<Object> currentBatch;

        BatchInserter(JCQueue queue, int batchSz) {
            this.queue = queue;
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
            int publishCount = queue.tryPublishInternal(currentBatch);
            int retryCount = 0;
            while (publishCount == 0) { // retry till at least 1 element is drained
                for (JCQueueMetrics jcQueueMetric : queue.jcqMetrics) {
                    jcQueueMetric.notifyInsertFailure();
                }
                if (retryCount == 0) { // check avoids multiple log msgs when in a idle loop
                    LOG.debug("Experiencing Back Pressure when flushing batch to Q: '{}'. Entering BackPressure Wait.",
                        queue.getQueueName());
                }
                retryCount = queue.backPressureWaitStrategy.idle(retryCount);
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                publishCount = queue.tryPublishInternal(currentBatch);
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
            int publishCount = queue.tryPublishInternal(currentBatch);
            if (publishCount == 0) {
                for (JCQueueMetrics jcQueueMetric : queue.jcqMetrics) {
                    jcQueueMetric.notifyInsertFailure();
                }
                return false;
            } else {
                currentBatch.subList(0, publishCount).clear();
                return true;
            }
        }
    } // class BatchInserter
}