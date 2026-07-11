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
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
    // dedicated lane for low-volume control tuples (flush/tick/metrics tick/feedback), drained before recvQueue.
    // Like recvQueue, this lane is not drained on close(): any tuples still buffered at shutdown are discarded.
    private final MpscArrayQueue<Object> controlQueue;
    private final int overflowLimit; // ensures... overflowCount <= overflowLimit. if set to 0, disables overflow limiting.
    private final int producerBatchSz;
    private final boolean dynamicBatch;
    private final DirectInserter directInserter = new DirectInserter(this);
    private final ThreadLocal<BatchInserter> thdLocalBatcher = new ThreadLocal<BatchInserter>(); // ensure 1 instance per producer thd.
    // ensure 1 instance per producer thd.
    private final ThreadLocal<DynamicBatchInserter> thdLocalDynamicBatcher = new ThreadLocal<DynamicBatchInserter>();
    private final IWaitStrategy backPressureWaitStrategy;
    private final String queueName;

    // Throttle for the control-lane-full WARN so a stalled executor cannot flood the logs; log at most once per interval.
    private static final long CONTROL_DROP_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);
    private final AtomicLong lastControlDropLogNanos = new AtomicLong(Long.MIN_VALUE);

    public JCQueue(String queueName, String metricNamePrefix, int size, int overflowLimit, int producerBatchSz,
                   IWaitStrategy backPressureWaitStrategy, String topologyId, String componentId, List<Integer> taskIds,
                   int port, StormMetricRegistry metricRegistry) {
        this(queueName, metricNamePrefix, size, overflowLimit, producerBatchSz, backPressureWaitStrategy, topologyId, componentId,
                taskIds, port, metricRegistry, false);
    }

    public JCQueue(String queueName, String metricNamePrefix, int size, int overflowLimit, int producerBatchSz,
                   IWaitStrategy backPressureWaitStrategy, String topologyId, String componentId, List<Integer> taskIds,
                   int port, StormMetricRegistry metricRegistry, boolean dynamicBatch) {
        this(queueName, metricNamePrefix, size, overflowLimit, producerBatchSz, backPressureWaitStrategy, topologyId, componentId,
                taskIds, port, metricRegistry, dynamicBatch, 0);
    }

    public JCQueue(String queueName, String metricNamePrefix, int size, int overflowLimit, int producerBatchSz,
                   IWaitStrategy backPressureWaitStrategy, String topologyId, String componentId, List<Integer> taskIds,
                   int port, StormMetricRegistry metricRegistry, boolean dynamicBatch, int controlQueueSize) {
        this.queueName = queueName;
        this.dynamicBatch = dynamicBatch;
        this.overflowLimit = overflowLimit;
        this.recvQueue = new MpscArrayQueue<>(size);
        this.overflowQ = new MpscUnboundedArrayQueue<>(size);
        this.controlQueue = (controlQueueSize > 0) ? new MpscArrayQueue<>(controlQueueSize) : null;

        for (Integer taskId : taskIds) {
            this.jcqMetrics.add(new JCQueueMetrics(metricNamePrefix, topologyId, componentId, taskId, port,
                    metricRegistry, recvQueue, overflowQ, controlQueue));
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
        int controlCount = (controlQueue == null) ? 0 : controlQueue.size();
        return controlCount + recvQueue.size() + overflowQ.size();
    }

    /**
     * Load of the data plane only. The control lane and overflow lane are deliberately excluded so that load-aware groupings
     * (e.g. LoadAwareShuffleGrouping) are unaffected by pending control tuples.
     */
    public double getQueueLoad() {
        return ((double) recvQueue.size()) / recvQueue.capacity();
    }

    /**
     * Non blocking. Returns immediately if Q is empty. Returns number of elements consumed from Q.
     */
    private int consumeImpl(Consumer consumer, ExitCondition exitCond) throws InterruptedException {
        int controlDrainCount = 0;
        if (controlQueue != null) {
            // drain the control lane first: it is small and bounded, so it cannot starve the data plane
            while (exitCond.keepRunning()) {
                Object tuple = controlQueue.poll();
                if (tuple == null) {
                    break;
                }
                consumer.accept(tuple);
                ++controlDrainCount;
            }
        }

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
        int total = controlDrainCount + drainCount + overflowDrainCount;
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
            if (dynamicBatch) {
                DynamicBatchInserter d = thdLocalDynamicBatcher.get();
                if (d == null) {
                    d = new DynamicBatchInserter(this, producerBatchSz);
                    thdLocalDynamicBatcher.set(d);
                }
                inserter = d;
            } else {
                BatchInserter b = thdLocalBatcher.get();
                if (b == null) {
                    b = new BatchInserter(this, producerBatchSz);
                    thdLocalBatcher.set(b);
                }
                inserter = b;
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
     * Whether this queue has a dedicated control lane for low-volume control tuples.
     */
    public boolean isControlLaneEnabled() {
        return controlQueue != null;
    }

    /**
     * Non-blocking, un-batched write to the control lane, exempt from backpressure. Falls back to an un-batched
     * write to the recvQueue (same as {@link #tryPublishDirect(Object)}) when the control lane is disabled.
     * Returns false if the target queue is full; a failed control publish is dropped by the caller and counted
     * via the control drop metric, which is safe because control signals are periodic and self-healing.
     */
    public boolean tryPublishControl(Object obj) {
        if (controlQueue == null) {
            return tryPublishInternal(obj);
        }
        if (controlQueue.offer(obj)) {
            return true;
        }
        for (JCQueueMetrics jcQueueMetric : jcqMetrics) {
            jcQueueMetric.notifyControlMsgDrop();
        }
        maybeWarnControlDrop();
        return false;
    }

    // A full control lane means the consuming executor thread is stalled; surface it in the logs (rate-limited) and
    // not only via the control_dropped_messages metric, which an operator may not be charting.
    private void maybeWarnControlDrop() {
        // Reads the clock on every invocation, but this is not a hot path: it runs only when a control tuple is
        // dropped, and control tuples reach tryPublishControl only via the isControlStreamId guard (timer-driven
        // tick/flush/metrics-tick/feedback signals). So drops are bounded by those signal periods (order of a few
        // per second per queue even under a full stall), not by the data rate. A time-based throttle also has to
        // read the clock to know the interval elapsed; on Linux nanoTime() is a vDSO read (no syscall/context switch).
        long now = System.nanoTime();
        long last = lastControlDropLogNanos.get();
        if (now - last >= CONTROL_DROP_LOG_INTERVAL_NANOS && lastControlDropLogNanos.compareAndSet(last, now)) {
            LOG.warn("Control lane full on queue '{}'; dropping control tuple. The consuming executor thread is likely "
                    + "stalled. See the receive-queue-control_dropped_messages metric.", queueName);
        }
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
        @Override
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
        // WeakReference breaks the ThreadLocal retention cycle: thdLocalBatcher is an instance field
        // of JCQueue, so the ThreadLocalMap key (the ThreadLocal object) is kept strongly reachable
        // via value(BatchInserter) -> queue(JCQueue) -> field. A WeakReference here cuts that path,
        // allowing the key to become weakly-reachable and the entry to be expunged once the JCQueue
        // is no longer externally referenced.
        private final WeakReference<JCQueue> queueRef;
        private final ArrayList<Object> currentBatch;

        BatchInserter(JCQueue queue, int batchSz) {
            this.queueRef = new WeakReference<>(queue);
            this.batchSz = batchSz;
            this.currentBatch = new ArrayList<>(batchSz + 1);
        }

        /**
         * Number of buffered elements that triggers a flush. Constant here; subclasses may vary it at runtime.
         */
        int batchSize() {
            return batchSz;
        }

        /**
         * Hook invoked after every non-empty flush, passing whether the batch had reached {@link #batchSize()} when the flush started.
         * No-op here; subclasses may use it to adapt {@link #batchSize()}.
         */
        void afterFlush(boolean wasFull) {
        }

        /**
         * Blocking call - retires till element is successfully added.
         */
        @Override
        public void publish(Object obj) throws InterruptedException {
            currentBatch.add(obj);
            if (currentBatch.size() >= batchSize()) {
                flush();
            }
        }

        /**
         * Non-Blocking call. return value indicates success/failure
         */
        @Override
        public boolean tryPublish(Object obj) {
            if (currentBatch.size() >= batchSize()) {
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
            JCQueue queue = queueRef.get();
            if (queue == null) {
                // The JCQueue was GC'd (topology stopped on a long-lived thread, e.g. LocalCluster).
                // Nothing to flush; discard the buffered batch and return cleanly.
                currentBatch.clear();
                return;
            }
            boolean wasFull = currentBatch.size() >= batchSize();
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
            afterFlush(wasFull);
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
            JCQueue queue = queueRef.get();
            if (queue == null) {
                // The JCQueue was GC'd (topology stopped on a long-lived thread, e.g. LocalCluster).
                // Nothing to flush; discard the buffered batch and report success.
                currentBatch.clear();
                return true;
            }
            boolean wasFull = currentBatch.size() >= batchSize();
            int publishCount = queue.tryPublishInternal(currentBatch);
            if (publishCount == 0) {
                for (JCQueueMetrics jcQueueMetric : queue.jcqMetrics) {
                    jcQueueMetric.notifyInsertFailure();
                }
                // afterFlush is invoked intentionally even though nothing was published: a full batch that the recvQueue
                // could not accept is a heavy-load signal, so subclasses grow the effective batch size (see
                // DynamicBatchInserter#afterFlush). This matches the blocking flush(), which also grows on wasFull after its
                // retry loop drains the queue. Do not move this out of the failure branch without revisiting that symmetry.
                afterFlush(wasFull);
                return false;
            } else {
                currentBatch.subList(0, publishCount).clear();
                afterFlush(wasFull);
                return true;
            }
        }
    } // class BatchInserter

    /**
     * A {@link BatchInserter} that adapts its batch size between 1 and a configured maximum using AIMD, to favor low latency under
     * light load while preserving throughput under heavy load. It reuses the parent's publish/flush logic and only customizes the
     * flush threshold ({@link #batchSize()}) and the post-flush adaptation ({@link #afterFlush(boolean)}): a flush of a full batch
     * is read as heavy load and additively grows the effective size; a flush of a partially-filled batch (e.g. driven by the
     * flush-tuple timer) is read as light load and multiplicatively shrinks it toward 1. Not thread safe. Have one instance per
     * producer thread or synchronize externally.
     */
    static class DynamicBatchInserter extends BatchInserter {
        private final int maxBatchSz;
        private int effectiveBatchSz;

        DynamicBatchInserter(JCQueue queue, int maxBatchSz) {
            super(queue, maxBatchSz); // sizes the buffer to the max; the flush threshold comes from batchSize()
            this.maxBatchSz = maxBatchSz;
            this.effectiveBatchSz = 1; // start small to favor latency; grows under sustained load
        }

        @Override
        int batchSize() {
            return effectiveBatchSz;
        }

        @Override
        void afterFlush(boolean wasFull) {
            if (wasFull) {
                effectiveBatchSz = Math.min(maxBatchSz, effectiveBatchSz + 1); // additive increase
            } else {
                effectiveBatchSz = Math.max(1, effectiveBatchSz >> 1); // multiplicative decrease
            }
        }
    }
}