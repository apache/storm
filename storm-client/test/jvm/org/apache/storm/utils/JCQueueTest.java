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
package org.apache.storm.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.codahale.metrics.Gauge;
import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.policy.IWaitStrategy;
import org.apache.storm.policy.WaitStrategyPark;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JCQueueTest {

    private final static int TIMEOUT = 5000; // MS
    private final static int PRODUCER_NUM = 4;
    IWaitStrategy waitStrategy = new WaitStrategyPark(100);

    @Test
    public void testFirstMessageFirst() {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            JCQueue queue = createQueue("firstMessageOrder", 16);

            queue.publish("FIRST");

            Runnable producer = new IncProducer(queue, 100, 1);

            final AtomicReference<Object> result = new AtomicReference<>();
            Runnable consumer = new ConsumerThd(queue, new JCQueue.Consumer() {
                private boolean head = true;

                @Override
                public void accept(Object event) {
                    if (head) {
                        head = false;
                        result.set(event);
                    }
                }

                @Override
                public void flush() {
                }
            });

            run(producer, consumer, queue);
            assertEquals("FIRST", result.get(),
                "We expect to receive first published message first, but received " + result.get());
        });
    }

    @Test
    public void testInOrder() {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            final AtomicBoolean allInOrder = new AtomicBoolean(true);

            JCQueue queue = createQueue("consumerHang", 1024);
            Runnable producer = new IncProducer(queue, 1024 * 1024, 100);
            Runnable consumer = new ConsumerThd(queue, new JCQueue.Consumer() {
                long _expected = 0;

                @Override
                public void accept(Object obj) {
                    if (_expected != ((Number) obj).longValue()) {
                        allInOrder.set(false);
                        System.out.println("Expected " + _expected + " but got " + obj);
                    }
                    _expected++;
                }

                @Override
                public void flush() {
                }
            });
            run(producer, consumer, queue, 1000, 1);
            assertTrue(allInOrder.get(), "Messages delivered out of order");
        });
    }

    @Test
    public void testInOrderBatch() {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            final AtomicBoolean allInOrder = new AtomicBoolean(true);

            JCQueue queue = createQueue("consumerHang", 10, 1024);
            Runnable producer = new IncProducer(queue, 1024 * 1024, 100);
            Runnable consumer = new ConsumerThd(queue, new JCQueue.Consumer() {
                long _expected = 0;

                @Override
                public void accept(Object obj) {
                    if (_expected != ((Number) obj).longValue()) {
                        allInOrder.set(false);
                        System.out.println("Expected " + _expected + " but got " + obj);
                    }
                    _expected++;
                }

                @Override
                public void flush() {
                }
            });

            run(producer, consumer, queue, 1000, 1);
            assertTrue(allInOrder.get(), "Messages delivered out of order");
        });
    }

    @Test
    public void testInOrderDynamicBatch() {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            final AtomicBoolean allInOrder = new AtomicBoolean(true);

            JCQueue queue = createQueue("dynamicBatch", 10, 1024, true);
            Runnable producer = new IncProducer(queue, 1024 * 1024, 100);
            Runnable consumer = new ConsumerThd(queue, new JCQueue.Consumer() {
                long _expected = 0;

                @Override
                public void accept(Object obj) {
                    if (_expected != ((Number) obj).longValue()) {
                        allInOrder.set(false);
                        System.out.println("Expected " + _expected + " but got " + obj);
                    }
                    _expected++;
                }

                @Override
                public void flush() {
                }
            });

            run(producer, consumer, queue, 1000, 1);
            assertTrue(allInOrder.get(), "Messages delivered out of order");
        });
    }

    @Test
    public void testDynamicBatchStartsAtOne() {
        JCQueue queue = createQueue("dynStart", 1024);
        JCQueue.DynamicBatchInserter inserter = new JCQueue.DynamicBatchInserter(queue, 8);
        assertEquals(1, inserter.batchSize());
    }

    @Test
    public void testDynamicBatchGrowsOnFullFlush() throws Exception {
        JCQueue queue = createQueue("dynGrow", 1024);
        JCQueue.DynamicBatchInserter inserter = new JCQueue.DynamicBatchInserter(queue, 8);

        inserter.publish(1L);                       // batch hits effective(1) -> full flush -> grow to 2
        assertEquals(2, inserter.batchSize());

        inserter.publish(2L);                       // batch size 1 < 2, no flush
        inserter.publish(3L);                       // batch size 2 == 2 -> full flush -> grow to 3
        assertEquals(3, inserter.batchSize());
    }

    @Test
    public void testDynamicBatchCapsAtMax() throws Exception {
        JCQueue queue = createQueue("dynCap", 1024);
        JCQueue.DynamicBatchInserter inserter = new JCQueue.DynamicBatchInserter(queue, 4);
        for (int i = 0; i < 40; i++) {
            inserter.publish((long) i);
            assertTrue(inserter.batchSize() <= 4, "effective batch exceeded max");
        }
        assertEquals(4, inserter.batchSize());
    }

    @Test
    public void testDynamicBatchShrinksOnPartialFlush() throws Exception {
        JCQueue queue = createQueue("dynShrink", 1024);
        JCQueue.DynamicBatchInserter inserter = new JCQueue.DynamicBatchInserter(queue, 8);
        growEffectiveTo(inserter, 4);

        inserter.publish(100L);                     // partial batch (size 1 < 4)
        inserter.flush();                           // timer-style flush of a partial batch -> shrink (4 -> 2)
        assertEquals(2, inserter.batchSize());
    }

    @Test
    public void testDynamicBatchEmptyFlushIsNoOp() throws Exception {
        JCQueue queue = createQueue("dynEmpty", 1024);
        JCQueue.DynamicBatchInserter inserter = new JCQueue.DynamicBatchInserter(queue, 8);
        growEffectiveTo(inserter, 4);               // leaves the batch empty after the growing flush

        inserter.flush();                           // empty batch -> no adaptation
        assertEquals(4, inserter.batchSize());
    }

    @Test
    public void testDynamicBatchNeverDropsBelowOne() throws Exception {
        JCQueue queue = createQueue("dynFloor", 1024);
        JCQueue.DynamicBatchInserter inserter = new JCQueue.DynamicBatchInserter(queue, 8);
        growEffectiveTo(inserter, 2);

        inserter.publish(1L);                       // partial (size 1 < 2)
        inserter.flush();                           // shrink 2 -> 1
        assertEquals(1, inserter.batchSize());
    }

    @Test
    public void testDynamicBatchGrowsNotShrinksUnderBackpressure() throws Exception {
        JCQueue queue = createQueue("dynBp", 8);
        JCQueue.DynamicBatchInserter inserter = new JCQueue.DynamicBatchInserter(queue, 4);

        inserter.publish(1L);                       // eff 1 -> 2, recvQueue now has 1 element
        assertEquals(2, inserter.batchSize());

        while (queue.tryPublishDirect(99L)) {       // fill recvQueue to capacity
        }

        assertTrue(inserter.tryPublish(2L));        // batch size 1
        assertTrue(inserter.tryPublish(3L));        // batch size 2 (== effective)
        assertFalse(inserter.tryPublish(4L));       // full batch, queue full -> tryFlush fails
        // A full batch that could not be published must be read as heavy load (grow), not light load (shrink).
        assertEquals(3, inserter.batchSize());
    }

    @Test
    public void testControlTuplesDrainedBeforeData() {
        JCQueue queue = createQueueWithControlLane("controlFirst", 16, 4);
        assertTrue(queue.isControlLaneEnabled());

        for (int i = 0; i < 8; i++) {
            assertTrue(queue.tryPublishDirect(i));
        }
        // published AFTER the data backlog, must still be consumed first
        assertTrue(queue.tryPublishControl("CTRL"));

        List<Object> drained = new ArrayList<>();
        queue.consume(collectingConsumer(drained));

        assertEquals(9, drained.size());
        assertEquals("CTRL", drained.get(0), "control tuple must be drained ahead of the data backlog");
    }

    @Test
    public void testControlLaneDropsOnFullWithoutBlocking() {
        JCQueue queue = createQueueWithControlLane("controlDrop", 16, 2);

        int accepted = 0;
        while (accepted < 64 && queue.tryPublishControl("CTRL" + accepted)) {
            accepted++;
        }
        assertTrue(accepted > 0 && accepted < 64, "control lane must be bounded");

        // a full control lane drops (returns false) instead of blocking, and leaves the data path unaffected
        assertFalse(queue.tryPublishControl("EXTRA"));
        assertTrue(queue.tryPublishDirect("DATA"));
        assertEquals(accepted + 1, queue.size());
    }

    @Test
    public void testControlPublishFallsBackToRecvQueueWhenLaneDisabled() {
        JCQueue queue = createQueue("controlDisabled", 16);
        assertFalse(queue.isControlLaneEnabled());

        assertTrue(queue.tryPublishControl("CTRL"));
        assertEquals(1, queue.size());

        List<Object> drained = new ArrayList<>();
        queue.consume(collectingConsumer(drained));
        assertEquals(Collections.singletonList("CTRL"), drained);
    }

    @Test
    public void testControlTuplesDrainedBeforeOverflow() {
        JCQueue queue = createQueueWithControlLane("controlBeforeOverflow", 16, 4);

        assertTrue(queue.tryPublishToOverflow("OVERFLOW"));
        assertTrue(queue.tryPublishDirect("DATA"));
        assertTrue(queue.tryPublishControl("CTRL"));

        List<Object> drained = new ArrayList<>();
        int consumed = queue.consume(collectingConsumer(drained));

        assertEquals(3, consumed, "consume() return value must include control tuples");
        assertEquals(Arrays.asList("CTRL", "DATA", "OVERFLOW"), drained,
            "drain order must be control lane, then recvQueue, then overflowQ");
    }

    @Test
    public void testControlLaneIsFifo() {
        JCQueue queue = createQueueWithControlLane("controlFifo", 16, 8);
        for (int i = 0; i < 5; i++) {
            assertTrue(queue.tryPublishControl("CTRL" + i));
        }

        List<Object> drained = new ArrayList<>();
        queue.consume(collectingConsumer(drained));
        assertEquals(Arrays.asList("CTRL0", "CTRL1", "CTRL2", "CTRL3", "CTRL4"), drained);
    }

    @Test
    public void testConsumerFlushCalledAfterControlOnlyDrain() {
        JCQueue queue = createQueueWithControlLane("controlFlush", 16, 4);
        assertTrue(queue.tryPublishControl("CTRL"));

        AtomicInteger flushCount = new AtomicInteger();
        int consumed = queue.consume(new JCQueue.Consumer() {
            @Override
            public void accept(Object event) {
            }

            @Override
            public void flush() {
                flushCount.incrementAndGet();
            }
        });

        assertEquals(1, consumed);
        assertEquals(1, flushCount.get(),
            "a drain that consumed only control tuples must still flush the consumer (e.g. deliver a flush tuple's effect)");
    }

    @Test
    public void testExitConditionStopsControlDrain() {
        JCQueue queue = createQueueWithControlLane("controlExitCond", 16, 4);
        assertTrue(queue.tryPublishControl("CTRL"));

        List<Object> drained = new ArrayList<>();
        int consumed = queue.consume(collectingConsumer(drained), () -> false);

        assertEquals(0, consumed, "exit condition must be honored before draining the control lane");
        assertTrue(drained.isEmpty());
        assertEquals(1, queue.size(), "unconsumed control tuple must remain queued");
    }

    @Test
    public void testControlDropIsCountedInMetrics() {
        StormMetricRegistry registry = new StormMetricRegistry();
        JCQueue queue = new JCQueue("controlDropMetric", "controlDropMetric", 16, 0, 1, waitStrategy,
            "test", "test", Collections.singletonList(1000), 1000, registry, false, 2);

        int accepted = 0;
        while (accepted < 64 && queue.tryPublishControl("CTRL")) {
            accepted++;
        }
        assertTrue(accepted > 0 && accepted < 64);
        assertFalse(queue.tryPublishControl("EXTRA")); // second counted drop (first happened when the fill loop stopped)

        assertEquals((long) accepted, gaugeValue(registry, "control_population"),
            "control_population gauge must report the lane occupancy");
        assertEquals(2L, gaugeValue(registry, "control_dropped_messages"),
            "every rejected control publish must be counted as a dropped control message");
    }

    private long gaugeValue(StormMetricRegistry registry, String nameSuffix) {
        for (Map.Entry<String, Gauge> entry : registry.getRegistry().getGauges().entrySet()) {
            if (entry.getKey().contains(nameSuffix)) {
                return ((Number) entry.getValue().getValue()).longValue();
            }
        }
        throw new AssertionError("gauge not registered: " + nameSuffix);
    }

    @Test
    public void testQueueLoadExcludesControlLane() {
        JCQueue queue = createQueueWithControlLane("controlLoad", 16, 4);

        assertTrue(queue.tryPublishControl("CTRL"));

        assertEquals(0.0, queue.getQueueLoad(), 0.0, "control lane must not contribute to the data-plane load");
        assertEquals(1, queue.size(), "control lane must contribute to size()");
    }

    @Test
    public void testQueueIsCollectedAfterLongLivedProducerPublishes() {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
            ExecutorService producerPool = Executors.newSingleThreadExecutor();
            try {
                WeakReference<JCQueue> ref = publishFromLongLivedThread(producerPool);
                assertTrue(awaitGarbageCollection(ref),
                    "JCQueue was not garbage collected — BatchInserter may still hold a strong reference to it");
            } finally {
                producerPool.shutdownNow();
            }
        });
    }

    // Publishes a few tuples from a pooled thread so that the BatchInserter's ThreadLocal entry is
    // created on that thread. Once .get() returns the lambda is done and its closure ref is gone;
    // when the method returns the local `queue` variable goes out of scope. The only remaining
    // reference is the WeakReference — if it is not cleared after GC, the ThreadLocal cycle is still live.
    private WeakReference<JCQueue> publishFromLongLivedThread(ExecutorService pool) throws Exception {
        JCQueue queue = createQueue("leak", 100, 1024);
        pool.submit(() -> {
            try {
                for (long i = 0; i < 10; i++) {
                    queue.publish(i);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).get();
        return new WeakReference<>(queue);
    }

    private boolean awaitGarbageCollection(WeakReference<JCQueue> ref) throws InterruptedException {
        for (int i = 0; i < 50 && ref.get() != null; i++) {
            System.gc();
            Thread.sleep(100);
        }
        return ref.get() == null;
    }

    /** Drive the inserter with full flushes until the effective batch size reaches the target. */
    private void growEffectiveTo(JCQueue.DynamicBatchInserter inserter, int target) throws InterruptedException {
        long val = 0;
        while (inserter.batchSize() < target) {
            inserter.publish(val++);
        }
    }

    private void run(Runnable producer, Runnable consumer, JCQueue queue)
        throws InterruptedException {
        run(producer, consumer, queue, 20, PRODUCER_NUM);
    }

    private void run(Runnable producer, Runnable consumer, JCQueue queue, int sleepMs, int producerNum)
        throws InterruptedException {

        Thread[] producerThreads = new Thread[producerNum];
        for (int i = 0; i < producerNum; i++) {
            producerThreads[i] = new Thread(producer);
            producerThreads[i].start();
        }

        Thread consumerThread = new Thread(consumer);
        consumerThread.start();
        Thread.sleep(sleepMs);
        for (int i = 0; i < producerNum; i++) {
            producerThreads[i].interrupt();
        }
        for (int i = 0; i < producerNum; i++) {
            producerThreads[i].join(TIMEOUT);
            assertFalse(producerThreads[i].isAlive(), "producer " + i + " is still alive");
        }

        queue.close();
        consumerThread.interrupt();
        consumerThread.join(TIMEOUT);
        assertFalse(consumerThread.isAlive(), "consumer is still alive");
    }

    private JCQueue createQueue(String name, int queueSize) {
        return createQueue(name, 1, queueSize);
    }

    private JCQueue createQueue(String name, int batchSize, int queueSize) {
        return new JCQueue(name, name, queueSize, 0, batchSize, waitStrategy, "test", "test", Collections.singletonList(1000), 1000, new StormMetricRegistry());
    }

    private JCQueue createQueue(String name, int batchSize, int queueSize, boolean dynamicBatch) {
        return new JCQueue(name, name, queueSize, 0, batchSize, waitStrategy, "test", "test", Collections.singletonList(1000), 1000,
                new StormMetricRegistry(), dynamicBatch);
    }

    private JCQueue createQueueWithControlLane(String name, int queueSize, int controlQueueSize) {
        return new JCQueue(name, name, queueSize, 0, 1, waitStrategy, "test", "test", Collections.singletonList(1000), 1000,
                new StormMetricRegistry(), false, controlQueueSize);
    }

    private static JCQueue.Consumer collectingConsumer(List<Object> sink) {
        return new JCQueue.Consumer() {
            @Override
            public void accept(Object event) {
                sink.add(event);
            }

            @Override
            public void flush() {
            }
        };
    }

    private static class IncProducer implements Runnable {

        private final JCQueue queue;
        private final long _max;
        private final long min;

        public IncProducer(JCQueue queue, long _max, long min) {
            this.queue = queue;
            this._max = _max;
            this.min = min;
        }

        @Override
        public void run() {
            try {
                for (long i = 0; i < _max && (!Thread.currentThread().isInterrupted() || i < min); i++) {
                    queue.publish(i);
                }
            } catch (InterruptedException e) {
                //Just quit
            }
        }
    }

    private static class ConsumerThd implements Runnable {

        private final JCQueue.Consumer handler;
        private final JCQueue queue;

        ConsumerThd(JCQueue queue, JCQueue.Consumer handler) {
            this.handler = handler;
            this.queue = queue;
        }

        @Override
        public void run() {
            //The producers are shut down first, so keep going until the queue is empty.
            while (!Thread.currentThread().isInterrupted() || queue.size() != 0) {
                queue.consume(handler);
            }
        }
    }
}
