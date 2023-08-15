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

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
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
