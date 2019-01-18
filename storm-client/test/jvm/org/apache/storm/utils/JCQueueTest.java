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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.policy.IWaitStrategy;
import org.apache.storm.policy.WaitStrategyPark;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class JCQueueTest {

    private final static int TIMEOUT = 5000; // MS
    private final static int PRODUCER_NUM = 4;
    IWaitStrategy waitStrategy = new WaitStrategyPark(100);

    @Test(timeout = 10000)
    public void testFirstMessageFirst() throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            JCQueue queue = createQueue("firstMessageOrder", 16);

            queue.publish("FIRST");

            Runnable producer = new IncProducer(queue, i + 100);

            final AtomicReference<Object> result = new AtomicReference<>();
            Runnable consumer = new ConsumerThd(queue, new JCQueue.Consumer() {
                private boolean head = true;

                @Override
                public void accept(Object event) {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new RuntimeException(new InterruptedException("ConsumerThd interrupted"));
                    }
                    if (head) {
                        head = false;
                        result.set(event);
                    }
                }

                @Override
                public void flush() {
                    return;
                }
            });

            run(producer, consumer, queue);
            Assert.assertEquals("We expect to receive first published message first, but received " + result.get(),
                                "FIRST", result.get());
        }
    }

    @Test(timeout = 10000)
    public void testInOrder() throws InterruptedException {
        final AtomicBoolean allInOrder = new AtomicBoolean(true);

        JCQueue queue = createQueue("consumerHang", 1024);
        Runnable producer = new IncProducer(queue, 1024 * 1024);
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
                return;
            }
        });
        run(producer, consumer, queue, 1000, 1);
        Assert.assertTrue("Messages delivered out of order",
                          allInOrder.get());
    }

    @Test(timeout = 10000)
    public void testInOrderBatch() throws InterruptedException {
        final AtomicBoolean allInOrder = new AtomicBoolean(true);

        JCQueue queue = createQueue("consumerHang", 10, 1024);
        Runnable producer = new IncProducer(queue, 1024 * 1024);
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
                return;
            }
        });

        run(producer, consumer, queue, 1000, 1);
        Assert.assertTrue("Messages delivered out of order",
                          allInOrder.get());
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
            assertFalse("producer " + i + " is still alive", producerThreads[i].isAlive());
        }

        queue.close();
        consumerThread.interrupt();
        consumerThread.join(TIMEOUT);
        assertFalse("consumer is still alive", consumerThread.isAlive());
    }

    private JCQueue createQueue(String name, int queueSize) {
        return createQueue(name, 1, queueSize);
    }

    private JCQueue createQueue(String name, int batchSize, int queueSize) {
        return new JCQueue(name, queueSize, 0, batchSize, waitStrategy, "test", "test", 1000, 1000);
    }

    private static class IncProducer implements Runnable {
        private JCQueue queue;
        private long _max;

        IncProducer(JCQueue queue, long max) {
            this.queue = queue;
            this._max = max;
        }

        @Override
        public void run() {
            try {
                for (long i = 0; i < _max && !(Thread.currentThread().isInterrupted()); i++) {
                    queue.publish(i);
                }
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private static class ConsumerThd implements Runnable {
        private JCQueue.Consumer handler;
        private JCQueue queue;

        ConsumerThd(JCQueue queue, JCQueue.Consumer handler) {
            this.handler = handler;
            this.queue = queue;
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                queue.consume(handler);
            }
        }
    }
}
