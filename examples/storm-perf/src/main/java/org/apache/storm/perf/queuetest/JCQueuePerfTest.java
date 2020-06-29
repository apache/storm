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

package org.apache.storm.perf.queuetest;

import java.util.Collections;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.policy.WaitStrategyPark;
import org.apache.storm.utils.JCQueue;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JCQueuePerfTest {
    // Usage: Let it and then explicitly terminate.
    // Metrics will be printed when application is terminated.
    public static void main(String[] args) throws Exception {
        //        oneProducer1Consumer(1000);  // -- measurement 1
        //        twoProducer1Consumer(1000);    // -- measurement 2
        //        threeProducer1Consumer(1);   // -- measurement 3

        //        oneProducer2Consumers();     // -- measurement 4

        //        producerFwdConsumer();      // -- measurement 5

        //        ackingProducerSimulation(); // -- measurement 6

        while (true) {
            Thread.sleep(1000);
        }

    }

    private static void ackingProducerSimulation() {
        WaitStrategyPark ws = new WaitStrategyPark(100);
        StormMetricRegistry registry = new StormMetricRegistry();
        JCQueue spoutQ = new JCQueue("spoutQ", "spoutQ", 1024, 0, 100, ws, "test", "test", Collections.singletonList(1000), 1000, registry);
        JCQueue ackQ = new JCQueue("ackQ", "ackQ", 1024, 0, 100, ws, "test", "test", Collections.singletonList(1000), 1000, registry);

        final AckingProducer ackingProducer = new AckingProducer(spoutQ, ackQ);
        final Acker acker = new Acker(ackQ, spoutQ);

        runAllThds(ackingProducer, acker);
    }

    private static void producerFwdConsumer(int prodBatchSz) {
        WaitStrategyPark ws = new WaitStrategyPark(100);
        StormMetricRegistry registry = new StormMetricRegistry();
        JCQueue q1 = new JCQueue("q1", "q1", 1024, 0, prodBatchSz, ws, "test", "test",
                Collections.singletonList(1000), 1000, registry);
        JCQueue q2 = new JCQueue("q2", "q2", 1024, 0, prodBatchSz, ws, "test", "test", Collections.singletonList(1000), 1000, registry);

        final Producer prod = new Producer(q1);
        final Forwarder fwd = new Forwarder(q1, q2);
        final Consumer cons = new Consumer(q2);

        runAllThds(prod, fwd, cons);
    }


    private static void oneProducer1Consumer(int prodBatchSz) {
        JCQueue q1 = new JCQueue("q1", "q1", 50_000, 0, prodBatchSz, new WaitStrategyPark(100), "test", "test",
                Collections.singletonList(1000), 1000, new StormMetricRegistry());

        final Producer prod1 = new Producer(q1);
        final Consumer cons1 = new Consumer(q1);

        runAllThds(prod1, cons1);
    }

    private static void twoProducer1Consumer(int prodBatchSz) {
        JCQueue q1 = new JCQueue("q1", "q1", 50_000, 0, prodBatchSz, new WaitStrategyPark(100), "test", "test",
                Collections.singletonList(1000), 1000, new StormMetricRegistry());

        final Producer prod1 = new Producer(q1);
        final Producer prod2 = new Producer(q1);
        final Consumer cons1 = new Consumer(q1);

        runAllThds(prod1, prod2, cons1);
    }

    private static void threeProducer1Consumer(int prodBatchSz) {
        JCQueue q1 = new JCQueue("q1", "q1", 50_000, 0, prodBatchSz, new WaitStrategyPark(100), "test", "test",
                Collections.singletonList(1000), 1000, new StormMetricRegistry());

        final Producer prod1 = new Producer(q1);
        final Producer prod2 = new Producer(q1);
        final Producer prod3 = new Producer(q1);
        final Consumer cons1 = new Consumer(q1);

        runAllThds(prod1, prod2, prod3, cons1);
    }


    private static void oneProducer2Consumers(int prodBatchSz) {
        WaitStrategyPark ws = new WaitStrategyPark(100);
        StormMetricRegistry registry = new StormMetricRegistry();
        JCQueue q1 = new JCQueue("q1", "q1", 1024, 0, prodBatchSz, ws, "test", "test", Collections.singletonList(1000), 1000, registry);
        JCQueue q2 = new JCQueue("q2", "q2", 1024, 0, prodBatchSz, ws, "test", "test", Collections.singletonList(1000), 1000, registry);

        final Producer2 prod1 = new Producer2(q1, q2);
        final Consumer cons1 = new Consumer(q1);
        final Consumer cons2 = new Consumer(q2);

        runAllThds(prod1, cons1, cons2);
    }

    public static void runAllThds(MyThread... threads) {
        for (Thread thread : threads) {
            thread.start();
        }
        addShutdownHooks(threads);
    }

    public static void addShutdownHooks(MyThread... threads) {

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.err.println("Stopping");
                for (Thread thread : threads) {
                    thread.interrupt();
                }

                for (Thread thread : threads) {
                    System.err.println("Waiting for " + thread.getName());
                    thread.join();
                }

                for (MyThread thread : threads) {
                    System.err.printf("%s : %d,  Throughput: %,d \n", thread.getName(), thread.count, thread.throughput());
                }
            } catch (InterruptedException e) {
                return;
            }
        }));

    }
}
