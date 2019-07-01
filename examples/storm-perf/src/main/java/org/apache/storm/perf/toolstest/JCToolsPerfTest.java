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

package org.apache.storm.perf.toolstest;

import org.jctools.queues.MpscArrayQueue;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JCToolsPerfTest {
    public static void main(String[] args) throws Exception {
        //        oneProducer1Consumer();
        //        twoProducer1Consumer();
        //        threeProducer1Consumer();
        //        oneProducer2Consumers();
        //        producerFwdConsumer();

        //        JCQueue spoutQ = new JCQueue("spoutQ", 1024, 100, 0);
        //        JCQueue ackQ = new JCQueue("ackQ", 1024, 100, 0);
        //
        //        final AckingProducer ackingProducer = new AckingProducer(spoutQ, ackQ);
        //        final Acker acker = new Acker(ackQ, spoutQ);
        //
        //        runAllThds(ackingProducer, acker);

        while (true) {
            Thread.sleep(1000);
        }

    }

    private static void oneProducer1Consumer() {
        MpscArrayQueue<Object> q1 = new MpscArrayQueue<Object>(50_000);

        final Prod prod1 = new Prod(q1);
        final Cons cons1 = new Cons(q1);

        runAllThds(prod1, cons1);
    }

    private static void twoProducer1Consumer() {
        MpscArrayQueue<Object> q1 = new MpscArrayQueue<Object>(50_000);

        final Prod prod1 = new Prod(q1);
        final Prod prod2 = new Prod(q1);
        final Cons cons1 = new Cons(q1);

        runAllThds(prod1, cons1, prod2);
    }

    private static void threeProducer1Consumer() {
        MpscArrayQueue<Object> q1 = new MpscArrayQueue<Object>(50_000);

        final Prod prod1 = new Prod(q1);
        final Prod prod2 = new Prod(q1);
        final Prod prod3 = new Prod(q1);
        final Cons cons1 = new Cons(q1);

        runAllThds(prod1, prod2, prod3, cons1);
    }


    private static void oneProducer2Consumers() {
        MpscArrayQueue<Object> q1 = new MpscArrayQueue<Object>(50_000);
        MpscArrayQueue<Object> q2 = new MpscArrayQueue<Object>(50_000);

        final Prod2 prod1 = new Prod2(q1, q2);
        final Cons cons1 = new Cons(q1);
        final Cons cons2 = new Cons(q2);

        runAllThds(prod1, cons1, cons2);
    }

    public static void runAllThds(MyThd... threads) {
        for (Thread thread : threads) {
            thread.start();
        }
        addShutdownHooks(threads);
    }

    public static void addShutdownHooks(MyThd... threads) {

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.err.println("Stopping");
                for (MyThd thread : threads) {
                    thread.halt = true;
                }

                for (Thread thread : threads) {
                    System.err.println("Waiting for " + thread.getName());
                    thread.join();
                }

                for (MyThd thread : threads) {
                    System.err.printf("%s : %d,  Throughput: %,d \n", thread.getName(), thread.count, thread.throughput());
                }
            } catch (InterruptedException e) {
                return;
            }
        }));

    }

}



