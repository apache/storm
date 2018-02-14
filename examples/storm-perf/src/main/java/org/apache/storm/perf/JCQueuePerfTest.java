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

package org.apache.storm.perf;

import java.util.concurrent.locks.LockSupport;

import org.apache.storm.policy.WaitStrategyPark;
import org.apache.storm.utils.JCQueue;
import org.apache.storm.utils.MutableLong;

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
        JCQueue spoutQ = new JCQueue("spoutQ", 1024, 0, 100, ws);
        JCQueue ackQ = new JCQueue("ackQ", 1024, 0, 100, ws);

        final AckingProducer ackingProducer = new AckingProducer(spoutQ, ackQ);
        final Acker acker = new Acker(ackQ, spoutQ);

        runAllThds(ackingProducer, acker);
    }

    private static void producerFwdConsumer(int prodBatchSz) {
        WaitStrategyPark ws = new WaitStrategyPark(100);
        JCQueue q1 = new JCQueue("q1", 1024, 0, prodBatchSz, ws);
        JCQueue q2 = new JCQueue("q2", 1024, 0, prodBatchSz, ws);

        final Producer prod = new Producer(q1);
        final Forwarder fwd = new Forwarder(q1, q2);
        final Consumer cons = new Consumer(q2);

        runAllThds(prod, fwd, cons);
    }


    private static void oneProducer1Consumer(int prodBatchSz) {
        JCQueue q1 = new JCQueue("q1", 50_000, 0, prodBatchSz, new WaitStrategyPark(100));

        final Producer prod1 = new Producer(q1);
        final Consumer cons1 = new Consumer(q1);

        runAllThds(prod1, cons1);
    }

    private static void twoProducer1Consumer(int prodBatchSz) {
        JCQueue q1 = new JCQueue("q1", 50_000, 0, prodBatchSz, new WaitStrategyPark(100));

        final Producer prod1 = new Producer(q1);
        final Producer prod2 = new Producer(q1);
        final Consumer cons1 = new Consumer(q1);

        runAllThds(prod1, prod2, cons1);
    }

    private static void threeProducer1Consumer(int prodBatchSz) {
        JCQueue q1 = new JCQueue("q1", 50_000, 0, prodBatchSz, new WaitStrategyPark(100));

        final Producer prod1 = new Producer(q1);
        final Producer prod2 = new Producer(q1);
        final Producer prod3 = new Producer(q1);
        final Consumer cons1 = new Consumer(q1);

        runAllThds(prod1, prod2, prod3, cons1);
    }


    private static void oneProducer2Consumers(int prodBatchSz) {
        WaitStrategyPark ws = new WaitStrategyPark(100);
        JCQueue q1 = new JCQueue("q1", 1024, 0, prodBatchSz, ws);
        JCQueue q2 = new JCQueue("q2", 1024, 0, prodBatchSz, ws);

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


abstract class MyThread extends Thread {
    public long count = 0;
    public long runTime = 0;

    public MyThread(String thdName) {
        super(thdName);
    }

    public long throughput() {
        return getCount() / (runTime / 1000);
    }

    public long getCount() {
        return count;
    }
}

class Producer extends MyThread {
    private final JCQueue q;

    public Producer(JCQueue q) {
        super("Producer");
        this.q = q;
    }

    @Override
    public void run() {
        try {
            long start = System.currentTimeMillis();
            while (!Thread.interrupted()) {
                q.publish(++count);
            }
            runTime = System.currentTimeMillis() - start;
        } catch (InterruptedException e) {
            return;
        }
    }

}

// writes to two queues
class Producer2 extends MyThread {
    private final JCQueue q1;
    private final JCQueue q2;

    public Producer2(JCQueue q1, JCQueue q2) {
        super("Producer2");
        this.q1 = q1;
        this.q2 = q2;
    }

    @Override
    public void run() {
        try {
            long start = System.currentTimeMillis();
            while (!Thread.interrupted()) {
                q1.publish(++count);
                q2.publish(count);
            }
            runTime = System.currentTimeMillis() - start;
        } catch (InterruptedException e) {
            return;
        }

    }
}


// writes to two queues
class AckingProducer extends MyThread {
    private final JCQueue ackerInQ;
    private final JCQueue spoutInQ;

    public AckingProducer(JCQueue ackerInQ, JCQueue spoutInQ) {
        super("AckingProducer");
        this.ackerInQ = ackerInQ;
        this.spoutInQ = spoutInQ;
    }

    @Override
    public void run() {
        try {
            Handler handler = new Handler();
            long start = System.currentTimeMillis();
            while (!Thread.interrupted()) {
                int x = spoutInQ.consume(handler);
                ackerInQ.publish(count);
            }
            runTime = System.currentTimeMillis() - start;
        } catch (InterruptedException e) {
            return;
        }
    }

    private class Handler implements JCQueue.Consumer {
        @Override
        public void accept(Object event) {
            // no-op
        }

        @Override
        public void flush() {
            // no-op
        }
    }
}

// reads from ackerInQ and writes to spout queue
class Acker extends MyThread {
    private final JCQueue ackerInQ;
    private final JCQueue spoutInQ;

    public Acker(JCQueue ackerInQ, JCQueue spoutInQ) {
        super("Acker");
        this.ackerInQ = ackerInQ;
        this.spoutInQ = spoutInQ;
    }


    @Override
    public void run() {
        long start = System.currentTimeMillis();
        Handler handler = new Handler();
        while (!Thread.interrupted()) {
            ackerInQ.consume(handler);
        }
        runTime = System.currentTimeMillis() - start;
    }

    private class Handler implements JCQueue.Consumer {
        @Override
        public void accept(Object event) {
            try {
                spoutInQ.publish(event);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void flush() throws InterruptedException {
            spoutInQ.flush();
        }
    }
}

class Consumer extends MyThread {
    public final MutableLong counter = new MutableLong(0);
    private final JCQueue q;

    public Consumer(JCQueue q) {
        super("Consumer");
        this.q = q;
    }

    @Override
    public void run() {
        Handler handler = new Handler();
        long start = System.currentTimeMillis();
        while (!Thread.interrupted()) {
            int x = q.consume(handler);
            if (x == 0) {
                LockSupport.parkNanos(1);
            }
        }
        runTime = System.currentTimeMillis() - start;
    }

    @Override
    public long getCount() {
        return counter.get();
    }

    private class Handler implements JCQueue.Consumer {
        @Override
        public void accept(Object event) {
            counter.increment();
        }

        @Override
        public void flush() {
            // no-op
        }
    }
}


class Forwarder extends MyThread {
    public final MutableLong counter = new MutableLong(0);
    private final JCQueue inq;
    private final JCQueue outq;

    public Forwarder(JCQueue inq, JCQueue outq) {
        super("Forwarder");
        this.inq = inq;
        this.outq = outq;
    }

    @Override
    public void run() {
        Handler handler = new Handler();
        long start = System.currentTimeMillis();
        while (!Thread.interrupted()) {
            int x = inq.consume(handler);
            if (x == 0) {
                LockSupport.parkNanos(1);
            }
        }
        runTime = System.currentTimeMillis() - start;
    }

    @Override
    public long getCount() {
        return counter.get();
    }

    private class Handler implements JCQueue.Consumer {
        @Override
        public void accept(Object event) {
            try {
                outq.publish(event);
                counter.increment();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void flush() {
            // no-op
        }
    }
}