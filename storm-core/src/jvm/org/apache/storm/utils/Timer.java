/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.utils;

import org.apache.storm.callback.AsyncLoopDefaultKill;
import org.apache.storm.callback.RunnableCallback;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/* The timer defined in this file is very similar to java.util.Timer, except
   it integrates with Storm's time simulation capabilities. This lets us test
   code that does asynchronous work on the timer thread*/

public class Timer {
    private PriorityQueue priorityQueue;
    private AtomicBoolean active = new AtomicBoolean(true);
    private Semaphore notify = new Semaphore(0);
    private RunnableCallback kill_fn;
    private Thread thread;

    public Timer() {
        kill_fn = new AsyncLoopDefaultKill();
        this.init(true, kill_fn, "timer", Thread.MAX_PRIORITY, true);
    }

    public Timer(String threadName) {
        kill_fn = new AsyncLoopDefaultKill();

        this.init(true, kill_fn, threadName, Thread.MAX_PRIORITY, true);
    }

    public Timer(RunnableCallback kill_fn, String threadName) {
        this.init(true, kill_fn, threadName, Thread.MAX_PRIORITY, true);
    }

    public Timer(boolean daemon, RunnableCallback kill_fn, String threadName, int priority, boolean start) {
        this.init(daemon, kill_fn, threadName, priority, start);
    }

    private void init(boolean daemon, RunnableCallback kill_fn, String threadName, int priority, boolean start) {
        priorityQueue = new PriorityQueue<ScheduleRunnable>(10, new TimerComparator());
        this.kill_fn = kill_fn;
        thread = new TimerThread(priorityQueue);
        thread.setDaemon(daemon);
        thread.setName(threadName);
        thread.setPriority(priority);
        if (start) {
            thread.start();
        }
    }

    public void start() {
        thread.start();
    }

    public void scheduleRecurringWithJitter(RunnableCallback afn, long delayMs, long recurMs, long jitterMs) {
        ScheduleRunnable scheduleRunnable = new ScheduleRunnable(afn, delayMs, recurMs, jitterMs);
        addQueue(scheduleRunnable);
    }

    public void scheduleRecurring(RunnableCallback afn, long delayMs, long recurMs) {
        ScheduleRunnable scheduleRunnable = new ScheduleRunnable(afn, delayMs, recurMs, 0);
        addQueue(scheduleRunnable);
    }

    protected void addQueue(ScheduleRunnable scheduleRunnable) {
        // This avoids a race condition with cancel-timer.
        synchronized (priorityQueue) {
            priorityQueue.add(scheduleRunnable);
        }
    }

    public AtomicBoolean getActive() {
        return active;
    }

    public void checkActive() {
        if (!active.get()) {
            throw new IllegalStateException("Timer is not active");
        }
    }

    public boolean timerWaiting() {
        return Time.isThreadWaiting(thread);
    }

    public void cancelTimer() throws InterruptedException {
        checkActive();
        synchronized (priorityQueue) {
            active.set(false);
            thread.interrupt();
        }
        notify.acquire();
    }

    class TimerThread extends Thread {
        private PriorityQueue priorityQueue;

        public TimerThread(PriorityQueue queue) {
            priorityQueue = queue;
        }

        public void run() {
            while (active.get()) {
                try {
                    ScheduleRunnable afn = null;
                    synchronized (priorityQueue) {
                        afn = (ScheduleRunnable) priorityQueue.peek();
                    }
                    if (afn != null && System.currentTimeMillis() >= afn.getNextTimeMs()) {
                        synchronized (this) {
                            afn = (ScheduleRunnable) priorityQueue.poll();
                        }
                        afn.run();
                        addQueue(afn);
                    }

                    if (afn.getNextTimeMs() > 0) {
                        long minSleep = Math.min(1000, afn.getNextTimeMs() - System.currentTimeMillis());
                        Utils.sleep(minSleep);
                    } else {
                        Utils.sleep(1000);
                    }
                } catch (Throwable e) {
                    // Because the interrupted exception can be
                    // wrapped in a RuntimeException.
                    if (!Utils.exceptionCanse(e)) {
                        kill_fn.run();
                        active.set(false);
                        throw e;
                    }
                }
                notify.release();
            }
        }
    }

    class TimerComparator implements Comparator<ScheduleRunnable> {
        @Override
        public int compare(ScheduleRunnable o1, ScheduleRunnable o2) {
            return o1.getNextTimeMs() > o2.getNextTimeMs() ? 1 : -1;
        }

        @Override
        public boolean equals(Object obj) {
            return true;
        }
    }

    class ScheduleRunnable implements Runnable {

        private long recurMs;
        private Long jitterMs;
        private long nextTimeMs;
        private RunnableCallback afn;
        private Random random = new Random();

        public ScheduleRunnable(RunnableCallback afn, long delayMs, long recurMs) {
            init(afn, delayMs, recurMs, 0);
        }

        public ScheduleRunnable(RunnableCallback afn, long delayMs, long recurMs, long jitterMs) {
            init(afn, delayMs, recurMs, jitterMs);
        }

        private void init(RunnableCallback afn, long delayMs, long recurMs, long jitterMs) {
            this.afn = afn;
            this.recurMs = recurMs;
            this.jitterMs = jitterMs;
            this.nextTimeMs = delayMs + System.currentTimeMillis();
        }

        public void run() {
            afn.run();
            this.nextTimeMs = recurMs + System.currentTimeMillis();
            if (jitterMs > 0) {
                this.nextTimeMs = nextTimeMs + random.nextInt(jitterMs.intValue());

            }
        }

        public long getNextTimeMs() {
            return nextTimeMs;
        }
    }
}
