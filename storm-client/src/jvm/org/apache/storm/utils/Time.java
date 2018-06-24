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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements time simulation support. When time simulation is enabled, methods on this class will use fixed time. When time
 * simulation is disabled, methods will pass through to relevant java.lang.System/java.lang.Thread calls. Methods using units higher than
 * nanoseconds will pass through to System.currentTimeMillis(). Methods supporting nanoseconds will pass through to System.nanoTime().
 */
public class Time {
    private static final Logger LOG = LoggerFactory.getLogger(Time.class);
    private static final AtomicBoolean SIMULATING = new AtomicBoolean(false);
    private static final AtomicLong AUTO_ADVANCE_NANOS_ON_SLEEP = new AtomicLong(0);
    private static final Map<Thread, AtomicLong> THREAD_SLEEP_TIMES_NANOS = new ConcurrentHashMap<>();
    private static final Object SLEEP_TIMES_LOCK = new Object();
    private static final AtomicLong SIMULATED_CURR_TIME_NANOS = new AtomicLong(0); 

    public static boolean isSimulating() {
        return SIMULATING.get();
    }

    public static void sleepUntil(long targetTimeMs) throws InterruptedException {
        if (SIMULATING.get()) {
            simulatedSleepUntilNanos(millisToNanos(targetTimeMs));
        } else {
            long sleepTimeMs = targetTimeMs - currentTimeMillis();
            if (sleepTimeMs > 0) {
                Thread.sleep(sleepTimeMs);
            }
        }
    }

    public static void sleepUntilNanos(long targetTimeNanos) throws InterruptedException {
        if (SIMULATING.get()) {
            simulatedSleepUntilNanos(targetTimeNanos);
        } else {
            long sleepTimeNanos = targetTimeNanos - nanoTime();
            long sleepTimeMs = nanosToMillis(sleepTimeNanos);
            int sleepTimeNanosSansMs = (int) (sleepTimeNanos % 1_000_000);
            if (sleepTimeNanos > 0) {
                Thread.sleep(sleepTimeMs, sleepTimeNanosSansMs);
            }
        }
    }

    private static void simulatedSleepUntilNanos(long targetTimeNanos) throws InterruptedException {
        try {
            synchronized (SLEEP_TIMES_LOCK) {
                if (!SIMULATING.get()) {
                    LOG.debug("{} is still sleeping after simulated time disabled.", Thread.currentThread(),
                        new RuntimeException("STACK TRACE"));
                    throw new InterruptedException();
                }
                THREAD_SLEEP_TIMES_NANOS.put(Thread.currentThread(), new AtomicLong(targetTimeNanos));
            }
            while (SIMULATED_CURR_TIME_NANOS.get() < targetTimeNanos) {
                synchronized (SLEEP_TIMES_LOCK) {
                    if (!SIMULATING.get()) {
                        LOG.debug("{} is still sleeping after simulated time disabled.", Thread.currentThread(),
                                  new RuntimeException("STACK TRACE"));
                        throw new InterruptedException();
                    }
                    long autoAdvance = AUTO_ADVANCE_NANOS_ON_SLEEP.get();
                    if (autoAdvance > 0) {
                        advanceTimeNanos(autoAdvance);
                    }
                }
                Thread.sleep(10);
            }
        } finally {
            THREAD_SLEEP_TIMES_NANOS.remove(Thread.currentThread());
        }
    }
    
    public static void sleep(long ms) throws InterruptedException {
        if (ms > 0) {
            if (SIMULATING.get()) {
                simulatedSleepUntilNanos(millisToNanos(currentTimeMillis() + ms));
            } else {
                Thread.sleep(ms);
            }
        }
    }

    public static void parkNanos(long nanos) throws InterruptedException {
        if (nanos > 0) {
            if (SIMULATING.get()) {
                simulatedSleepUntilNanos(nanoTime() + nanos);
            } else {
                LockSupport.parkNanos(nanos);
            }
        }
    }

    public static void sleepSecs(long secs) throws InterruptedException {
        if (secs > 0) {
            sleep(secs * 1000);
        }
    }

    public static long nanoTime() {
        if (SIMULATING.get()) {
            return SIMULATED_CURR_TIME_NANOS.get();
        } else {
            return System.nanoTime();
        }
    }

    public static long currentTimeMillis() {
        if (SIMULATING.get()) {
            return nanosToMillis(SIMULATED_CURR_TIME_NANOS.get());
        } else {
            return System.currentTimeMillis();
        }
    }

    public static long nanosToMillis(long nanos) {
        return nanos / 1_000_000;
    }

    public static long millisToNanos(long millis) {
        return millis * 1_000_000;
    }

    public static long secsToMillis(int secs) {
        return 1000 * (long) secs;
    }

    public static long secsToMillisLong(double secs) {
        return (long) (1000 * secs);
    }

    public static int currentTimeSecs() {
        return (int) (currentTimeMillis() / 1000);
    }

    public static int deltaSecs(int timeInSeconds) {
        return Time.currentTimeSecs() - timeInSeconds;
    }

    public static long deltaMs(long timeInMilliseconds) {
        return Time.currentTimeMillis() - timeInMilliseconds;
    }

    public static void advanceTime(long ms) {
        advanceTimeNanos(millisToNanos(ms));
    }

    public static void advanceTimeNanos(long nanos) {
        if (!SIMULATING.get()) {
            throw new IllegalStateException("Cannot simulate time unless in simulation mode");
        }
        if (nanos < 0) {
            throw new IllegalArgumentException("advanceTime only accepts positive time as an argument");
        }
        synchronized (SLEEP_TIMES_LOCK) {
            long newTime = SIMULATED_CURR_TIME_NANOS.addAndGet(nanos);
            Iterator<AtomicLong> sleepTimesIter = THREAD_SLEEP_TIMES_NANOS.values().iterator();
            while (sleepTimesIter.hasNext()) {
                AtomicLong curr = sleepTimesIter.next();
                if (SIMULATED_CURR_TIME_NANOS.get() >= curr.get()) {
                    sleepTimesIter.remove();
                }
            }
            LOG.debug("Advanced simulated time to {}", newTime);
        }
    }

    public static void advanceTimeSecs(long secs) {
        advanceTime(secs * 1_000);
    }

    public static boolean isThreadWaiting(Thread t) {
        if (!SIMULATING.get()) {
            throw new IllegalStateException("Must be in simulation mode");
        }
        AtomicLong time = THREAD_SLEEP_TIMES_NANOS.get(t);
        return !t.isAlive() || time != null && nanoTime() < time.longValue();
    }

    public static class SimulatedTime implements AutoCloseable {

        public SimulatedTime() {
            this(null);
        }

        public SimulatedTime(Number advanceTimeMs) {
            synchronized (Time.SLEEP_TIMES_LOCK) {
                Time.SIMULATING.set(true);
                Time.SIMULATED_CURR_TIME_NANOS.set(0);
                Time.THREAD_SLEEP_TIMES_NANOS.clear();
                if (advanceTimeMs != null) {
                    Time.AUTO_ADVANCE_NANOS_ON_SLEEP.set(millisToNanos(advanceTimeMs.longValue()));
                } else {
                    Time.AUTO_ADVANCE_NANOS_ON_SLEEP.set(0);
                }
                LOG.warn("AutoCloseable Simulated Time Starting...");
            }
        }

        @Override
        public void close() {
            synchronized (Time.SLEEP_TIMES_LOCK) {
                Time.SIMULATING.set(false);
                LOG.warn("AutoCloseable Simulated Time Ending...");
            }
        }
    }
}
