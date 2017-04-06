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

import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements time simulation support. When time simulation is enabled, methods on this class will use fixed time.
 * When time simulation is disabled, methods will pass through to relevant java.lang.System/java.lang.Thread calls.
 * Methods using units higher than nanoseconds will pass through to System.currentTimeMillis(). Methods supporting nanoseconds will pass through to System.nanoTime().
 */
public class Time {
    private static final Logger LOG = LoggerFactory.getLogger(Time.class);
    private static AtomicBoolean simulating = new AtomicBoolean(false);
    private static AtomicLong autoAdvanceNanosOnSleep = new AtomicLong(0);
    private static volatile Map<Thread, AtomicLong> threadSleepTimesNanos;
    private static final Object sleepTimesLock = new Object();
    private static AtomicLong simulatedCurrTimeNanos;
    
    public static class SimulatedTime implements AutoCloseable {

        public SimulatedTime() {
            this(null);
        }
        
        public SimulatedTime(Number advanceTimeMs) {
            synchronized(Time.sleepTimesLock) {
                Time.simulating.set(true);
                Time.simulatedCurrTimeNanos = new AtomicLong(0);
                Time.threadSleepTimesNanos = new ConcurrentHashMap<>();
                if (advanceTimeMs != null) {
                    Time.autoAdvanceNanosOnSleep.set(millisToNanos(advanceTimeMs.longValue()));
                }
                LOG.warn("AutoCloseable Simulated Time Starting...");
            }
        }
        
        @Override
        public void close() {
            synchronized(Time.sleepTimesLock) {
                Time.simulating.set(false);    
                Time.autoAdvanceNanosOnSleep.set(0);
                Time.threadSleepTimesNanos = null;
                LOG.warn("AutoCloseable Simulated Time Ending...");
            }
        }
    }
    
    @Deprecated
    public static void startSimulating() {
        synchronized(Time.sleepTimesLock) {
            Time.simulating.set(true);
            Time.simulatedCurrTimeNanos = new AtomicLong(0);
            Time.threadSleepTimesNanos = new ConcurrentHashMap<>();
            LOG.warn("Simulated Time Starting...");
        }
    }
    
    @Deprecated
    public static void stopSimulating() {
        synchronized(Time.sleepTimesLock) {
            Time.simulating.set(false);    
            Time.autoAdvanceNanosOnSleep.set(0);
            Time.threadSleepTimesNanos = null;
            LOG.warn("Simulated Time Ending...");
        }
    }
    
    public static boolean isSimulating() {
        return simulating.get();
    }
    
    public static void sleepUntil(long targetTimeMs) throws InterruptedException {
        if(simulating.get()) {
            simulatedSleepUntilNanos(millisToNanos(targetTimeMs));
        } else {
            long sleepTimeMs = targetTimeMs - currentTimeMillis();
            if(sleepTimeMs>0) {
                Thread.sleep(sleepTimeMs);
            }
        }
    }
    
    public static void sleepUntilNanos(long targetTimeNanos) throws InterruptedException {
        if(simulating.get()) {
            simulatedSleepUntilNanos(targetTimeNanos);
        } else {
            long sleepTimeNanos = targetTimeNanos-nanoTime();
            long sleepTimeMs = nanosToMillis(sleepTimeNanos);
            int sleepTimeNanosSansMs = (int)(sleepTimeNanos%1_000_000);
            if(sleepTimeNanos>0) {
                Thread.sleep(sleepTimeMs, sleepTimeNanosSansMs);
            } 
        }
    }
    
    private static void simulatedSleepUntilNanos(long targetTimeNanos) throws InterruptedException {
        try {
            synchronized (sleepTimesLock) {
                if (threadSleepTimesNanos == null) {
                    LOG.debug("{} is still sleeping after simulated time disabled.", Thread.currentThread(), new RuntimeException("STACK TRACE"));
                    throw new InterruptedException();
                }
                threadSleepTimesNanos.put(Thread.currentThread(), new AtomicLong(targetTimeNanos));
            }
            while (simulatedCurrTimeNanos.get() < targetTimeNanos) {
                synchronized (sleepTimesLock) {
                    if (threadSleepTimesNanos == null) {
                        LOG.debug("{} is still sleeping after simulated time disabled.", Thread.currentThread(), new RuntimeException("STACK TRACE"));
                        throw new InterruptedException();
                    }
                }
                long autoAdvance = autoAdvanceNanosOnSleep.get();
                if (autoAdvance > 0) {
                    advanceTimeNanos(autoAdvance);
                }
                Thread.sleep(10);
            }
        } finally {
            synchronized (sleepTimesLock) {
                if (simulating.get() && threadSleepTimesNanos != null) {
                    threadSleepTimesNanos.remove(Thread.currentThread());
                }
            }
        }
    }

    public static void sleep(long ms) throws InterruptedException {
        sleepUntil(currentTimeMillis()+ms);
    }
    
    public static void sleepNanos(long nanos) throws InterruptedException {
        sleepUntilNanos(nanoTime() + nanos);
    }

    public static void sleepSecs (long secs) throws InterruptedException {
        if (secs > 0) {
            sleep(secs * 1000);
        }
    }
    
    public static long nanoTime() {
        if (simulating.get()) {
            return simulatedCurrTimeNanos.get();
        } else {
            return System.nanoTime();
        }
    }
    
    public static long currentTimeMillis() {
        if(simulating.get()) {
            return nanosToMillis(simulatedCurrTimeNanos.get());
        } else {
            return System.currentTimeMillis();
        }
    }

    public static long nanosToMillis(long nanos) {
        return nanos/1_000_000;
    }
    
    public static long millisToNanos(long millis) {
        return millis*1_000_000;
    }
    
    public static long secsToMillis (int secs) {
        return 1000*(long) secs;
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
        if (!simulating.get()) {
            throw new IllegalStateException("Cannot simulate time unless in simulation mode");
        }
        if (nanos < 0) {
            throw new IllegalArgumentException("advanceTime only accepts positive time as an argument");
        }
        long newTime = simulatedCurrTimeNanos.addAndGet(nanos);
        LOG.debug("Advanced simulated time to {}", newTime);
    }
    
    public static void advanceTimeSecs(long secs) {
        advanceTime(secs * 1_000);
    }
    
    public static boolean isThreadWaiting(Thread t) {
        if(!simulating.get()) {
            throw new IllegalStateException("Must be in simulation mode");
        }
        AtomicLong time;
        synchronized(sleepTimesLock) {
            time = threadSleepTimesNanos.get(t);
        }
        return !t.isAlive() || time!=null && nanoTime() < time.longValue();
    }
}
