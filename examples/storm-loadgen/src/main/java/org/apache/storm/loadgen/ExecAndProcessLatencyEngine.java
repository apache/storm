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

package org.apache.storm.loadgen;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * A more accurate sleep implementation.
 */
public class ExecAndProcessLatencyEngine implements Serializable {
    private static final long NANO_IN_MS = TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS);
    private final SlowExecutorPattern skewedPattern;

    public static long toNano(double ms) {
        return (long)(ms * NANO_IN_MS);
    }

    private final AtomicLong parkOffset = new AtomicLong(0);
    private Random rand;
    private ScheduledExecutorService timer;

    public ExecAndProcessLatencyEngine() {
        this(null);
    }

    public ExecAndProcessLatencyEngine(SlowExecutorPattern skewedPattern) {
        this.skewedPattern = skewedPattern;
    }

    public void prepare() {
        this.rand = ThreadLocalRandom.current();
        this.timer = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * Sleep for a set number of nano seconds.
     * @param start the start time of the sleep
     * @param sleepAmount how many nano seconds after start when we should stop.
     */
    public void sleepNano(long start, long sleepAmount) {
        long endTime = start + sleepAmount;
        // A small control algorithm to adjust the amount of time that we sleep to make it more accurate
        long newEnd = endTime - parkOffset.get();
        long diff = newEnd - start;
        //There are some different levels of accuracy here, and we want to deal with all of them
        if (diff <= 1_000) {
            //We are done, nothing that short is going to work here
        } else if (diff < NANO_IN_MS) {
            //Busy wait...
            long sum = 0;
            while (System.nanoTime() < newEnd) {
                for (long i = 0; i < 1_000_000; i++) {
                    sum += i;
                }
            }
        } else {
            //More accurate that thread.sleep, but still not great
            LockSupport.parkNanos(newEnd - System.nanoTime());
        }
        parkOffset.addAndGet((System.nanoTime() - endTime) / 2);
    }

    public void sleepNano(long nano) {
        sleepNano(System.nanoTime(), nano);
    }

    public void sleepUntilNano(long endTime) {
        long start = System.nanoTime();
        sleepNano(start, endTime - start);
    }

    /**
     * Simulate both process and exec times.
     * @param executorIndex the index of this executor.  It is used to skew the latencies.
     * @param startTimeNs when the executor started in nano-seconds.
     * @param in the metrics for the input stream (or null if you don't want to use them).
     * @param r what to run when the process latency is up.  Note that this may run on a separate thread after this method call has
     *     completed.
     */
    public void simulateProcessAndExecTime(int executorIndex, long startTimeNs, InputStream in, Runnable r) {
        long extraTimeNs = skewedPattern == null ? 0 : toNano(skewedPattern.getExtraSlowness(executorIndex));
        long endExecNs = startTimeNs + extraTimeNs + (in == null ? 0 : ExecAndProcessLatencyEngine.toNano(in.execTime.nextRandom(rand)));
        long endProcNs = startTimeNs + extraTimeNs + (in == null ? 0 : ExecAndProcessLatencyEngine.toNano(in.processTime.nextRandom(rand)));

        if ((endProcNs - 1_000_000) < endExecNs) {
            sleepUntilNano(endProcNs);
            r.run();
        } else {
            timer.schedule(() -> {
                r.run();
            }, Math.max(0, endProcNs - System.nanoTime()), TimeUnit.NANOSECONDS);
        }

        sleepUntilNano(endExecNs);
    }
}
