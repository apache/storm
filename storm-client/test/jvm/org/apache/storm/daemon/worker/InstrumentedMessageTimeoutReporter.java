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

package org.apache.storm.daemon.worker;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A reporter for tests: its real thread runs the real cycles, a test can make the next cycle throw, and tests in other
 * packages can start it and read how many events it holds.
 */
public class InstrumentedMessageTimeoutReporter extends MessageTimeoutReporter {

    /** How long a test waits for something the reporter's thread has to do. */
    public static final long WAIT_SECS = 30;

    private final AtomicInteger cyclesEnded = new AtomicInteger();
    private final AtomicInteger linesFormatted = new AtomicInteger();
    private final AtomicReference<Throwable> nextCycleFailure = new AtomicReference<>();

    /** Makes the next cycle throw {@code failure}, a {@code RuntimeException} or an {@code Error}, before it does anything. */
    public void failNextCycle(Throwable failure) {
        nextCycleFailure.set(failure);
    }

    /** Hands over a timeout of task 1 of component "spout", for the tests that do not look at those two fields. */
    public void timedOut(long rootId, Object messageId) {
        timedOut(rootId, 1, "spout", messageId);
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public int queuedEvents() {
        return super.queuedEvents();
    }

    public int cyclesEnded() {
        return cyclesEnded.get();
    }

    public int linesFormatted() {
        return linesFormatted.get();
    }

    public boolean awaitThreadEnd() throws InterruptedException {
        thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECS));
        return !thread.isAlive();
    }

    @Override
    void runCycle() {
        Throwable failure = nextCycleFailure.getAndSet(null);
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw (RuntimeException) failure;
        }
        super.runCycle();
        cyclesEnded.incrementAndGet();
    }

    @Override
    String eventLine(long rootId, int taskId, String componentId, Object messageId) {
        linesFormatted.incrementAndGet();
        return super.eventLine(rootId, taskId, componentId, messageId);
    }
}
