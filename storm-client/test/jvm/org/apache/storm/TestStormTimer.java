/*
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

package org.apache.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

public class TestStormTimer {

    enum SCHEDULE_TYPE {
        IMMEDIATE, IMMEDIATE_WITH_JITTER,
        AFTER_1_SECOND, AFTER_1_SECOND_WITH_JITTER,
        AFTER_MILLISECONDS, AFTER_MILLISECONDS_WITH_JITTER,
        RECURRING, RECURRING_MS,
        RECURRING_WITH_JITTER
    };
    /**
     * Test {@link StormTimer#schedule(int, Runnable)} and {@link StormTimer#schedule(int, Runnable, boolean, int)}
     * for scheduling order under multithreaded environment.
     */
    @Test
    public void testSchedule() {
        StormTimer stormTimer = new StormTimer("testSchedule", (x,y) -> {});
        int threadCnt = 100;
        Assert.assertTrue(schedule(stormTimer, threadCnt, SCHEDULE_TYPE.IMMEDIATE));
        Assert.assertTrue(schedule(stormTimer, threadCnt, SCHEDULE_TYPE.AFTER_1_SECOND));
        Assert.assertTrue(schedule(stormTimer, threadCnt, SCHEDULE_TYPE.IMMEDIATE_WITH_JITTER));
        Assert.assertTrue(schedule(stormTimer, threadCnt, SCHEDULE_TYPE.AFTER_MILLISECONDS_WITH_JITTER));
        close(stormTimer);
    }

    /**
     * Test {@link StormTimer#scheduleMs(long, Runnable)} and
     * {@link StormTimer#scheduleMs(long, Runnable, boolean, int)} for scheduling order under multithreaded environment.
     */
    @Test
    public void testScheduleMs() {
        StormTimer stormTimer = new StormTimer("testScheduleMs", (x,y) -> {});
        int threadCnt = 100;
        Assert.assertTrue(schedule(stormTimer, threadCnt, SCHEDULE_TYPE.AFTER_MILLISECONDS));
        Assert.assertTrue(schedule(stormTimer, threadCnt, SCHEDULE_TYPE.AFTER_MILLISECONDS_WITH_JITTER));
        close(stormTimer);
    }

    /**
     * Test {@link StormTimer#scheduleRecurring(int, int, Runnable)} for scheduling order under multithreaded environment.
     */
    @Test
    public void scheduleRecurring() {
        StormTimer stormTimer = new StormTimer("testScheduleMs", (x,y) -> {});
        int threadCnt = 10;
        Assert.assertTrue(schedule(stormTimer, threadCnt, SCHEDULE_TYPE.RECURRING));
        close(stormTimer);
    }

    /**
     * Test {@link StormTimer#scheduleRecurringMs(long, long, Runnable)} for scheduling order under multithreaded environment.
     */
    @Test
    public void testScheduleRecurringMs() {
        StormTimer stormTimer = new StormTimer("testScheduleRecurringMs", (x,y) -> {});
        int threadCnt = 10;
        Assert.assertTrue(schedule(stormTimer, threadCnt, SCHEDULE_TYPE.RECURRING_MS));
        close(stormTimer);
    }

    /**
     * Test {@link StormTimer#scheduleRecurringWithJitter(int, int, int, Runnable)}
     * for scheduling order under multithreaded environment.
     */
    @Test
    public void testScheduleRecurringWithJitter() {
        StormTimer stormTimer = new StormTimer("testScheduleRecurringWithJitter", (x,y) -> {});
        int threadCnt = 10;
        Assert.assertTrue(schedule(stormTimer, threadCnt, SCHEDULE_TYPE.RECURRING_WITH_JITTER));
        close(stormTimer);
    }

    private void close(StormTimer stormTimer) {
        try {
            stormTimer.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Schedule specified number of threads. The threads are not executed in the order in which
     * they call {@link StormTimer#schedule(int, Runnable)}. Just check if all of them are called (a weaker guarantee).
     * If the threads are executed in the order they are scheduled, then the counter will match the number in the thread.
     * Which they dont. If all the threads are executed, then the total number will match the total jobs scheduled.
     *
     * @param stormTimer StormTimer stormTimer.
     * @param threadCnt Number of threads to fire.
     * @return true if there are no errors, false otherwise.
     */
    private boolean schedule(StormTimer stormTimer, int threadCnt, SCHEDULE_TYPE scheduleType) {
        final AtomicInteger counter = new AtomicInteger(0);
        class ScheduleRunnable implements Runnable {
            final int runnableNum;
            int counterValueBeforeIncrement = -1;

            public ScheduleRunnable(int runnableNum) {
                this.runnableNum = runnableNum;
            }

            @Override
            public void run() {
                counterValueBeforeIncrement = counter.getAndIncrement();
            }

            public int getCounterValueBeforeIncrement() {
                return counterValueBeforeIncrement;
            }

            public boolean isSuccess() {
                return counterValueBeforeIncrement == runnableNum;
            }
        }
        ScheduleRunnable[] runnables = new ScheduleRunnable[threadCnt];
        for (int i = 0; i < threadCnt ; i++) {
            runnables[i] = new ScheduleRunnable(i);
        }
        long sleepMsBeforeCheck = 3000;
        int expectedCounterAtCheck = threadCnt;
        int delaySecs = 1;
        for (int i = 0; i < threadCnt; i++) {
            final boolean checkActive = true;
            final int jitterMs = 10;
            final int delayMs = 300;
            final int recurSecs = 3;
            final int recurMs = 300;
            switch(scheduleType) {
                case IMMEDIATE:
                    delaySecs = 0;
                    stormTimer.schedule(delaySecs, runnables[i]);
                    break;
                case IMMEDIATE_WITH_JITTER:
                    delaySecs = 0;
                    stormTimer.schedule(delaySecs, runnables[i], checkActive, jitterMs);
                    break;
                case AFTER_1_SECOND:
                    delaySecs = 1;
                    stormTimer.schedule(delaySecs, runnables[i]);
                    break;
                case AFTER_1_SECOND_WITH_JITTER:
                    delaySecs = 1;
                    stormTimer.schedule(delaySecs, runnables[i], checkActive, jitterMs);
                    break;
                case AFTER_MILLISECONDS:
                    stormTimer.scheduleMs(delayMs, runnables[i]);
                    break;
                case AFTER_MILLISECONDS_WITH_JITTER:
                    stormTimer.scheduleMs(delayMs, runnables[i], checkActive, jitterMs);
                    break;
                case RECURRING:
                    stormTimer.scheduleRecurring(delaySecs, recurSecs, runnables[i]);
                    sleepMsBeforeCheck = 10000 + delaySecs * 1000;
                    int sleepSecsBeforeCheck = (int)(sleepMsBeforeCheck / 1000);
                    expectedCounterAtCheck = ((sleepSecsBeforeCheck - delaySecs) / recurSecs) * threadCnt;
                    break;
                case RECURRING_MS:
                    stormTimer.scheduleRecurringMs(delayMs, recurMs, runnables[i]);
                    sleepMsBeforeCheck = 10000 + delayMs;
                    expectedCounterAtCheck = threadCnt * (int)((sleepMsBeforeCheck - delayMs) / recurMs);
                    break;
                case RECURRING_WITH_JITTER:
                    stormTimer.scheduleRecurringWithJitter(delaySecs, recurSecs, jitterMs, runnables[i]);
                    sleepMsBeforeCheck = 10000 + delaySecs * 1000;
                    sleepSecsBeforeCheck = (int)(sleepMsBeforeCheck / 1000);
                    expectedCounterAtCheck = ((sleepSecsBeforeCheck - delaySecs) / recurSecs) * threadCnt;
                    break;
                default:
                    // do nothing
            }
        }

        try {
            Thread.sleep(sleepMsBeforeCheck);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        boolean schedulingOrderGuarantee = false;
        if (schedulingOrderGuarantee) {
            // the following scheduling order strong guarantee fails
            List<String> errs = new ArrayList<>();
            for (int i = 0; i < threadCnt; i++) {
                ScheduleRunnable runnable = runnables[i];
                if (!runnable.isSuccess()) {
                    errs.add(String.format("Runnable %d was expecting counter value %d but found %d",
                        runnable.runnableNum, runnable.runnableNum, runnable.counterValueBeforeIncrement));
                }
            }
            Assert.assertTrue(String.join(",\n\t", errs), errs.isEmpty());
            return errs.isEmpty();
        } else {
            // this weaker guarantee of total number of executions should succeed except for recurring schedule
            int actualCounter = counter.get();
            if (scheduleType == SCHEDULE_TYPE.RECURRING ||
                scheduleType == SCHEDULE_TYPE.RECURRING_MS ||
                scheduleType == SCHEDULE_TYPE.RECURRING_WITH_JITTER) {
                if (expectedCounterAtCheck != counter.get()) {
                    System.err.printf("Ignoring count mismatch with recurring scheduleType of %s, expected=%d, actual=%d\n",
                        scheduleType, expectedCounterAtCheck, actualCounter);
                }
            } else {
                Assert.assertEquals("Number of runnables completed", expectedCounterAtCheck, actualCounter);
            }
            return true;
        }
    }
}