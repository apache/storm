/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.metric.internal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for RateTracker
 */
public class RateTrackerTest {

    @Test
    public void testExactRate() {
        //This test is in two phases.  The first phase fills up the 10 buckets with 10 tuples each
        // We purposely simulate a 1 second bucket size so the rate will always be 10 per second.
        final long interval = 1000L;
        long time = 0L;
        RateTracker rt = new RateTracker(10000, 10, time);
        double[] expected = new double[]{ 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0 };
        for (int i = 0; i < expected.length; i++) {
            double exp = expected[i];
            rt.notify(10);
            time += interval;
            double actual = rt.reportRate(time);
            rt.forceRotate(1, interval);
            assertEquals(exp, actual, 0.00001, "Expected rate on iteration " + i + " is wrong.");
        }
        //In the second part of the test the rate doubles to 20 per second but the rate tracker
        // increases its result slowly as we push the 10 tuples per second buckets out and replace them
        // with 20 tuples per second. 
        expected = new double[]{ 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0 };
        for (int i = 0; i < expected.length; i++) {
            double exp = expected[i];
            rt.notify(20);
            time += interval;
            double actual = rt.reportRate(time);
            rt.forceRotate(1, interval);
            assertEquals(exp, actual, 0.00001, "Expected rate on iteration " + i + " is wrong.");
        }
    }

    @Test
    public void testEclipsedAllWindows() {
        long time = 0;
        RateTracker rt = new RateTracker(10000, 10, time);
        rt.notify(10);
        rt.forceRotate(10, 1000L);
        assertEquals(0.0, rt.reportRate(10000L), 0.00001);
    }

    @Test
    public void testEclipsedOneWindow() {
        long time = 0;
        RateTracker rt = new RateTracker(10000, 10, time);
        rt.notify(1);
        double r1 = rt.reportRate(1000L);
        rt.forceRotate(1, 1000L);
        rt.notify(1);
        double r2 = rt.reportRate(2000L);

        assertEquals(r1, r2, 0.00001);
    }

    @Test
    public void testEclipsedNineWindows() {
        long time = 0;
        RateTracker rt = new RateTracker(10000, 10, time);
        rt.notify(1);
        double r1 = rt.reportRate(1000);
        rt.forceRotate(9, 1000);
        rt.notify(9);
        double r2 = rt.reportRate(10000);

        assertEquals(r1, r2, 0.00001);
    }
}
