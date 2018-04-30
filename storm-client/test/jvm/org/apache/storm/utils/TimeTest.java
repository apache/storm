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

import org.apache.storm.utils.Time.SimulatedTime;
import org.junit.Assert;
import org.junit.Test;

public class TimeTest {

    @Test
    public void secsToMillisLongTest() {
        Assert.assertEquals(Time.secsToMillisLong(0), 0);
        Assert.assertEquals(Time.secsToMillisLong(0.002), 2);
        Assert.assertEquals(Time.secsToMillisLong(1), 1000);
        Assert.assertEquals(Time.secsToMillisLong(1.08), 1080);
        Assert.assertEquals(Time.secsToMillisLong(10), 10000);
        Assert.assertEquals(Time.secsToMillisLong(10.1), 10100);
    }

    @Test(expected = IllegalStateException.class)
    public void ifNotSimulatingAdvanceTimeThrowsTest() {
        Time.advanceTime(1000);
    }

    @Test
    public void isSimulatingReturnsTrueDuringSimulationTest() {
        Assert.assertFalse(Time.isSimulating());
        try (SimulatedTime t = new SimulatedTime()) {
            Assert.assertTrue(Time.isSimulating());
        }
    }

    @Test
    public void shouldNotAdvanceTimeTest() {
        try (SimulatedTime t = new SimulatedTime()) {
            long current = Time.currentTimeMillis();
            Time.advanceTime(0);
            Assert.assertEquals(Time.deltaMs(current), 0);
        }
    }

    @Test
    public void shouldAdvanceForwardTest() {
        try (SimulatedTime t = new SimulatedTime()) {
            long current = Time.currentTimeMillis();
            Time.advanceTime(1000);
            Assert.assertEquals(Time.deltaMs(current), 1000);
            Time.advanceTime(500);
            Assert.assertEquals(Time.deltaMs(current), 1500);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfAttemptToAdvanceBackwardsTest() {
        try (SimulatedTime t = new SimulatedTime()) {
            Time.advanceTime(-1500);
        }
    }

    @Test
    public void deltaSecsConvertsToSecondsTest() {
        try (SimulatedTime t = new SimulatedTime()) {
            int current = Time.currentTimeSecs();
            Time.advanceTime(1000);
            Assert.assertEquals(Time.deltaSecs(current), 1);
        }
    }

    @Test
    public void deltaSecsTruncatesFractionalSecondsTest() {
        try (SimulatedTime t = new SimulatedTime()) {
            int current = Time.currentTimeSecs();
            Time.advanceTime(1500);
            Assert.assertEquals(Time.deltaSecs(current), 1, 0);
        }
    }

}
