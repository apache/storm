/**
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

package org.apache.storm.metrics2;

import junit.framework.TestCase;
import org.apache.storm.utils.Time;
import org.junit.Assert;
import org.junit.Test;

public class WindowedMeterTest extends TestCase {

    @Test
    public void testIncrementRotate() {
        try (Time.SimulatedTime t = new Time.SimulatedTime()) {
            WindowedMeter meter = new WindowedMeter("meter1", new StormMetricRegistry(), "testTopology",
                    "component1", "stream1",1, 6720, 2);
            meter.close(); // prevent task from rotating

            // add 10 and verify all timecounts see it
            meter.incBy(10);
            Long total = new Long(10L);
            Assert.assertEquals(total, meter.getTimeCounts().get("600"));
            Assert.assertEquals(total, meter.getTimeCounts().get("10800"));
            Assert.assertEquals(total, meter.getTimeCounts().get("86400"));
            Assert.assertEquals(total, meter.getTimeCounts().get(":all-time"));
            Assert.assertEquals(total, new Long(meter.getMeter().getCount()));

            // rotate 11 minutes and validate 10 minute bucket is reduced while
            // others remain steady.
            Time.advanceTime(11L * 60L * 1000L);
            meter.rotateSched();
            Assert.assertTrue(meter.getTimeCounts().get("600") < total);
            Assert.assertEquals(total, meter.getTimeCounts().get("10800"));
            Assert.assertEquals(total, meter.getTimeCounts().get("86400"));
            Assert.assertEquals(total, meter.getTimeCounts().get(":all-time"));
            Assert.assertEquals(total, new Long(meter.getMeter().getCount()));

            // add another 10.  non 10 minute buckets should see 20
            meter.incBy(10);
            total = new Long(20L);
            long tenMinValue = meter.getTimeCounts().get("600");
            Assert.assertTrue(meter.getTimeCounts().get("600") >= 10L);
            Assert.assertTrue(meter.getTimeCounts().get("600") < total);
            Assert.assertEquals(total, meter.getTimeCounts().get("10800"));
            Assert.assertEquals(total, meter.getTimeCounts().get("86400"));
            Assert.assertEquals(total, meter.getTimeCounts().get(":all-time"));
            Assert.assertEquals(total, new Long(meter.getMeter().getCount()));

            // rotate after 4 hours.  10 minute and 3 hour buckets should be reduced.
            Time.advanceTime(4L * 60L * 60L * 1000L);
            meter.rotateSched();
            Assert.assertTrue(meter.getTimeCounts().get("600") < tenMinValue);
            Assert.assertTrue(meter.getTimeCounts().get("10800") < 20L);
            Assert.assertEquals(total, meter.getTimeCounts().get("86400"));
            Assert.assertEquals(total, meter.getTimeCounts().get(":all-time"));
            Assert.assertEquals(total, new Long(meter.getMeter().getCount()));
        }
    }
}
