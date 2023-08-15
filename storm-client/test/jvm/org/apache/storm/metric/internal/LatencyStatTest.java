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

package org.apache.storm.metric.internal;

import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for LatencyStat
 */
public class LatencyStatTest {
    final long TEN_MIN_IN_MS = 10 * 60 * 1000;
    final long THIRTY_SEC_IN_MS = 30 * 1000;
    final double THREE_HOUR_IN_MS = 3 * 60 * 60 * 1000;
    final double ONE_DAY_IN_MS = 24 * 60 * 60 * 1000;

    @Test
    public void testBasic() {
        long time = 0L;
        LatencyStat lat = new LatencyStat(10, time);
        while (time < TEN_MIN_IN_MS) {
            lat.record(100);
            time += THIRTY_SEC_IN_MS;
            assertEquals(100.0, (Double) lat.getValueAndReset(time), 0.01);
        }

        Map<String, Double> found = lat.getTimeLatAvg(time);
        assertEquals(4, found.size());
        assertEquals(100.0, found.get("600"), 0.01);
        assertEquals(100.0, found.get("10800"), 0.01);
        assertEquals(100.0, found.get("86400"), 0.01);
        assertEquals(100.0, found.get(":all-time"), 0.01);

        while (time < THREE_HOUR_IN_MS) {
            lat.record(200);
            time += THIRTY_SEC_IN_MS;
            assertEquals(200.0, (Double) lat.getValueAndReset(time), 0.01);
        }

        double expected = ((100.0 * TEN_MIN_IN_MS / THIRTY_SEC_IN_MS) + (200.0 * (THREE_HOUR_IN_MS - TEN_MIN_IN_MS) / THIRTY_SEC_IN_MS)) /
                          (THREE_HOUR_IN_MS / THIRTY_SEC_IN_MS);
        found = lat.getTimeLatAvg(time);
        assertEquals(4, found.size());
        assertEquals(200.0, found.get("600"), 0.01); //flushed the buffers completely
        assertEquals(expected, found.get("10800"), 0.01);
        assertEquals(expected, found.get("86400"), 0.01);
        assertEquals(expected, found.get(":all-time"), 0.01);

        while (time < ONE_DAY_IN_MS) {
            lat.record(300);
            time += THIRTY_SEC_IN_MS;
            assertEquals(300.0, (Double) lat.getValueAndReset(time), 0.01);
        }

        expected = ((100.0 * TEN_MIN_IN_MS / THIRTY_SEC_IN_MS) + (200.0 * (THREE_HOUR_IN_MS - TEN_MIN_IN_MS) / THIRTY_SEC_IN_MS) +
                    (300.0 * (ONE_DAY_IN_MS - THREE_HOUR_IN_MS) / THIRTY_SEC_IN_MS)) /
                   (ONE_DAY_IN_MS / THIRTY_SEC_IN_MS);
        found = lat.getTimeLatAvg(time);
        assertEquals(4, found.size());
        assertEquals(300.0, found.get("600"), 0.01); //flushed the buffers completely
        assertEquals(300.0, found.get("10800"), 0.01);
        assertEquals(expected, found.get("86400"), 0.01);
        assertEquals(expected, found.get(":all-time"), 0.01);
    }
}
