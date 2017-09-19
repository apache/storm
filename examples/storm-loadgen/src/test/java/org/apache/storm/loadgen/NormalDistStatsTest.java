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

import org.junit.Test;

import static org.junit.Assert.*;

public class NormalDistStatsTest {
    public static void assertNDSEquals(NormalDistStats a, NormalDistStats b) {
        assertEquals("mean", a.mean, b.mean, 0.0001);
        assertEquals("min", a.min, b.min, 0.0001);
        assertEquals("max", a.max, b.max, 0.0001);
        assertEquals("stddev", a.stddev, b.stddev, 0.0001);
    }

    @Test
    public void scaleBy() throws Exception {
        NormalDistStats orig = new NormalDistStats(1.0, 0.5, 0.0, 2.0);
        assertNDSEquals(orig, orig.scaleBy(1.0));
        NormalDistStats expectedDouble = new NormalDistStats(2.0, 0.5, 1.0, 3.0);
        assertNDSEquals(expectedDouble, orig.scaleBy(2.0));
        NormalDistStats expectedHalf = new NormalDistStats(0.5, 0.5, 0.0, 1.5);
        assertNDSEquals(expectedHalf, orig.scaleBy(0.5));
    }

}