/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.st.tests.window;

import org.apache.storm.st.helper.AbstractTest;
import org.apache.storm.st.topology.window.SlidingTimeCorrectness;
import org.apache.storm.st.topology.window.SlidingWindowCorrectness;
import org.apache.storm.st.wrapper.TopoWrap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.junit.jupiter.api.Assertions.fail;

public final class SlidingWindowTest extends AbstractTest {
    private final WindowVerifier windowVerifier = new WindowVerifier();
    private TopoWrap topo;

    @DataProvider
    public static Object[][] generateCountWindows() {
        return new Object[][]{
                {-1, 10},
                {10, -1},
                {0, 10},
                {10, 0},
                {0, 0},
                {-1, -1},
                {5, 10},
                {1, 1},
                {10, 5},
                {100, 10},
                {100, 100},
                {200, 100},
                {500, 100},
        };
    }

    @Test(dataProvider = "generateCountWindows")
    public void testWindowCount(int windowSize, int slideSize) throws Exception {
        final SlidingWindowCorrectness testable = new SlidingWindowCorrectness(windowSize, slideSize);
        final String topologyName = this.getClass().getSimpleName() + "-size-window" + windowSize + "-slide" + slideSize;
        if (windowSize <= 0 || slideSize <= 0) {
            try {
                testable.newTopology();
                fail("Expected IllegalArgumentException was not thrown.");
            } catch (IllegalArgumentException ignore) {
                return;
            }
        }
        topo = new TopoWrap(cluster, topologyName, testable.newTopology());
        windowVerifier.runAndVerifyCount(windowSize, slideSize, testable, topo);
    }

    @DataProvider
    public static Object[][] generateTimeWindows() {
        return new Object[][]{
                {-1, 10},
                {10, -1},
                {0, 10},
                {10, 0},
                {0, 0},
                {-1, -1},
                {1, 1},
                {5, 2},
                {2, 5},
                {20, 5},
                {20, 10},
        };
    }

    @Test(dataProvider = "generateTimeWindows")
    public void testTimeWindow(int windowSec, int slideSec) throws Exception {
        final SlidingTimeCorrectness testable = new SlidingTimeCorrectness(windowSec, slideSec);
        final String topologyName = this.getClass().getSimpleName() + "-sec-window" + windowSec + "-slide" + slideSec;
        if (windowSec <= 0 || slideSec <= 0) {
            try {
                testable.newTopology();
                fail("Expected IllegalArgumentException was not thrown.");
            } catch (IllegalArgumentException ignore) {
                return;
            }
        }
        topo = new TopoWrap(cluster, topologyName, testable.newTopology());
        windowVerifier.runAndVerifyTime(windowSec, slideSec, testable, topo);
    }

    @AfterMethod
    public void cleanup() throws Exception {
        if (topo != null) {
            topo.killOrThrow();
            topo = null;
        }
    }
}
