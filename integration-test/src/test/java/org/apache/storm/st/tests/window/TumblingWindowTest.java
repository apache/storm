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
import org.apache.storm.st.wrapper.TopoWrap;
import org.apache.storm.st.topology.window.TumblingTimeCorrectness;
import org.apache.storm.st.topology.window.TumblingWindowCorrectness;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public final class TumblingWindowTest extends AbstractTest {
    private final WindowVerifier windowVerifier = new WindowVerifier();
    private TopoWrap topo;

    @DataProvider
    public static Object[][] generateWindows() {
        final Object[][] objects = new Object[][]{
                {-1},
                {0},
                {1},
                {10},
                {250},
                {500},
        };
        return objects;
    }

    @Test(dataProvider = "generateWindows")
    public void testTumbleCount(int tumbleSize) throws Exception {
        final TumblingWindowCorrectness testable = new TumblingWindowCorrectness(tumbleSize);
        final String topologyName = this.getClass().getSimpleName() + "-size" + tumbleSize;
        if (tumbleSize <= 0) {
            try {
                testable.newTopology();
                fail("Expected IllegalArgumentException was not thrown.");
            } catch (IllegalArgumentException ignore) {
                return;
            }
        }
        topo = new TopoWrap(cluster, topologyName, testable.newTopology());
        windowVerifier.runAndVerifyCount(tumbleSize, tumbleSize, testable, topo);
    }

    @DataProvider
    public static Object[][] generateTumbleTimes() {
        final Object[][] objects = new Object[][]{
                {-1},
                {0},
                {1},
                {2},
                {5},
                {10},
        };
        return objects;
    }

    @Test(dataProvider = "generateTumbleTimes")
    public void testTumbleTime(int tumbleSec) throws Exception {
        final TumblingTimeCorrectness testable = new TumblingTimeCorrectness(tumbleSec);
        final String topologyName = this.getClass().getSimpleName() + "-sec" + tumbleSec;
        if (tumbleSec <= 0) {
            assertThrows(IllegalArgumentException.class, () -> testable.newTopology());
        }
        topo = new TopoWrap(cluster, topologyName, testable.newTopology());
        windowVerifier.runAndVerifyTime(tumbleSec, tumbleSec, testable, topo);
    }

    @AfterMethod
    public void cleanup() throws Exception {
        if (topo != null) {
            topo.killOrThrow();
            topo = null;
        }
    }
}
