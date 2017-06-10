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

import java.io.IOException;
import org.apache.storm.st.helper.AbstractTest;
import org.apache.storm.st.wrapper.LogData;
import org.apache.storm.st.wrapper.TopoWrap;
import org.apache.thrift.TException;
import org.apache.storm.st.topology.TestableTopology;
import org.apache.storm.st.topology.window.SlidingTimeCorrectness;
import org.apache.storm.st.topology.window.SlidingWindowCorrectness;
import org.apache.storm.st.topology.window.data.TimeData;
import org.apache.storm.st.topology.window.data.TimeDataWindow;
import org.apache.storm.st.utils.TimeUtil;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.MalformedURLException;
import java.util.List;

public final class SlidingWindowTest extends AbstractTest {
    private static Logger log = LoggerFactory.getLogger(SlidingWindowTest.class);
    private TopoWrap topo;

    @DataProvider
    public static Object[][] generateCountWindows() {
        final Object[][] objects = new Object[][]{
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
        return objects;
    }

    @Test(dataProvider = "generateCountWindows")
    public void testWindowCount(int windowSize, int slideSize) throws Exception {
        final SlidingWindowCorrectness testable = new SlidingWindowCorrectness(windowSize, slideSize);
        final String topologyName = this.getClass().getSimpleName() + "w" + windowSize + "s" + slideSize;
        if (windowSize <= 0 || slideSize <= 0) {
            try {
                testable.newTopology();
                Assert.fail("Expected IllegalArgumentException was not thrown.");
            } catch (IllegalArgumentException ignore) {
                return;
            }
        }
        topo = new TopoWrap(cluster, topologyName, testable.newTopology());
        runAndVerifyCount(windowSize, slideSize, testable, topo);
    }

    static void runAndVerifyCount(int windowSize, int slideSize, TestableTopology testable, TopoWrap topo) throws IOException, TException, MalformedURLException {
        topo.submitSuccessfully();
        final int minBoltEmits = 5;
        //Sliding windows should produce one window every slideSize tuples
        //Wait for the spout to emit at least enough tuples to get minBoltEmit windows and at least one full window
        final int minSpoutEmits = Math.max(windowSize, minBoltEmits * slideSize);
        
        String boltName = testable.getBoltName();
        String spoutName = testable.getSpoutName();
        //Waiting for spout tuples isn't strictly necessary since we also wait for bolt emits, but do it anyway
        topo.assertProgress(minSpoutEmits, spoutName, 180);
        topo.assertProgress(minBoltEmits, boltName, 180);
        List<TopoWrap.ExecutorURL> boltUrls = topo.getLogUrls(boltName);
        log.info(boltUrls.toString());
        final List<LogData> allBoltData = topo.getLogData(boltName);
        final List<LogData> allSpoutData = topo.getLogData(spoutName);
        Assert.assertTrue(allBoltData.size() >= minBoltEmits,
                "Expecting min " + minBoltEmits + " bolt emits, found: " + allBoltData.size() + " \n\t" + allBoltData);
        final int numberOfWindows = allBoltData.size();
        for(int i = 0; i < numberOfWindows; ++i ) {
            log.info("Comparing window: " + (i + 1) + " of " + numberOfWindows);
            final int toIndex = (i + 1) * slideSize;
            final int fromIndex = toIndex - windowSize;
            final int positiveFromIndex = fromIndex > 0 ? fromIndex : 0;
            final List<LogData> windowData = allSpoutData.subList(positiveFromIndex, toIndex);
            final String actualString = allBoltData.get(i).toString();
            for (LogData oneLog : windowData) {
                final String logStr = oneLog.getData();
                Assert.assertTrue(actualString.contains(logStr),
                        String.format("Missing: '%s' \nActual: '%s' \nCalculated window: '%s'", logStr, actualString, windowData));
            }
        }
    }

    @DataProvider
    public static Object[][] generateTimeWindows() {
        final Object[][] objects = new Object[][]{
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
        return objects;
    }

    @Test(dataProvider = "generateTimeWindows")
    public void testTimeWindow(int windowSec, int slideSec) throws Exception {
        final SlidingTimeCorrectness testable = new SlidingTimeCorrectness(windowSec, slideSec);
        final String topologyName = this.getClass().getSimpleName() + "w" + windowSec + "s" + slideSec;
        if (windowSec <= 0 || slideSec <= 0) {
            try {
                testable.newTopology();
                Assert.fail("Expected IllegalArgumentException was not thrown.");
            } catch (IllegalArgumentException ignore) {
                return;
            }
        }
        topo = new TopoWrap(cluster, topologyName, testable.newTopology());
        runAndVerifyTime(windowSec, slideSec, testable, topo);
    }

    static void runAndVerifyTime(int windowSec, int slideSec, TestableTopology testable, TopoWrap topo) throws IOException, TException, java.net.MalformedURLException {
        topo.submitSuccessfully();
        final int minSpoutEmits = 100;
        final int minBoltEmits = 5;
        String boltName = testable.getBoltName();
        String spoutName = testable.getSpoutName();
        //Waiting for spout tuples isn't strictly necessary since we also wait for bolt emits, but do it anyway
        topo.assertProgress(minSpoutEmits, spoutName, 60 + 10 * (windowSec + slideSec));
        topo.assertProgress(minBoltEmits, boltName, 60 + 10 * (windowSec + slideSec));
        final List<TimeData> allSpoutDataDeserialized = topo.getLogData(spoutName, TimeData.CLS);
        final List<LogData> allBoltData = topo.getLogData(boltName);
        final List<TimeDataWindow> allBoltDataDeserialized = topo.deserializeLogData(allBoltData, TimeDataWindow.CLS);
        Assert.assertTrue(allBoltData.size() >= minBoltEmits,
                "Expecting min " + minBoltEmits + " bolt emits, found: " + allBoltData.size() + " \n\t" + allBoltData);
        final DateTime firstEndTime = TimeUtil.ceil(new DateTime(allSpoutDataDeserialized.get(0).getDate()).withZone(DateTimeZone.UTC), slideSec);
        final int numberOfWindows = allBoltData.size();
        for(int i = 0; i < numberOfWindows; ++i ) {
            final DateTime toDate = firstEndTime.plusSeconds(i * slideSec);
            final DateTime  fromDate =  toDate.minusSeconds(windowSec);
            log.info("Comparing window: " + fromDate + " to " + toDate + " iter " + (i+1) + "/" + numberOfWindows);
            final TimeDataWindow computedWindow = TimeDataWindow.newInstance(allSpoutDataDeserialized,fromDate, toDate);
            final LogData oneBoltLog = allBoltData.get(i);
            final TimeDataWindow actualWindow = allBoltDataDeserialized.get(i);
            log.info("Actual window: " + actualWindow.getDescription());
            log.info("Computed window: " + computedWindow.getDescription());
            for (TimeData oneLog : computedWindow) {
                Assert.assertTrue(actualWindow.contains(oneLog),
                        String.format("Missing: '%s' \n\tActual: '%s' \n\tComputed window: '%s'", oneLog, oneBoltLog, computedWindow));
            }
            for (TimeData oneLog : actualWindow) {
                Assert.assertTrue(computedWindow.contains(oneLog),
                        String.format("Extra: '%s' \n\tActual: '%s' \n\tComputed window: '%s'", oneLog, oneBoltLog, computedWindow));
            }
        }
    }

    @AfterMethod
    public void cleanup() throws Exception {
        if (topo != null) {
            topo.killQuietly();
        }
    }
}
