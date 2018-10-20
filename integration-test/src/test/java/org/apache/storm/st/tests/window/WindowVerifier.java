/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.st.tests.window;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.storm.st.topology.TestableTopology;
import org.apache.storm.st.topology.window.data.TimeData;
import org.apache.storm.st.topology.window.data.TimeDataWindow;
import org.apache.storm.st.utils.StringDecorator;
import org.apache.storm.st.utils.TimeUtil;
import org.apache.storm.st.wrapper.DecoratedLogLine;
import org.apache.storm.st.wrapper.TopoWrap;
import org.apache.storm.thrift.TException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

public class WindowVerifier {

    public static final Logger LOG = LoggerFactory.getLogger(WindowVerifier.class);
    
    /**
     * Run the topology and verify that the number and contents of count based windows is as expected
     * once the spout and bolt have emitted sufficient tuples.
     * The spout and bolt are required to log exactly one log line per emit/window using {@link StringDecorator}
     */
    public void runAndVerifyCount(int windowSize, int slideSize, TestableTopology testable, TopoWrap topo) throws IOException, TException, MalformedURLException {
        topo.submitSuccessfully();
        final int minBoltEmits = 5;
        //Sliding windows should produce one window every slideSize tuples
        //Wait for the spout to emit at least enough tuples to get minBoltEmit windows and at least one full window
        final int minSpoutEmits = Math.max(windowSize, minBoltEmits * slideSize);
        
        String boltName = testable.getBoltName();
        String spoutName = testable.getSpoutName();
        //Waiting for spout tuples isn't strictly necessary since we also wait for bolt emits, but do it anyway
        topo.assertProgress(minSpoutEmits, testable.getSpoutExecutors(), spoutName, 180);
        topo.assertProgress(minBoltEmits, testable.getBoltExecutors(), boltName, 180);
        
        final List<DecoratedLogLine> allDecoratedBoltLogs = topo.getDecoratedLogLines(boltName);
        final List<DecoratedLogLine> allDecoratedSpoutLogs = topo.getDecoratedLogLines(spoutName);
        //We expect the bolt to log exactly one decorated line per emit
        Assert.assertTrue(allDecoratedBoltLogs.size() >= minBoltEmits,
                "Expecting min " + minBoltEmits + " bolt emits, found: " + allDecoratedBoltLogs.size() + " \n\t" + allDecoratedBoltLogs);
        
        final int numberOfWindows = allDecoratedBoltLogs.size();
        for(int i = 0; i < numberOfWindows; ++i ) {
            LOG.info("Comparing window: " + (i + 1) + " of " + numberOfWindows);
            final int toIndex = (i + 1) * slideSize;
            final int fromIndex = toIndex - windowSize;
            final int positiveFromIndex = fromIndex > 0 ? fromIndex : 0;
            final List<DecoratedLogLine> expectedWindowContents = allDecoratedSpoutLogs.subList(positiveFromIndex, toIndex);
            final String actualString = allDecoratedBoltLogs.get(i).toString();
            for (DecoratedLogLine windowData : expectedWindowContents) {
                final String logStr = windowData.getData();
                Assertions.assertTrue(actualString.contains(logStr),
                        () -> String.format("Missing: '%s' \nActual: '%s' \nCalculated window: '%s'", logStr, actualString, expectedWindowContents));
            }
        }
    }
    
    /**
     * Run the topology and verify that the number and contents of time based windows is as expected
     * once the spout and bolt have emitted sufficient tuples.
     * The spout and bolt are required to log exactly one log line per emit/window using {@link StringDecorator}
     */
    public void runAndVerifyTime(int windowSec, int slideSec, TestableTopology testable, TopoWrap topo) throws IOException, TException, java.net.MalformedURLException {
        topo.submitSuccessfully();
        final int minSpoutEmits = 100;
        final int minBoltEmits = 5;
        
        String boltName = testable.getBoltName();
        String spoutName = testable.getSpoutName();
        
        //Waiting for spout tuples isn't strictly necessary since we also wait for bolt emits, but do it anyway
        //Allow two minutes for topology startup, then wait for at most the time it should take to produce 10 windows
        topo.assertProgress(minSpoutEmits, testable.getSpoutExecutors(), spoutName, 180 + 10 * slideSec);
        topo.assertProgress(minBoltEmits, testable.getBoltExecutors(), boltName, 180 + 10 * slideSec);
        
        final List<TimeData> allSpoutLogLines = topo.getDeserializedDecoratedLogLines(spoutName, TimeData::fromJson);
        final List<TimeDataWindow> allBoltLogLines = topo.getDeserializedDecoratedLogLines(boltName, TimeDataWindow::fromJson);
        Assert.assertTrue(allBoltLogLines.size() >= minBoltEmits,
                "Expecting min " + minBoltEmits + " bolt emits, found: " + allBoltLogLines.size() + " \n\t" + allBoltLogLines);
        
        final DateTime firstWindowEndTime = TimeUtil.ceil(new DateTime(allSpoutLogLines.get(0).getDate()).withZone(DateTimeZone.UTC), slideSec);
        final int numberOfWindows = allBoltLogLines.size();
        /*
         * Windows should be aligned to the slide size, starting at firstWindowEndTime - windowSec.
         * Because all windows are aligned to the slide size, we can partition the spout emitted timestamps by which window they should fall in.
         * This checks that the partitioned spout emits fall in the expected windows, based on the logs from the spout and bolt.
         */
        for (int i = 0; i < numberOfWindows; ++i) {
            final DateTime windowEnd = firstWindowEndTime.plusSeconds(i * slideSec);
            final DateTime  windowStart =  windowEnd.minusSeconds(windowSec);
            LOG.info("Comparing window: " + windowStart + " to " + windowEnd + " iter " + (i+1) + "/" + numberOfWindows);
            
            final List<TimeData> expectedSpoutEmitsInWindow = allSpoutLogLines.stream()
                .filter(spoutLog -> {
                    DateTime spoutLogTime = new DateTime(spoutLog.getDate());
                    //The window boundaries are )windowStart, windowEnd)
                    return spoutLogTime.isAfter(windowStart) && spoutLogTime.isBefore(windowEnd.plusMillis(1));
                }).collect(Collectors.toList());
            TimeDataWindow expectedWindow = new TimeDataWindow(expectedSpoutEmitsInWindow);
            
            final TimeDataWindow actualWindow = allBoltLogLines.get(i);
            LOG.info("Actual window: " + actualWindow.getDescription());
            LOG.info("Expected window: " + expectedWindow.getDescription());
            for (TimeData oneLog : expectedWindow.getTimeData()) {
                Assertions.assertTrue(actualWindow.getTimeData().contains(oneLog),
                        () -> String.format("Missing: '%s' \n\tActual: '%s' \n\tComputed window: '%s'", oneLog, actualWindow, expectedWindow));
            }
            for (TimeData oneLog : actualWindow.getTimeData()) {
                Assertions.assertTrue(expectedWindow.getTimeData().contains(oneLog),
                        () -> String.format("Extra: '%s' \n\tActual: '%s' \n\tComputed window: '%s'", oneLog, actualWindow, expectedWindow));
            }
        }
    }
    
}
