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

package org.apache.storm.metric;

import java.util.Collection;
import java.util.Collections;
import org.apache.storm.metric.api.DataPoint;
import org.apache.storm.metric.api.IClusterMetricsConsumer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class ClusterMetricsConsumerExecutorTest {

    @Before
    public void setUp() {
        MockFailingClusterMetricsConsumer.resetAllCounts();
    }

    @Test
    public void testPrepareDoesNotThrowExceptionWhenInitializingClusterMetricsConsumerIsFailing() throws Exception {
        ClusterMetricsConsumerExecutor sut = new ClusterMetricsConsumerExecutor(
            MockFailingClusterMetricsConsumer.class.getName(), 2);

        // it shouldn't propagate any exceptions
        sut.prepare();
        sut.prepare();

        Assert.assertEquals(2, MockFailingClusterMetricsConsumer.getPrepareCallCount());
    }

    @Test
    public void testHandleDataPointsWithClusterMetricsShouldSkipHandlingMetricsIfFailedBefore() throws Exception {
        ClusterMetricsConsumerExecutor sut = new ClusterMetricsConsumerExecutor(
            MockFailingClusterMetricsConsumer.class.getName(), 2);

        // below calls shouldn't propagate any exceptions
        sut.prepare();

        // no specific reason to mock... this is one of easiest ways to make dummy instance
        sut.handleDataPoints(mock(IClusterMetricsConsumer.ClusterInfo.class), Collections.emptyList());

        Assert.assertEquals(1, MockFailingClusterMetricsConsumer.getPrepareCallCount());
        Assert.assertEquals(0, MockFailingClusterMetricsConsumer.getHandleDataPointsWithClusterInfoCallCount());
    }

    @Test
    public void testHandleDataPointsWithSupervisorMetricsShouldRetryInitializingClusterMetricsConsumerIfFailedBefore() throws Exception {
        ClusterMetricsConsumerExecutor sut = new ClusterMetricsConsumerExecutor(
            MockFailingClusterMetricsConsumer.class.getName(), 2);

        // below calls shouldn't propagate any exceptions
        sut.prepare();

        // no specific reason to mock... this is one of easiest ways to make dummy instance
        sut.handleDataPoints(mock(IClusterMetricsConsumer.SupervisorInfo.class), Collections.emptyList());

        Assert.assertEquals(1, MockFailingClusterMetricsConsumer.getPrepareCallCount());
        Assert.assertEquals(0, MockFailingClusterMetricsConsumer.getHandleDataPointsWithSupervisorInfoCallCount());
    }

    public static class MockFailingClusterMetricsConsumer implements IClusterMetricsConsumer {
        private static int prepareCallCount = 0;
        private static int handleDataPointsWithClusterInfoCallCount = 0;
        private static int handleDataPointsWithSupervisorInfoCallCount = 0;
        private static int cleanupCallCount = 0;

        public static int getPrepareCallCount() {
            return prepareCallCount;
        }

        public static int getHandleDataPointsWithClusterInfoCallCount() {
            return handleDataPointsWithClusterInfoCallCount;
        }

        public static int getHandleDataPointsWithSupervisorInfoCallCount() {
            return handleDataPointsWithSupervisorInfoCallCount;
        }

        public static int getCleanupCallCount() {
            return cleanupCallCount;
        }

        public static void resetAllCounts() {
            prepareCallCount = 0;
            handleDataPointsWithClusterInfoCallCount = 0;
            handleDataPointsWithSupervisorInfoCallCount = 0;
            cleanupCallCount = 0;
        }

        @Override
        public void prepare(Object registrationArgument) {
            prepareCallCount++;

            throw new RuntimeException("prepare failing...");
        }

        @Override
        public void handleDataPoints(ClusterInfo clusterInfo, Collection<DataPoint> dataPoints) {
            handleDataPointsWithClusterInfoCallCount++;
        }

        @Override
        public void handleDataPoints(SupervisorInfo supervisorInfo, Collection<DataPoint> dataPoints) {
            handleDataPointsWithSupervisorInfoCallCount++;
        }

        @Override
        public void cleanup() {
            cleanupCallCount++;
        }
    }

}
