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

package org.apache.storm.executor;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.metrics2.TaskMetrics;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ChildEwmaStats}: the per-source-task aggregation of downstream jitter
 * reports and the {@link ChildEwmaStats#compareByJitter} ordering used by
 * {@link org.apache.storm.grouping.JitterAwareStreamGrouping} to pick the lowest-jitter child.
 */
public class ChildEwmaStatsTest {

    private static final int PARENT = 10;
    private static final int CHILD_A = 20;
    private static final int CHILD_B = 21;

    private Map<String, Double> metrics(double execute, double process, double complete) {
        Map<String, Double> m = new HashMap<>();
        m.put(TaskMetrics.METRIC_NAME_EXECUTE_JITTER, execute);
        m.put(TaskMetrics.METRIC_NAME_PROCESS_JITTER, process);
        m.put(TaskMetrics.METRIC_NAME_COMPLETE_JITTER, complete);
        return m;
    }

    @Test
    public void disabled_updateIsNoOpAndStatsEmpty() {
        ChildEwmaStats stats = new ChildEwmaStats(false);
        stats.update(PARENT, CHILD_A, new EwmaFeedbackRecord(1, 2, 3));
        assertTrue(stats.getStats(PARENT).isEmpty());
    }

    @Test
    public void update_storesPerChildMetricsForParent() {
        ChildEwmaStats stats = new ChildEwmaStats(true);
        stats.update(PARENT, CHILD_A, new EwmaFeedbackRecord(1.0, 2.0, 3.0));

        Map<Integer, Map<String, Double>> byChild = stats.getStats(PARENT);
        assertEquals(1, byChild.size());
        Map<String, Double> child = byChild.get(CHILD_A);
        assertEquals(3.0, child.get(TaskMetrics.METRIC_NAME_EXECUTE_JITTER));
        assertEquals(1.0, child.get(TaskMetrics.METRIC_NAME_PROCESS_JITTER));
        assertEquals(2.0, child.get(TaskMetrics.METRIC_NAME_COMPLETE_JITTER));
    }

    @Test
    public void update_isIsolatedPerParentTask() {
        ChildEwmaStats stats = new ChildEwmaStats(true);
        stats.update(PARENT, CHILD_A, new EwmaFeedbackRecord(1, 2, 3));
        assertTrue(stats.getStats(999).isEmpty(), "a parent with no reports sees no stats");
    }

    @Test
    public void update_latestReportOverwritesPrevious() {
        ChildEwmaStats stats = new ChildEwmaStats(true);
        stats.update(PARENT, CHILD_A, new EwmaFeedbackRecord(1, 1, 1));
        stats.update(PARENT, CHILD_A, new EwmaFeedbackRecord(9, 9, 9));
        assertEquals(9.0, stats.getStats(PARENT).get(CHILD_A).get(TaskMetrics.METRIC_NAME_EXECUTE_JITTER));
    }

    @Test
    public void update_absentMetricNotStored() {
        // VOID-valued components are skipped by EwmaFeedbackRecord#forEachMetric, so they never reach the map.
        ChildEwmaStats stats = new ChildEwmaStats(true);
        stats.update(PARENT, CHILD_A, new EwmaFeedbackRecord(-1, -1, 5.0));
        Map<String, Double> child = stats.getStats(PARENT).get(CHILD_A);
        assertEquals(1, child.size());
        assertEquals(5.0, child.get(TaskMetrics.METRIC_NAME_EXECUTE_JITTER));
    }

    @Test
    public void compareByJitter_executeJitterWinsFirst() {
        // Lower execute-jitter is preferred regardless of the other two metrics.
        Map<String, Double> low = metrics(1.0, 9.0, 9.0);
        Map<String, Double> high = metrics(2.0, 0.0, 0.0);
        assertTrue(ChildEwmaStats.compareByJitter(low, high) < 0);
        assertTrue(ChildEwmaStats.compareByJitter(high, low) > 0);
    }

    @Test
    public void compareByJitter_fallsThroughToProcessThenComplete() {
        Map<String, Double> a = metrics(1.0, 1.0, 5.0);
        Map<String, Double> b = metrics(1.0, 2.0, 0.0);
        // tie on execute -> process decides (a wins)
        assertTrue(ChildEwmaStats.compareByJitter(a, b) < 0);

        Map<String, Double> c = metrics(1.0, 1.0, 3.0);
        Map<String, Double> d = metrics(1.0, 1.0, 4.0);
        // tie on execute and process -> complete decides (c wins)
        assertTrue(ChildEwmaStats.compareByJitter(c, d) < 0);
    }

    @Test
    public void compareByJitter_equalMetricsAreEqual() {
        assertEquals(0, ChildEwmaStats.compareByJitter(metrics(1, 2, 3), metrics(1, 2, 3)));
    }

    @Test
    public void compareByJitter_missingMetricTreatedAsWorst() {
        // An empty map (a child that has not reported) must lose to any measured value.
        Map<String, Double> measured = metrics(5.0, 5.0, 5.0);
        Map<String, Double> empty = new HashMap<>();
        assertTrue(ChildEwmaStats.compareByJitter(measured, empty) < 0);
        assertTrue(ChildEwmaStats.compareByJitter(empty, measured) > 0);
        assertEquals(0, ChildEwmaStats.compareByJitter(empty, empty));
    }
}
