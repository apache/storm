/*
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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.storm.metrics2.TaskMetrics;

/**
 * Thread-safe store of EWMA jitter statistics reported by downstream (child) tasks back to a parent task.
 * The data is indexed by parent {@code taskId} so a lookup touches only that task's children:
 * {@link #getStats} is an O(1) map lookup and {@link #update} is O(metrics), neither scanning the whole
 * store. This keeps cost bound to a single task's child fan-out, independent of how many tasks the
 * executor hosts.
 */
public class ChildEwmaStats {

    private final boolean enabled;
    private final Map<Integer, ConcurrentHashMap<Integer, Map<String, Double>>> byTask;

    private static final String[] JITTER_PRIORITY = {
        TaskMetrics.METRIC_NAME_EXECUTE_JITTER,
        TaskMetrics.METRIC_NAME_PROCESS_JITTER,
        TaskMetrics.METRIC_NAME_COMPLETE_JITTER,
    };

    public ChildEwmaStats(boolean enabled) {
        this.enabled = enabled;
        this.byTask = enabled ? new ConcurrentHashMap<>() : Collections.emptyMap();
    }

    /**
     * Records the jitter metrics reported by {@code childTaskId} for the given parent {@code taskId}.
     * Runs in O(metrics) by writing straight into the task's bucket; no rescanning of existing data.
     */
    public void update(int taskId, int childTaskId, EwmaFeedbackRecord feedback) {
        if (!enabled) {
            return;
        }
        ConcurrentHashMap<Integer, Map<String, Double>> children =
            byTask.computeIfAbsent(taskId, k -> new ConcurrentHashMap<>());
        Map<String, Double> metrics = children.computeIfAbsent(childTaskId, k -> new ConcurrentHashMap<>());
        feedback.forEachMetric(metrics::put);
    }

    /**
     * Returns the latest reported value of each metric, per child task, for the given source
     * {@code taskId} as {@code childTaskId -> (metricName -> value)}.
     */
    public Map<Integer, Map<String, Double>> getStats(int taskId) {
        if (!enabled) {
            return Collections.emptyMap();
        }
        Map<Integer, Map<String, Double>> children = byTask.get(taskId);
        return children == null ? Collections.emptyMap() : children;
    }

    /**
     * Compares two stats maps following {@link #JITTER_PRIORITY}, ascending. A missing metric
     * is treated as {@link Double#MAX_VALUE} so it loses to any measured value.
     */
    public static int compareByJitter(Map<String, Double> a, Map<String, Double> b) {
        for (String metric : JITTER_PRIORITY) {
            int cmp = Double.compare(
                a.getOrDefault(metric, Double.MAX_VALUE),
                b.getOrDefault(metric, Double.MAX_VALUE));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }
}
