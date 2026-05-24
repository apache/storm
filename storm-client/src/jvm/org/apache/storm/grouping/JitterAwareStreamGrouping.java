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

package org.apache.storm.grouping;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.storm.executor.ChildEwmaStats;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.WorkerTopologyContext;

/**
 * A {@link CustomStreamGrouping} that routes each tuple to the downstream (child) task with the lowest
 * jitter, as reported back to the emitting task through upstream feedback and aggregated by
 * {@link ChildEwmaStats}. Candidates are ordered with {@link ChildEwmaStats#compareByJitter}, so a lower
 * {@code __execute-jitter} wins first, then {@code __process-jitter}, then {@code __complete-jitter}.
 * Until a source task has any feedback data — and for targets that have not reported yet — the grouping
 * falls back to round-robin over the target tasks, so it degrades to an even spread rather than pinning a
 * single task.
 */
public class JitterAwareStreamGrouping implements LoadAwareCustomStreamGrouping {

    private final AtomicInteger roundRobin = new AtomicInteger();
    private List<Integer> targetTasks;
    private ChildEwmaStats stats;

    @Override
    public void refreshLoad(LoadMapping loadMapping) {
        // load mapping agnostic
    }

    @Override
    public void registerEwmaStats(ChildEwmaStats childEwmaStats) {
        this.stats = childEwmaStats;
    }

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks;
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        if (targetTasks == null || targetTasks.isEmpty()) {
            return Collections.emptyList();
        }
        if (targetTasks.size() == 1) {
            return targetTasks;
        }

        if (stats == null) {
            return roundRobin();
        }

        // childTaskId -> (metricName -> averaged value), as reported back to this source task.
        Map<Integer, Map<String, Double>> childStats = stats.getStats(taskId);
        if (childStats.isEmpty()) {
            return roundRobin();
        }

        Integer best = null;
        Map<String, Double> bestMetrics = null;
        boolean anyData = false;
        for (Integer target : targetTasks) {
            Map<String, Double> metrics = childStats.get(target);
            if (metrics != null && !metrics.isEmpty()) {
                anyData = true;
            } else {
                // A target with no feedback yet is treated as worst by compareByJitter (empty map).
                metrics = Collections.emptyMap();
            }
            if (best == null || ChildEwmaStats.compareByJitter(metrics, bestMetrics) < 0) {
                best = target;
                bestMetrics = metrics;
            }
        }

        if (!anyData) {
            // No target has reported for this source task yet: spread evenly instead of pinning the first.
            return roundRobin();
        }
        return Collections.singletonList(best);
    }

    private List<Integer> roundRobin() {
        int index = Math.floorMod(roundRobin.getAndIncrement(), targetTasks.size());
        return Collections.singletonList(targetTasks.get(index));
    }

}
