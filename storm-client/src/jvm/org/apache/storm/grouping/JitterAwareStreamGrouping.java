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
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.storm.executor.ChildEwmaStats;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.task.WorkerTopologyContext;

/**
 * A {@link LoadAwareCustomStreamGrouping} that steers each tuple toward the downstream (child) task with
 * lower jitter, as reported back to the emitting task through upstream feedback and aggregated by
 * {@link ChildEwmaStats}. Jitter is compared with {@link ChildEwmaStats#compareByJitter}, so a lower
 * {@code __execute-jitter} wins first, then {@code __process-jitter}, then {@code __complete-jitter}.
 *
 * <p>Steering uses <b>power-of-two-choices</b>: for each tuple two target tasks are sampled at random and
 * the lower-jitter one wins. Random sampling keeps the best task from receiving every tuple (the
 * "thundering herd" a plain arg-min selection would cause) while still biasing traffic toward the good
 * tasks.
 *
 * <p>Whenever jitter cannot pick a winner — the sampled pair ties (equal jitter, or neither has reported),
 * no feedback exists yet for the source task, or no {@link ChildEwmaStats} was registered — the decision is
 * delegated to an embedded {@link LoadAwareShuffleGrouping}. When <i>all</i> targets carry equal jitter,
 * every sampled pair ties, so the grouping behaves as a pure load-aware shuffle. {@link #refreshLoad} is
 * forwarded to that delegate, so the fallback path honours real system load and locality.
 */
public class JitterAwareStreamGrouping implements LoadAwareCustomStreamGrouping {

    private final LoadAwareShuffleGrouping fallback = new LoadAwareShuffleGrouping();
    private List<Integer> targetTasks;
    private ChildEwmaStats stats;

    // deterministic test
    @VisibleForTesting
    Random random;

    @Override
    public void refreshLoad(LoadMapping loadMapping) {
        fallback.refreshLoad(loadMapping);
    }

    @Override
    public void registerEwmaStats(ChildEwmaStats childEwmaStats) {
        this.stats = childEwmaStats;
    }

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks;
        // The fallback rejects an empty target list; chooseTasks short-circuits that case before delegating.
        if (targetTasks != null && !targetTasks.isEmpty()) {
            fallback.prepare(context, stream, targetTasks);
        }
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
            return fallback.chooseTasks(taskId, values);
        }

        Map<Integer, Map<String, Double>> childStats = stats.getStats(taskId);
        if (childStats.isEmpty()) {
            return fallback.chooseTasks(taskId, values);
        }

        // Power-of-two-choices: sample two distinct targets and keep the lower-jitter one.
        int n = targetTasks.size();
        int i = nextInt(n);
        int j = nextInt(n - 1);
        if (j >= i) {
            j++;
        }
        Integer a = targetTasks.get(i);
        Integer b = targetTasks.get(j);

        // An unreported target is an empty map, which compareByJitter treats as worst.
        Map<String, Double> metricsA = childStats.getOrDefault(a, Collections.emptyMap());
        Map<String, Double> metricsB = childStats.getOrDefault(b, Collections.emptyMap());
        int cmp = ChildEwmaStats.compareByJitter(metricsA, metricsB);
        if (cmp < 0) {
            return Collections.singletonList(a);
        }
        if (cmp > 0) {
            return Collections.singletonList(b);
        }
        // Tie (equal jitter, or both unreported): no jitter winner -> defer to the load-aware fallback.
        return fallback.chooseTasks(taskId, values);
    }

    private int nextInt(int bound) {
        return random != null ? random.nextInt(bound) : ThreadLocalRandom.current().nextInt(bound);
    }

}
