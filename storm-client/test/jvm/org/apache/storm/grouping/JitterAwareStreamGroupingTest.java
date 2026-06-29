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

package org.apache.storm.grouping;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.Config;
import org.apache.storm.executor.ChildEwmaStats;
import org.apache.storm.executor.EwmaFeedbackRecord;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.shade.com.google.common.collect.Sets;
import org.apache.storm.task.WorkerTopologyContext;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link JitterAwareStreamGrouping}: power-of-two-choices steering toward the lower-jitter
 * child once {@link ChildEwmaStats} is populated, and delegation to the embedded
 * {@link LoadAwareShuffleGrouping} whenever jitter cannot pick a winner (no feedback, or a tied pair).
 *
 * <p>With exactly two targets, P2C deterministically samples both, so the lower-jitter target always wins
 * those tests without needing to control the random source.
 */
public class JitterAwareStreamGroupingTest {

    private static final int SOURCE_TASK = 1;
    private static final List<Object> VALUES = Collections.singletonList("v");

    private Map<String, Object> createConf() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_NETWORK_TOPOGRAPHY_PLUGIN, "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping");
        conf.put(Config.TOPOLOGY_LOCALITYAWARE_HIGHER_BOUND, 0.8);
        conf.put(Config.TOPOLOGY_LOCALITYAWARE_LOWER_BOUND, 0.2);
        return conf;
    }

    /** Minimal context that satisfies the embedded {@link LoadAwareShuffleGrouping#prepare}. */
    private WorkerTopologyContext mockContext(List<Integer> availableTaskIds) {
        WorkerTopologyContext context = mock(WorkerTopologyContext.class);
        when(context.getConf()).thenReturn(createConf());
        Map<Integer, NodeInfo> taskNodeToPort = new HashMap<>();
        NodeInfo nodeInfo = new NodeInfo("node-id", Sets.newHashSet(6700L));
        availableTaskIds.forEach(e -> taskNodeToPort.put(e, nodeInfo));
        when(context.getTaskToNodePort()).thenReturn(new AtomicReference<>(taskNodeToPort));
        when(context.getAssignmentId()).thenReturn("node-id");
        when(context.getThisWorkerPort()).thenReturn(6700);
        when(context.getNodeToHost()).thenReturn(new AtomicReference<>(Collections.singletonMap("node-id", "hostname1")));
        return context;
    }

    private JitterAwareStreamGrouping prepared(List<Integer> targets, ChildEwmaStats stats) {
        JitterAwareStreamGrouping grouping = new JitterAwareStreamGrouping();
        if (stats != null) {
            grouping.registerEwmaStats(stats);
        }
        grouping.prepare(mockContext(targets), null, targets);
        return grouping;
    }

    @Test
    public void chooseTasks_emptyTargetsReturnsEmpty() {
        JitterAwareStreamGrouping grouping = prepared(Collections.emptyList(), null);
        assertTrue(grouping.chooseTasks(SOURCE_TASK, VALUES).isEmpty());
    }

    @Test
    public void chooseTasks_singleTargetAlwaysReturnsIt() {
        JitterAwareStreamGrouping grouping = prepared(Collections.singletonList(42), new ChildEwmaStats(true));
        for (int i = 0; i < 5; i++) {
            assertEquals(Collections.singletonList(42), grouping.chooseTasks(SOURCE_TASK, VALUES));
        }
    }

    @Test
    public void chooseTasks_noStatsRegistered_delegatesToFallback() {
        List<Integer> targets = Arrays.asList(10, 11, 12);
        JitterAwareStreamGrouping grouping = prepared(targets, null);
        assertDelegatedSpread(grouping, targets);
    }

    @Test
    public void chooseTasks_statsEmptyForSource_delegatesToFallback() {
        List<Integer> targets = Arrays.asList(10, 11, 12);
        JitterAwareStreamGrouping grouping = prepared(targets, new ChildEwmaStats(true));
        assertDelegatedSpread(grouping, targets);
    }

    @Test
    public void chooseTasks_steersToLowerJitterChild() {
        // Two targets => P2C samples both; the lower execute-jitter (11) always wins.
        List<Integer> targets = Arrays.asList(10, 11);
        ChildEwmaStats stats = new ChildEwmaStats(true);
        stats.update(SOURCE_TASK, 10, new EwmaFeedbackRecord(5, 5, 8.0));
        stats.update(SOURCE_TASK, 11, new EwmaFeedbackRecord(5, 5, 2.0));
        JitterAwareStreamGrouping grouping = prepared(targets, stats);

        for (int i = 0; i < 5; i++) {
            assertEquals(Collections.singletonList(11), grouping.chooseTasks(SOURCE_TASK, VALUES));
        }
    }

    @Test
    public void chooseTasks_prefersReportedOverUnreported() {
        // Only 11 has reported; with two targets it is always in the pair and beats the unreported 10.
        List<Integer> targets = Arrays.asList(10, 11);
        ChildEwmaStats stats = new ChildEwmaStats(true);
        stats.update(SOURCE_TASK, 11, new EwmaFeedbackRecord(7.0, 7.0, 7.0));
        JitterAwareStreamGrouping grouping = prepared(targets, stats);

        for (int i = 0; i < 5; i++) {
            assertEquals(Collections.singletonList(11), grouping.chooseTasks(SOURCE_TASK, VALUES));
        }
    }

    @Test
    public void chooseTasks_tiedJitter_delegatesToFallback() {
        // Equal jitter on both targets => every pair ties => the load-aware fallback decides and spreads.
        List<Integer> targets = Arrays.asList(10, 11);
        ChildEwmaStats stats = new ChildEwmaStats(true);
        stats.update(SOURCE_TASK, 10, new EwmaFeedbackRecord(5.0, 5.0, 5.0));
        stats.update(SOURCE_TASK, 11, new EwmaFeedbackRecord(5.0, 5.0, 5.0));
        JitterAwareStreamGrouping grouping = prepared(targets, stats);
        assertDelegatedSpread(grouping, targets);
    }

    @Test
    public void chooseTasks_isPerSourceTask() {
        // Two targets, source 1 only: child 10 is best; the grouping must not consult another source's stats.
        List<Integer> targets = Arrays.asList(10, 11);
        ChildEwmaStats stats = new ChildEwmaStats(true);
        stats.update(1, 10, new EwmaFeedbackRecord(1, 1, 1.0));
        stats.update(1, 11, new EwmaFeedbackRecord(1, 1, 9.0));
        JitterAwareStreamGrouping grouping = prepared(targets, stats);

        for (int i = 0; i < 5; i++) {
            assertEquals(Collections.singletonList(10), grouping.chooseTasks(1, VALUES));
        }
    }

    @Test
    public void chooseTasks_p2cAvoidsHerd() {
        // Three strictly-ordered targets, real randomness: the best (10) takes the plurality but NOT 100%
        // (herd avoided), the worst (12) never wins a pair, and 11 takes the remainder.
        List<Integer> targets = Arrays.asList(10, 11, 12);
        ChildEwmaStats stats = new ChildEwmaStats(true);
        stats.update(SOURCE_TASK, 10, new EwmaFeedbackRecord(5, 5, 2.0)); // best
        stats.update(SOURCE_TASK, 11, new EwmaFeedbackRecord(5, 5, 5.0));
        stats.update(SOURCE_TASK, 12, new EwmaFeedbackRecord(5, 5, 8.0)); // worst
        JitterAwareStreamGrouping grouping = prepared(targets, stats);

        Map<Integer, Integer> hits = new HashMap<>();
        int total = 3000;
        for (int i = 0; i < total; i++) {
            List<Integer> chosen = grouping.chooseTasks(SOURCE_TASK, VALUES);
            assertEquals(1, chosen.size());
            hits.merge(chosen.get(0), 1, Integer::sum);
        }
        int best = hits.getOrDefault(10, 0);
        int mid = hits.getOrDefault(11, 0);
        int worst = hits.getOrDefault(12, 0);
        assertTrue(best > 0 && best < total, "best target should take a share but not the whole herd: " + best);
        assertTrue(best > mid, "best target should outweigh the middle one: " + best + " vs " + mid);
        assertTrue(mid > 0, "middle target should still receive traffic: " + mid);
        // The worst target is never the lower-jitter of any sampled pair, and there are no ties to delegate.
        assertEquals(0, worst, "worst target should never win a P2C comparison");
    }

    /** Over a full sweep, every result is a single valid target and every target is used (fallback spread). */
    private void assertDelegatedSpread(JitterAwareStreamGrouping grouping, List<Integer> targets) {
        Map<Integer, Integer> hits = new HashMap<>();
        for (int i = 0; i < targets.size() * 1000; i++) {
            List<Integer> chosen = grouping.chooseTasks(SOURCE_TASK, VALUES);
            assertEquals(1, chosen.size());
            assertTrue(targets.contains(chosen.get(0)));
            hits.merge(chosen.get(0), 1, Integer::sum);
        }
        for (Integer target : targets) {
            assertTrue(hits.getOrDefault(target, 0) > 0, "target " + target + " should receive some traffic");
        }
    }
}
