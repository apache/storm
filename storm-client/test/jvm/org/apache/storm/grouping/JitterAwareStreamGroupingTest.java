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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.executor.ChildEwmaStats;
import org.apache.storm.executor.EwmaFeedbackRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link JitterAwareStreamGrouping}: round-robin fallback before feedback data exists,
 * and lowest-jitter steering once {@link ChildEwmaStats} is populated for the emitting source task.
 */
public class JitterAwareStreamGroupingTest {

    private static final int SOURCE_TASK = 1;
    private static final List<Object> VALUES = Collections.singletonList("v");

    private JitterAwareStreamGrouping prepared(List<Integer> targets, ChildEwmaStats stats) {
        JitterAwareStreamGrouping grouping = new JitterAwareStreamGrouping();
        if (stats != null) {
            grouping.registerEwmaStats(stats);
        }
        // context is unused by prepare(); only the target task list is retained.
        grouping.prepare(null, null, targets);
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
    public void chooseTasks_noStatsRegistered_roundRobins() {
        List<Integer> targets = Arrays.asList(10, 11, 12);
        JitterAwareStreamGrouping grouping = prepared(targets, null);
        assertRoundRobin(grouping, targets);
    }

    @Test
    public void chooseTasks_statsEmptyForSource_roundRobins() {
        List<Integer> targets = Arrays.asList(10, 11, 12);
        JitterAwareStreamGrouping grouping = prepared(targets, new ChildEwmaStats(true));
        assertRoundRobin(grouping, targets);
    }

    @Test
    public void chooseTasks_noReportedTargetForSource_roundRobins() {
        // The source task has feedback, but only from children that are not among the current targets.
        List<Integer> targets = Arrays.asList(10, 11);
        ChildEwmaStats stats = new ChildEwmaStats(true);
        stats.update(SOURCE_TASK, 99, new EwmaFeedbackRecord(1, 1, 1));
        JitterAwareStreamGrouping grouping = prepared(targets, stats);
        assertRoundRobin(grouping, targets);
    }

    @Test
    public void chooseTasks_steersToLowestJitterChild() {
        List<Integer> targets = Arrays.asList(10, 11, 12);
        ChildEwmaStats stats = new ChildEwmaStats(true);
        // execute-jitter is the primary key: task 11 is the lowest.
        stats.update(SOURCE_TASK, 10, new EwmaFeedbackRecord(5, 5, 8.0));
        stats.update(SOURCE_TASK, 11, new EwmaFeedbackRecord(5, 5, 2.0));
        stats.update(SOURCE_TASK, 12, new EwmaFeedbackRecord(5, 5, 6.0));
        JitterAwareStreamGrouping grouping = prepared(targets, stats);

        for (int i = 0; i < 5; i++) {
            assertEquals(Collections.singletonList(11), grouping.chooseTasks(SOURCE_TASK, VALUES));
        }
    }

    @Test
    public void chooseTasks_partialData_prefersReportedOverUnreported() {
        // Only task 12 has reported; 10 and 11 are unreported and must be treated as worst.
        List<Integer> targets = Arrays.asList(10, 11, 12);
        ChildEwmaStats stats = new ChildEwmaStats(true);
        stats.update(SOURCE_TASK, 12, new EwmaFeedbackRecord(7.0, 7.0, 7.0));
        JitterAwareStreamGrouping grouping = prepared(targets, stats);

        assertEquals(Collections.singletonList(12), grouping.chooseTasks(SOURCE_TASK, VALUES));
    }

    @Test
    public void chooseTasks_isPerSourceTask() {
        List<Integer> targets = Arrays.asList(10, 11);
        ChildEwmaStats stats = new ChildEwmaStats(true);
        // For source task 1, child 10 is best; the grouping must not consult another source's stats.
        stats.update(1, 10, new EwmaFeedbackRecord(1, 1, 1.0));
        stats.update(1, 11, new EwmaFeedbackRecord(1, 1, 9.0));
        JitterAwareStreamGrouping grouping = prepared(targets, stats);

        assertEquals(Collections.singletonList(10), grouping.chooseTasks(1, VALUES));
    }

    private void assertRoundRobin(JitterAwareStreamGrouping grouping, List<Integer> targets) {
        // Over a full cycle, every target is selected exactly once (even spread, no pinning).
        Map<Integer, Integer> hits = new HashMap<>();
        int rounds = 4;
        for (int i = 0; i < targets.size() * rounds; i++) {
            List<Integer> chosen = grouping.chooseTasks(SOURCE_TASK, VALUES);
            assertEquals(1, chosen.size());
            assertTrue(targets.contains(chosen.get(0)));
            hits.merge(chosen.get(0), 1, Integer::sum);
        }
        for (Integer target : targets) {
            assertEquals(rounds, hits.get(target), "target " + target + " should get an even share");
        }
        // sanity: all distinct targets were used
        assertEquals(new ArrayList<>(targets).size(), hits.size());
    }
}
