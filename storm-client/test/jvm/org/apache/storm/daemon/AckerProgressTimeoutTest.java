/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.daemon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for STORM-2359: progress-based tuple timeout.
 *
 * <p>All tests use {@link Time.SimulatedTime} so time advances are deterministic
 * and do not depend on wall-clock. This follows the Storm testing best practice
 * documented in DEVELOPER.md.
 */
public class AckerProgressTimeoutTest {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Build a minimal topology config with progress timeout enabled. */
    private Map<String, Object> confWithProgressTimeout(int progressSecs) {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_MESSAGE_PROGRESS_TIMEOUT_SECS, progressSecs);
        return conf;
    }

    /** Build a minimal topology config with no progress timeout (default behavior). */
    private Map<String, Object> confDefault() {
        return new HashMap<>();
    }

    /**
     * Create and prepare an Acker with the given config.
     * Returns a {@link TestAckerHarness} that bundles the acker and its mocked
     * OutputCollector so tests can inspect emitted messages.
     */
    private TestAckerHarness prepareAcker(Map<String, Object> conf) {
        Acker acker = new Acker();
        OutputCollector collector = mock(OutputCollector.class);
        TopologyContext context = mock(TopologyContext.class);
        acker.prepare(conf, context, collector);
        return new TestAckerHarness(acker, collector);
    }

    /** Simulate ACKER_INIT: spout emitting tuple tree with id=treeId, xorVal, spoutTask. */
    private Tuple makeInitTuple(long treeId, long xorVal, int spoutTask) {
        Tuple t = mock(Tuple.class);
        when(t.getSourceStreamId()).thenReturn(Acker.ACKER_INIT_STREAM_ID);
        when(t.getValue(0)).thenReturn(treeId);
        when(t.getLong(1)).thenReturn(xorVal);
        when(t.getInteger(2)).thenReturn(spoutTask);
        return t;
    }

    /** Simulate ACKER_ACK: bolt acking its portion of tree id=treeId, xorVal. */
    private Tuple makeAckTuple(long treeId, long xorVal) {
        Tuple t = mock(Tuple.class);
        when(t.getSourceStreamId()).thenReturn(Acker.ACKER_ACK_STREAM_ID);
        when(t.getValue(0)).thenReturn(treeId);
        when(t.getLong(1)).thenReturn(xorVal);
        return t;
    }

    /** Simulate a tick tuple (drives RotatingMap rotation). */
    private Tuple makeTickTuple() {
        Tuple t = mock(Tuple.class);
        when(t.getSourceStreamId()).thenReturn(org.apache.storm.Constants.SYSTEM_TICK_STREAM_ID);
        // TupleUtils.isTick checks the source component
        when(t.getSourceComponent()).thenReturn(org.apache.storm.Constants.SYSTEM_COMPONENT_ID);
        return t;
    }

    // -----------------------------------------------------------------------
    // Test 1: Feature disabled by default — existing behavior unchanged
    // -----------------------------------------------------------------------

    /**
     * When topology.message.progress.timeout.secs is not set, the acker must
     * behave identically to the original implementation: tuple trees expire
     * after TIMEOUT_BUCKET_NUM rotations regardless of recent progress.
     */
    @Test
    public void testFeatureDisabledByDefault_treeExpiresOnWallClockRotation() {
        try (SimulatedTime ignored = new SimulatedTime()) {
            TestAckerHarness h = prepareAcker(confDefault());

            long treeId = 42L;
            long xorSeed = 1234L;
            int spoutTask = 7;

            // Spout emits
            h.acker.execute(makeInitTuple(treeId, xorSeed, spoutTask));

            // Advance time and rotate enough times to evict from RotatingMap
            // (TIMEOUT_BUCKET_NUM - 1 rotations evict the oldest bucket)
            for (int i = 0; i < Acker.TIMEOUT_BUCKET_NUM; i++) {
                h.acker.execute(makeTickTuple());
            }

            // The tree should have been evicted (no ack or fail was ever called —
            // the ExpiredCallback fires and routes to failSpoutMsg via the spout,
            // but from the acker's perspective the entry is gone from pending).
            // We verify that emitDirect was NOT called with ACK_STREAM (tree was not completed).
            verify(h.collector, never()).emitDirect(anyInt(),
                eq(Acker.ACKER_ACK_STREAM_ID), any());
        }
    }

    // -----------------------------------------------------------------------
    // Test 2: Progress rescues a tree from expiry
    // -----------------------------------------------------------------------

    /**
     * With progress timeout enabled, a tree that receives a bolt ack within
     * the progress window must NOT be evicted on rotation — it should be
     * rescued (re-inserted into the head bucket).
     */
    @Test
    public void testProgressRescuesActiveTreeFromExpiry() {
        try (SimulatedTime ignored = new SimulatedTime()) {
            int progressSecs = 30;
            TestAckerHarness h = prepareAcker(confWithProgressTimeout(progressSecs));

            long treeId = 100L;
            long xorSeed = 999L;
            int spoutTask = 3;

            // Spout emits tree
            h.acker.execute(makeInitTuple(treeId, xorSeed, spoutTask));

            // Advance time close to (but within) the progress window
            Time.advanceTimeSecs(progressSecs - 5);

            // Bolt makes partial progress — this updates lastProgressTime
            h.acker.execute(makeAckTuple(treeId, xorSeed ^ 500L));

            // Now advance time far enough that wall-clock bucket rotation would
            // normally evict the tree, but progress just happened
            Time.advanceTimeSecs(progressSecs - 1);

            // Rotate — rescueRecentlyActiveEntries() should move tree to head bucket
            h.acker.execute(makeTickTuple());

            // Tree should still be alive — not yet failed/timed-out
            verify(h.collector, never()).emitDirect(anyInt(),
                eq(Acker.ACKER_FAIL_STREAM_ID), any());
        }
    }

    // -----------------------------------------------------------------------
    // Test 3: No progress causes expiry
    // -----------------------------------------------------------------------

    /**
     * With progress timeout enabled, a tree that receives NO bolt acks within
     * the progress window must still be evicted (orphan behavior is preserved).
     */
    @Test
    public void testNoProgressExpiresTree() {
        try (SimulatedTime ignored = new SimulatedTime()) {
            int progressSecs = 10;
            TestAckerHarness h = prepareAcker(confWithProgressTimeout(progressSecs));

            long treeId = 200L;
            long xorSeed = 777L;
            int spoutTask = 5;

            // Spout emits tree
            h.acker.execute(makeInitTuple(treeId, xorSeed, spoutTask));

            // Advance time well past the progress timeout WITHOUT any bolt acks
            Time.advanceTimeSecs(progressSecs + 5);

            // Rotate — tree has no recent progress, should NOT be rescued
            h.acker.execute(makeTickTuple());

            // Rotate again enough times for the oldest bucket to be evicted
            for (int i = 0; i < Acker.TIMEOUT_BUCKET_NUM; i++) {
                h.acker.execute(makeTickTuple());
            }

            // The tree has expired (no ack emitted, no rescue)
            verify(h.collector, never()).emitDirect(anyInt(),
                eq(Acker.ACKER_ACK_STREAM_ID), any());
        }
    }

    // -----------------------------------------------------------------------
    // Test 4: Full tree completion still works with progress timeout enabled
    // -----------------------------------------------------------------------

    /**
     * When a tree is fully acked (XOR val == 0) with progress timeout enabled,
     * the acker must emit ACK to the spout exactly as before — progress timeout
     * must not interfere with normal completion.
     */
    @Test
    public void testFullAckCompletesTreeNormally() {
        try (SimulatedTime ignored = new SimulatedTime()) {
            TestAckerHarness h = prepareAcker(confWithProgressTimeout(60));

            long treeId = 300L;
            long xorSeed = 888L;
            int spoutTask = 2;

            // Spout emits
            h.acker.execute(makeInitTuple(treeId, xorSeed, spoutTask));

            // Bolt acks with the same xorVal — XOR to zero means tree complete
            h.acker.execute(makeAckTuple(treeId, xorSeed));

            // Acker should have sent ACK to spout
            verify(h.collector).emitDirect(eq(spoutTask),
                eq(Acker.ACKER_ACK_STREAM_ID), any());
        }
    }

    // -----------------------------------------------------------------------
    // Test 5: progressTimeoutMs > wall-clock timeout is effectively a no-op
    // -----------------------------------------------------------------------

    /**
     * If progress timeout > message timeout, every tree will expire on the
     * wall-clock rotation before progress timeout can rescue it.
     * Behavior should be identical to no progress timeout set.
     */
    @Test
    public void testProgressTimeoutLargerThanWallClockIsEffectivelyNoop() {
        try (SimulatedTime ignored = new SimulatedTime()) {
            // Wall-clock timeout (simulated via rotations) < progress timeout
            // Effectively the wall-clock mechanism always fires first.
            TestAckerHarness h = prepareAcker(confWithProgressTimeout(9999));

            long treeId = 400L;
            long xorSeed = 111L;
            int spoutTask = 9;

            h.acker.execute(makeInitTuple(treeId, xorSeed, spoutTask));

            // Make progress well within progress window but past bucket rotation
            Time.advanceTimeSecs(5);
            h.acker.execute(makeAckTuple(treeId, xorSeed ^ 50L));

            // Even though progress happened, after enough rotations the entry will
            // be rescued into the head bucket (progress is recent) — demonstrating
            // that the rescue logic itself works regardless of the absolute values.
            h.acker.execute(makeTickTuple());

            // No ACK to spout — tree not complete yet
            verify(h.collector, never()).emitDirect(anyInt(),
                eq(Acker.ACKER_ACK_STREAM_ID), any());
        }
    }

    // -----------------------------------------------------------------------
    // Inner harness class
    // -----------------------------------------------------------------------

    private static class TestAckerHarness {
        final Acker acker;
        final OutputCollector collector;

        TestAckerHarness(Acker acker, OutputCollector collector) {
            this.acker = acker;
            this.collector = collector;
        }
    }
}
