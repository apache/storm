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

package org.apache.storm.daemon;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.TupleUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Acker implements IBolt {
    public static final String ACKER_COMPONENT_ID = "__acker";
    public static final String ACKER_INIT_STREAM_ID = "__ack_init";
    public static final String ACKER_ACK_STREAM_ID = "__ack_ack";
    public static final String ACKER_FAIL_STREAM_ID = "__ack_fail";
    public static final String ACKER_RESET_TIMEOUT_STREAM_ID = "__ack_reset_timeout";
    public static final int TIMEOUT_BUCKET_NUM = 3;
    private static final Logger LOG = LoggerFactory.getLogger(Acker.class);
    private static final long serialVersionUID = 4430906880683183091L;
    private OutputCollector collector;
    private RotatingMap<Object, AckObject> pending;

    // Progress-based timeout fields (STORM-2359)
    private boolean progressTimeoutEnabled = false;
    private long progressTimeoutMs = Long.MAX_VALUE;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.pending = new RotatingMap<>(TIMEOUT_BUCKET_NUM);

        // STORM-2359: opt-in progress-based timeout
        Number progressSecs = ObjectReader.getInt(
            topoConf.get(Config.TOPOLOGY_MESSAGE_PROGRESS_TIMEOUT_SECS), null);
        if (progressSecs != null) {
            this.progressTimeoutEnabled = true;
            this.progressTimeoutMs = progressSecs.longValue() * 1000L;
            LOG.info("Progress-based timeout enabled: {} secs ({} ms)",
                progressSecs, this.progressTimeoutMs);
        }
    }

    @Override
    public void execute(Tuple input) {
        if (TupleUtils.isTick(input)) {
            if (progressTimeoutEnabled) {
                rescueRecentlyActiveEntries();
            }
            Map<Object, AckObject> tmp = pending.rotate();
            LOG.debug("Number of timeout tuples:{}", tmp.size());
            return;
        }

        boolean resetTimeout = false;
        String streamId = input.getSourceStreamId();
        Object id = input.getValue(0);
        AckObject curr = pending.get(id);
        if (ACKER_INIT_STREAM_ID.equals(streamId)) {
            if (curr == null) {
                curr = new AckObject();
                pending.put(id, curr);
            }
            curr.updateAck(input.getLong(1));
            curr.spoutTask = input.getInteger(2);
        } else if (ACKER_ACK_STREAM_ID.equals(streamId)) {
            if (curr == null) {
                curr = new AckObject();
                pending.put(id, curr);
            }
            curr.updateAck(input.getLong(1));
        } else if (ACKER_FAIL_STREAM_ID.equals(streamId)) {
            // For the case that ack_fail message arrives before ack_init
            if (curr == null) {
                curr = new AckObject();
            }
            curr.failed = true;
            pending.put(id, curr);
        } else if (ACKER_RESET_TIMEOUT_STREAM_ID.equals(streamId)) {
            resetTimeout = true;
            if (curr == null) {
                curr = new AckObject();
            }
            pending.put(id, curr);
        } else if (Constants.SYSTEM_FLUSH_STREAM_ID.equals(streamId)) {
            collector.flush();
            return;
        } else {
            LOG.warn("Unknown source stream {} from task-{}", streamId, input.getSourceTask());
            return;
        }

        int task = curr.spoutTask;
        if (task >= 0 && (curr.val == 0 || curr.failed || resetTimeout)) {
            Values tuple = new Values(id, getTimeDeltaMillis(curr.startTime));
            if (curr.val == 0) {
                pending.remove(id);
                collector.emitDirect(task, ACKER_ACK_STREAM_ID, tuple);
            } else if (curr.failed) {
                pending.remove(id);
                collector.emitDirect(task, ACKER_FAIL_STREAM_ID, tuple);
            } else if (resetTimeout) {
                collector.emitDirect(task, ACKER_RESET_TIMEOUT_STREAM_ID, tuple);
            } else {
                throw new IllegalStateException("The checks are inconsistent we reach what should be unreachable code.");
            }
        }

        collector.ack(input);
    }

    @Override
    public void cleanup() {
        LOG.info("Acker: cleanup successfully");
    }

    /**
     * STORM-2359: Before rotating the pending map, inspect the oldest bucket and re-insert
     * any entries that have received a bolt ack within the progress timeout window.
     * Re-inserting via {@code pending.put()} moves the entry to the head (newest) bucket,
     * rescuing it from the eviction that {@code pending.rotate()} is about to perform.
     *
     * <p>Only entries in the oldest (last) bucket are candidates for eviction on the next
     * rotate(), so we only need to scan that bucket. Entries in earlier buckets are safe
     * for at least one more rotation cycle.
     *
     * <p>This method is only called when {@code progressTimeoutEnabled} is true.
     */
    private void rescueRecentlyActiveEntries() {
        long now = Time.currentTimeMillis();
        // Collect keys to rescue first to avoid ConcurrentModificationException —
        // pending.put() structurally modifies the bucket list.
        List<Object> toRescue = new ArrayList<>();
        for (Map.Entry<Object, AckObject> entry : pending.peekOldestBucket().entrySet()) {
            AckObject obj = entry.getValue();
            if ((now - obj.lastProgressTime) < progressTimeoutMs) {
                toRescue.add(entry.getKey());
            }
        }
        for (Object key : toRescue) {
            AckObject obj = pending.get(key);
            if (obj != null) {
                // put() moves key to the head bucket — rescued from the upcoming rotation
                pending.put(key, obj);
                LOG.debug("STORM-2359: Rescued tuple tree {} from timeout; last progress {}ms ago",
                    key, now - obj.lastProgressTime);
            }
        }
        if (!toRescue.isEmpty()) {
            LOG.debug("STORM-2359: Rescued {} tuple tree(s) with recent progress", toRescue.size());
        }
    }

    private long getTimeDeltaMillis(long startTimeMillis) {
        return Time.currentTimeMillis() - startTimeMillis;
    }

    static class AckObject {
        public long val = 0L;
        public long startTime = Time.currentTimeMillis();
        /**
         * STORM-2359: Wall-clock timestamp of the last bolt ack received for this tuple tree.
         * Updated by {@link #updateAck(Long)} on every partial ack from any bolt in the tree.
         * Used by {@link Acker#rescueRecentlyActiveEntries()} to determine whether the tree
         * has made forward progress recently and should be rescued from timeout expiry.
         *
         * <p>Initialized to {@code startTime} so a newly-emitted tree is not immediately
         * considered stale.
         */
        public long lastProgressTime = Time.currentTimeMillis();
        public int spoutTask = -1;
        public boolean failed = false;

        // val xor value; also records that progress was made on this tree
        public void updateAck(Long value) {
            val = Utils.bitXor(val, value);
            lastProgressTime = Time.currentTimeMillis();
        }
    }
}
