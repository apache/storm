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

package org.apache.storm.testing;

import static org.apache.storm.Testing.whileTimeout;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.storm.ILocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.Thrift;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.utils.RegisteredGlobalState;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tracked topology keeps metrics for every bolt and spout.
 * This allows a test to know how many tuples have been fully processed.
 * Metrics are tracked on a per cluster basis.  So only one tracked topology
 * should be run in a tracked cluster to avoid conflicts.
 */
public class TrackedTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TrackedTopology.class);
    private final StormTopology topology;
    private final AtomicInteger lastSpoutCommit;
    private final ILocalCluster cluster;

    /**
     * Create a new topology to be tracked.
     * @param origTopo the original topology.
     * @param cluster a cluster that should have been launched with tracking enabled.
     */
    public TrackedTopology(StormTopology origTopo, ILocalCluster cluster) {
        LOG.warn("CLUSTER {} - {}", cluster, cluster.getTrackedId());
        this.cluster = cluster;
        lastSpoutCommit = new AtomicInteger(0);
        String id = cluster.getTrackedId();
        topology = origTopo.deepCopy();
        for (Bolt bolt : topology.get_bolts().values()) {
            IRichBolt obj = (IRichBolt) Thrift.deserializeComponentObject(bolt.get_bolt_object());
            bolt.set_bolt_object(Thrift.serializeComponentObject(new BoltTracker(obj, id)));
        }
        for (SpoutSpec spout : topology.get_spouts().values()) {
            IRichSpout obj = (IRichSpout) Thrift.deserializeComponentObject(spout.get_spout_object());
            spout.set_spout_object(Thrift.serializeComponentObject(new SpoutTracker(obj, id)));
        }
    }

    @SuppressWarnings("unchecked")
    private static int globalAmt(String id, String key) {
        LOG.warn("Reading tracked metrics for ID {}", id);
        return ((ConcurrentHashMap<String, AtomicInteger>) RegisteredGlobalState.getState(id)).get(key).get();
    }

    public StormTopology getTopology() {
        return topology;
    }

    public ILocalCluster getCluster() {
        return cluster;
    }

    /**
     * Wait for 1 tuple to be fully processed.
     */
    public void trackedWait() {
        trackedWait(1, Testing.TEST_TIMEOUT_MS);
    }

    /**
     * Wait for amt tuples to be fully processed.
     */
    public void trackedWait(int amt) {
        trackedWait(amt, Testing.TEST_TIMEOUT_MS);
    }

    /**
     * Wait for amt tuples to be fully processed timeoutMs happens.
     */
    public void trackedWait(int amt, int timeoutMs) {
        final int target = amt + lastSpoutCommit.get();
        final String id = cluster.getTrackedId();
        Random rand = ThreadLocalRandom.current();
        whileTimeout(timeoutMs,
            () -> {
                int se = globalAmt(id, "spout-emitted");
                int transferred = globalAmt(id, "transferred");
                int processed = globalAmt(id, "processed");
                LOG.info("emitted {} target {} transferred {} processed {}", se, target, transferred, processed);
                return (target != se) || (transferred != processed);
            },
            () -> {
                Time.advanceTimeSecs(1);
                try {
                    Thread.sleep(rand.nextInt(200));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        lastSpoutCommit.set(target);
    }

    /**
     * Read a metric from the tracked cluster (NOT JUST THIS TOPOLOGY).
     * @param key one of "spout-emitted", "processed", or "transferred"
     * @return the amount of that metric
     */
    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public int globalAmt(String key) {
        return globalAmt(cluster.getTrackedId(), key);
    }
}
