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

package org.apache.storm;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.ILocalCluster.ILocalTopology;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.storm.testing.AckFailMapTracker;
import org.apache.storm.testing.FeederSpout;
import org.apache.storm.tuple.Values;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TickTupleTest {
    private final static Logger LOG = LoggerFactory.getLogger(TickTupleTest.class);
    private static final AtomicInteger tickTupleCount = new AtomicInteger();
    private static final AtomicReference<Tuple> nonTickTuple = new AtomicReference<>(null);
    private static final AtomicBoolean receivedAnyTuple = new AtomicBoolean();
    //This needs to be appropriately large to drown out any time advances performed during topology boot
    private static final int TICK_INTERVAL_SECS = 30;
    
    @AfterEach
    public void cleanUp() {
        tickTupleCount.set(0);
        nonTickTuple.set(null);
        receivedAnyTuple.set(false);
    }

    @Test
    public void testTickTupleWorksWithSystemBolt() throws Exception {
        try (ILocalCluster cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .build()) {
            
            TopologyBuilder builder = new TopologyBuilder();
            FeederSpout feeder = new FeederSpout(new Fields("field1"));
            AckFailMapTracker tracker = new AckFailMapTracker();
            feeder.setAckFailDelegate(tracker);
            
            builder.setSpout("Spout", feeder);
            builder.setBolt("Bolt", new NoopBolt())
                .shuffleGrouping("Spout");
            
            Config topoConf = new Config();
            topoConf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, TICK_INTERVAL_SECS);
            
            try (ILocalTopology topo = cluster.submitTopology("test", topoConf, builder.createTopology())) {
                //Use a bootstrap tuple to wait for topology to be running
                feeder.feed(new Values("val"), 1);
                AssertLoop.assertAcked(tracker, 1);
                /*
                 * Verify that some ticks are received. The interval between ticks is validated by the bolt.
                 * Too few and the checks will time out. Too many and the bolt may crash (not reliably, but the test should become flaky).
                 */
                try {
                    cluster.advanceClusterTime(TICK_INTERVAL_SECS);
                    waitForTicks(1);
                    cluster.advanceClusterTime(TICK_INTERVAL_SECS);
                    waitForTicks(2);
                    cluster.advanceClusterTime(TICK_INTERVAL_SECS);
                    waitForTicks(3);
                } catch (ConditionTimeoutException e) {
                    throw new AssertionError(e.getMessage());
                }
                assertNull("The bolt got a tuple that is not a tick tuple " + nonTickTuple.get(), nonTickTuple.get());
            }
        }
    }
    
    private void waitForTicks(int minTicks) {
        try {
            Awaitility.with()
                .pollInterval(1, TimeUnit.MILLISECONDS)
                .atMost(Testing.TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertThat(tickTupleCount.get(), Matchers.greaterThanOrEqualTo(minTicks)));
        } catch (ConditionTimeoutException e) {
            throw new AssertionError(e.getMessage());
        }
    }

    private static class NoopBolt extends BaseRichBolt {
        private OutputCollector collector;
        
        @Override
        public void prepare(Map<String, Object> conf, TopologyContext topologyContext, OutputCollector outputCollector) {
            collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            LOG.info("GOT {} at time {}", tuple, Time.currentTimeMillis());
            if (!receivedAnyTuple.get() && Time.currentTimeSecs() > TICK_INTERVAL_SECS) {
                throw new RuntimeException("Simulated time was higher than " + TICK_INTERVAL_SECS + " at start of test."
                    + " Increase the interval until this no longer occurs, but keep an eye on Storm's timeouts for e.g. worker heartbeat.");
            }
            receivedAnyTuple.set(true);
            if (tickTupleCount.get() > 3) {
                throw new RuntimeException("Unexpectedly many tick tuples");
            }
            if (TupleUtils.isTick(tuple)) {
                tickTupleCount.incrementAndGet();
                collector.ack(tuple);
            } else {
                if (tuple.getValues().size() == 1 && "val".equals(tuple.getValue(0))) {
                    collector.ack(tuple);
                } else {
                    nonTickTuple.set(tuple);
                }
            }
        }

        @Override
        public void cleanup() { }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {}
    }
}
