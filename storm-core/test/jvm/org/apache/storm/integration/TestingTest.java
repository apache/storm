/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.integration;

import static org.apache.storm.AssertLoop.assertAcked;
import static org.apache.storm.AssertLoop.assertFailed;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.Thrift;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.AckFailMapTracker;
import org.apache.storm.testing.FeederSpout;
import org.apache.storm.testing.MkTupleParam;
import org.apache.storm.testing.TrackedTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

public class TestingTest {

    @Test
    public void testSimulatedTime() throws Exception {
        assertThat(Time.isSimulating(), is(false));
        try(SimulatedTime time = new SimulatedTime()) {
            assertThat(Time.isSimulating(), is(true));
        }
    }
    
    @Test
    public void testWithLocalCluster() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
            .withSupervisors(2)
            .withPortsPerSupervisor(5)
            .build()) {
            assertThat(cluster, notNullValue());
            assertThat(cluster.getNimbus(), notNullValue());
        }
    }
    
    @Test
    public void testWithSimulatedTimeLocalCluster() throws Exception {
        assertThat(Time.isSimulating(), is(false));
        try (LocalCluster cluster = new LocalCluster.Builder()
            .withSupervisors(2)
            .withPortsPerSupervisor(5)
            .withSimulatedTime()
            .build()) {
            assertThat(cluster, notNullValue());
            assertThat(cluster.getNimbus(), notNullValue());
            assertThat(Time.isSimulating(), is(true));
        }
    }
    
    @Test
    public void testWithTrackedCluster() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withTracked()
            .build()) {
            AckTrackingFeeder feeder = new AckTrackingFeeder("num");

            Map<String, Thrift.SpoutDetails> spoutMap = new HashMap<>();
            spoutMap.put("1", Thrift.prepareSpoutDetails(feeder.getSpout()));

            Map<String, Thrift.BoltDetails> boltMap = new HashMap<>();
            boltMap.put("2", Thrift.prepareBoltDetails(Collections.singletonMap(Utils.getGlobalStreamId("1", null), Thrift.prepareShuffleGrouping()), new IdentityBolt()));
            boltMap.put("3", Thrift.prepareBoltDetails(Collections.singletonMap(Utils.getGlobalStreamId("1", null), Thrift.prepareShuffleGrouping()), new IdentityBolt()));

            Map<GlobalStreamId, Grouping> aggregatorInputs = new HashMap<>();
            aggregatorInputs.put(Utils.getGlobalStreamId("2", null), Thrift.prepareShuffleGrouping());
            aggregatorInputs.put(Utils.getGlobalStreamId("3", null), Thrift.prepareShuffleGrouping());
            boltMap.put("4", Thrift.prepareBoltDetails(aggregatorInputs, new AggBolt(4)));

            TrackedTopology tracked = new TrackedTopology(Thrift.buildTopology(spoutMap, boltMap), cluster);;

            cluster.submitTopology("test-acking2", new Config(), tracked);

            cluster.advanceClusterTime(11);
            feeder.feed(new Values(1));
            Testing.trackedWait(tracked, 1);
            feeder.assertNumAcks(0);
            feeder.feed(new Values(1));
            Testing.trackedWait(tracked, 1);
            feeder.assertNumAcks(2);
        }
    }
    
    @Test
    public void testAdvanceClusterTime() throws Exception {
        Config daemonConf = new Config();
        daemonConf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
        try (LocalCluster cluster = new LocalCluster.Builder()
            .withDaemonConf(daemonConf)
            .withSimulatedTime()
            .build()) {
            FeederSpout feeder = new FeederSpout(new Fields("field1"));
            AckFailMapTracker tracker = new AckFailMapTracker();
            feeder.setAckFailDelegate(tracker);

            Map<String, Thrift.SpoutDetails> spoutMap = new HashMap<>();
            spoutMap.put("1", Thrift.prepareSpoutDetails(feeder));

            Map<String, Thrift.BoltDetails> boltMap = new HashMap<>();
            boltMap.put("2", Thrift.prepareBoltDetails(Collections.singletonMap(Utils.getGlobalStreamId("1", null), Thrift.prepareShuffleGrouping()), new AckEveryOtherBolt()));

            StormTopology topology = Thrift.buildTopology(spoutMap, boltMap);
            
            Config stormConf = new Config();
            stormConf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10);

            cluster.submitTopology("timeout-tester", stormConf, topology);

            feeder.feed(new Values("a"), 1);
            feeder.feed(new Values("b"), 2);
            feeder.feed(new Values("c"), 3);
            cluster.advanceClusterTime(9);
            assertAcked(tracker, 1, 3);
            assertThat(tracker.isFailed(2), is(false));
            cluster.advanceClusterTime(12);
            assertFailed(tracker, 2);
        }
    }
    
    @Test
    public void testDisableTupleTimeout() throws Exception {
        Config daemonConf = new Config();
        daemonConf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, false);
        try (LocalCluster cluster = new LocalCluster.Builder()
            .withDaemonConf(daemonConf)
            .withSimulatedTime()
            .build()) {
            FeederSpout feeder = new FeederSpout(new Fields("field1"));
            AckFailMapTracker tracker = new AckFailMapTracker();
            feeder.setAckFailDelegate(tracker);

            Map<String, Thrift.SpoutDetails> spoutMap = new HashMap<>();
            spoutMap.put("1", Thrift.prepareSpoutDetails(feeder));

            Map<String, Thrift.BoltDetails> boltMap = new HashMap<>();
            boltMap.put("2", Thrift.prepareBoltDetails(Collections.singletonMap(Utils.getGlobalStreamId("1", null), Thrift.prepareShuffleGrouping()), new AckEveryOtherBolt()));

            StormTopology topology = Thrift.buildTopology(spoutMap, boltMap);
            
            Config stormConf = new Config();
            stormConf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10);
            stormConf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, false);

            cluster.submitTopology("disable-timeout-tester", stormConf, topology);

            feeder.feed(new Values("a"), 1);
            feeder.feed(new Values("b"), 2);
            feeder.feed(new Values("c"), 3);
            cluster.advanceClusterTime(9);
            assertAcked(tracker, 1, 3);
            assertThat(tracker.isFailed(2), is(false));
            cluster.advanceClusterTime(12);
            assertThat(tracker.isFailed(2), is(false));
        }
    }
    
    @Test
    public void testTestTuple() throws Exception {
        Tuple tuple = Testing.testTuple(new Values("james", "bond"));
        assertThat(tuple.getValues(), is(new Values("james", "bond")));
        assertThat(tuple.getSourceStreamId(), is(Utils.DEFAULT_STREAM_ID));
        assertThat(tuple.getFields().toList(), is(Arrays.asList("field1", "field2")));
        assertThat(tuple.getSourceComponent(), is("component"));
    }
    
    @Test
    public void testTestTupleWithMkTupleParam() throws Exception {
        MkTupleParam mkTupleParam = new MkTupleParam();
        mkTupleParam.setStream("test-stream");
        mkTupleParam.setComponent("test-component");
        mkTupleParam.setFields("fname", "lname");
        Tuple tuple = Testing.testTuple(new Values("james", "bond"), mkTupleParam);
        assertThat(tuple.getValues(), is(new Values("james", "bond")));
        assertThat(tuple.getSourceStreamId(), is("test-stream"));
        assertThat(tuple.getFields().toList(), is(Arrays.asList("fname", "lname")));
        assertThat(tuple.getSourceComponent(), is("test-component"));
    }
    
}
