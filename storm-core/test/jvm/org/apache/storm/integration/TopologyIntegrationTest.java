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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.Thrift;
import org.apache.storm.Thrift.BoltDetails;
import org.apache.storm.Thrift.SpoutDetails;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.generated.TopologyInitialStatus;
import org.apache.storm.hooks.BaseTaskHook;
import org.apache.storm.hooks.info.BoltAckInfo;
import org.apache.storm.hooks.info.BoltExecuteInfo;
import org.apache.storm.hooks.info.BoltFailInfo;
import org.apache.storm.hooks.info.EmitInfo;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.AckFailMapTracker;
import org.apache.storm.testing.CompleteTopologyParam;
import org.apache.storm.testing.FeederSpout;
import org.apache.storm.testing.FixedTuple;
import org.apache.storm.testing.IntegrationTest;
import org.apache.storm.testing.MockedSources;
import org.apache.storm.testing.TestAggregatesCounter;
import org.apache.storm.testing.TestConfBolt;
import org.apache.storm.testing.TestGlobalCount;
import org.apache.storm.testing.TestPlannerSpout;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.testing.TrackedTopology;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@IntegrationTest
public class TopologyIntegrationTest {

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    public void testBasicTopology(boolean useLocalMessaging) throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withSupervisors(4)
            .withDaemonConf(Collections.singletonMap(Config.STORM_LOCAL_MODE_ZMQ, !useLocalMessaging))
            .build()) {
            Map<String, SpoutDetails> spoutMap = Collections.singletonMap("1", Thrift.prepareSpoutDetails(new TestWordSpout(true), 3));
            Map<String, BoltDetails> boltMap = new HashMap<>();
            boltMap.put("2",
                Thrift.prepareBoltDetails(
                    Collections.singletonMap(
                        Utils.getGlobalStreamId("1", null),
                        Thrift.prepareFieldsGrouping(Collections.singletonList("word"))),
                    new TestWordCounter(), 4));
            boltMap.put("3",
                Thrift.prepareBoltDetails(
                    Collections.singletonMap(
                        Utils.getGlobalStreamId("1", null),
                        Thrift.prepareGlobalGrouping()),
                    new TestGlobalCount()));
            boltMap.put("4",
                Thrift.prepareBoltDetails(
                    Collections.singletonMap(
                        Utils.getGlobalStreamId("2", null),
                        Thrift.prepareGlobalGrouping()),
                    new TestAggregatesCounter()));
            StormTopology topology = Thrift.buildTopology(spoutMap, boltMap);

            Map<String, Object> stormConf = new HashMap<>();
            stormConf.put(Config.TOPOLOGY_WORKERS, 2);
            stormConf.put(Config.TOPOLOGY_TESTING_ALWAYS_TRY_SERIALIZE, true);

            List<FixedTuple> testTuples = Arrays.asList("nathan", "bob", "joey", "nathan").stream()
                .map(value -> new FixedTuple(new Values(value)))
                .collect(Collectors.toList());

            MockedSources mockedSources = new MockedSources(Collections.singletonMap("1", testTuples));

            CompleteTopologyParam completeTopologyParams = new CompleteTopologyParam();
            completeTopologyParams.setMockedSources(mockedSources);
            completeTopologyParams.setStormConf(stormConf);

            Map<String, List<FixedTuple>> results = Testing.completeTopology(cluster, topology, completeTopologyParams);

            assertThat(Testing.readTuples(results, "1"), containsInAnyOrder(
                new Values("nathan"),
                new Values("nathan"),
                new Values("bob"),
                new Values("joey")));
            assertThat(Testing.readTuples(results, "2"), containsInAnyOrder(
                new Values("nathan", 1),
                new Values("nathan", 2),
                new Values("bob", 1),
                new Values("joey", 1)
            ));
            assertThat(Testing.readTuples(results, "3"), contains(
                new Values(1),
                new Values(2),
                new Values(3),
                new Values(4)
            ));
            assertThat(Testing.readTuples(results, "4"), contains(
                new Values(1),
                new Values(2),
                new Values(3),
                new Values(4)
            ));
        }
    }

    private static class EmitTaskIdBolt extends BaseRichBolt {

        private int taskIndex;
        private OutputCollector collector;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("tid"));
        }

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            this.taskIndex = context.getThisTaskIndex();
        }

        @Override
        public void execute(Tuple input) {
            collector.emit(input, new Values(taskIndex));
            collector.ack(input);
        }

    }

    @Test
    public void testMultiTasksPerCluster() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withSupervisors(4)
            .build()) {
            Map<String, SpoutDetails> spoutMap = Collections.singletonMap("1", Thrift.prepareSpoutDetails(new TestWordSpout(true)));
            Map<String, BoltDetails> boltMap = new HashMap<>();
            boltMap.put("2",
                Thrift.prepareBoltDetails(
                    Collections.singletonMap(
                        Utils.getGlobalStreamId("1", null),
                        Thrift.prepareAllGrouping()),
                    new EmitTaskIdBolt(), 3, Collections.singletonMap(Config.TOPOLOGY_TASKS, 6)));
            StormTopology topology = Thrift.buildTopology(spoutMap, boltMap);

            MockedSources mockedSources = new MockedSources(Collections.singletonMap("1", Collections.singletonList(new FixedTuple(new Values("a")))));

            CompleteTopologyParam completeTopologyParams = new CompleteTopologyParam();
            completeTopologyParams.setMockedSources(mockedSources);

            Map<String, List<FixedTuple>> results = Testing.completeTopology(cluster, topology, completeTopologyParams);

            assertThat(Testing.readTuples(results, "2"), containsInAnyOrder(
                new Values(0),
                new Values(1),
                new Values(2),
                new Values(3),
                new Values(4),
                new Values(5)
            ));
        }
    }

    @Test
    public void testTimeout() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withSupervisors(4)
            .withDaemonConf(Collections.singletonMap(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true))
            .build()) {
            FeederSpout feeder = new FeederSpout(new Fields("field1"));
            AckFailMapTracker tracker = new AckFailMapTracker();
            feeder.setAckFailDelegate(tracker);
            Map<String, SpoutDetails> spoutMap = Collections.singletonMap("1", Thrift.prepareSpoutDetails(feeder));
            Map<String, BoltDetails> boltMap = new HashMap<>();
            boltMap.put("2",
                Thrift.prepareBoltDetails(
                    Collections.singletonMap(
                        Utils.getGlobalStreamId("1", null),
                        Thrift.prepareGlobalGrouping()),
                    new AckEveryOtherBolt()));
            StormTopology topology = Thrift.buildTopology(spoutMap, boltMap);

            cluster.submitTopology("timeout-tester", Collections.singletonMap(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10), topology);

            cluster.advanceClusterTime(11);
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

    private static class ResetTimeoutBolt extends BaseRichBolt {

        private int tupleCounter = 1;
        private Tuple firstTuple = null;
        private OutputCollector collector;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            if (tupleCounter == 1) {
                firstTuple = input;
            } else if (tupleCounter == 2) {
                collector.resetTimeout(firstTuple);
            } else if (tupleCounter == 5) {
                collector.ack(firstTuple);
                collector.ack(input);
            } else {
                collector.resetTimeout(firstTuple);
                collector.ack(input);
            }
            tupleCounter++;
        }
    }

    @Test
    public void testResetTimeout() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withDaemonConf(Collections.singletonMap(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true))
            .build()) {
            FeederSpout feeder = new FeederSpout(new Fields("field1"));
            AckFailMapTracker tracker = new AckFailMapTracker();
            feeder.setAckFailDelegate(tracker);
            Map<String, SpoutDetails> spoutMap = Collections.singletonMap("1", Thrift.prepareSpoutDetails(feeder));
            Map<String, BoltDetails> boltMap = new HashMap<>();
            boltMap.put("2",
                Thrift.prepareBoltDetails(
                    Collections.singletonMap(
                        Utils.getGlobalStreamId("1", null),
                        Thrift.prepareGlobalGrouping()),
                    new ResetTimeoutBolt()));
            StormTopology topology = Thrift.buildTopology(spoutMap, boltMap);

            cluster.submitTopology("reset-timeout-tester", Collections.singletonMap(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10), topology);

            //The first tuple wil be used to check timeout reset
            feeder.feed(new Values("a"), 1);
            //The second tuple is used to wait for the spout to rotate its pending map
            feeder.feed(new Values("b"), 2);
            cluster.advanceClusterTime(9);
            //The other tuples are used to reset the first tuple's timeout,
            //and to wait for the message to get through to the spout (acks use the same path as timeout resets)
            feeder.feed(new Values("c"), 3);
            assertAcked(tracker, 3);
            cluster.advanceClusterTime(9);
            feeder.feed(new Values("d"), 4);
            assertAcked(tracker, 4);
            cluster.advanceClusterTime(2);
            //The time is now twice the message timeout, the second tuple should expire since it was not acked
            //Waiting for this also ensures that the first tuple gets failed if reset-timeout doesn't work
            assertFailed(tracker, 2);
            //Put in a tuple to cause the first tuple to be acked
            feeder.feed(new Values("e"), 5);
            assertAcked(tracker, 5);
            //The first tuple should be acked, and should not have failed
            assertThat(tracker.isFailed(1), is(false));
            assertAcked(tracker, 1);
        }
    }

    private StormTopology mkValidateTopology() {
        Map<String, SpoutDetails> spoutMap = Collections.singletonMap("1", Thrift.prepareSpoutDetails(new TestWordSpout(true), 3));
        Map<String, BoltDetails> boltMap = Collections.singletonMap("2",
            Thrift.prepareBoltDetails(
                Collections.singletonMap(
                    Utils.getGlobalStreamId("1", null),
                    Thrift.prepareFieldsGrouping(Collections.singletonList("word"))),
                new TestWordCounter(), 4));
        return Thrift.buildTopology(spoutMap, boltMap);
    }

    private StormTopology mkInvalidateTopology1() {
        Map<String, SpoutDetails> spoutMap = Collections.singletonMap("1", Thrift.prepareSpoutDetails(new TestWordSpout(true), 3));
        Map<String, BoltDetails> boltMap = Collections.singletonMap("2",
            Thrift.prepareBoltDetails(
                Collections.singletonMap(
                    Utils.getGlobalStreamId("3", null),
                    Thrift.prepareFieldsGrouping(Collections.singletonList("word"))),
                new TestWordCounter(), 4));
        return Thrift.buildTopology(spoutMap, boltMap);
    }

    private StormTopology mkInvalidateTopology2() {
        Map<String, SpoutDetails> spoutMap = Collections.singletonMap("1", Thrift.prepareSpoutDetails(new TestWordSpout(true), 3));
        Map<String, BoltDetails> boltMap = Collections.singletonMap("2",
            Thrift.prepareBoltDetails(
                Collections.singletonMap(
                    Utils.getGlobalStreamId("1", null),
                    Thrift.prepareFieldsGrouping(Collections.singletonList("non-exists-field"))),
                new TestWordCounter(), 4));
        return Thrift.buildTopology(spoutMap, boltMap);
    }

    private StormTopology mkInvalidateTopology3() {
        Map<String, SpoutDetails> spoutMap = Collections.singletonMap("1", Thrift.prepareSpoutDetails(new TestWordSpout(true), 3));
        Map<String, BoltDetails> boltMap = Collections.singletonMap("2",
            Thrift.prepareBoltDetails(
                Collections.singletonMap(
                    Utils.getGlobalStreamId("1", "non-exists-stream"),
                    Thrift.prepareFieldsGrouping(Collections.singletonList("word"))),
                new TestWordCounter(), 4));
        return Thrift.buildTopology(spoutMap, boltMap);
    }

    private boolean tryCompleteWordCountTopology(LocalCluster cluster, StormTopology topology) throws Exception {
        try {
            List<FixedTuple> testTuples = Arrays.asList("nathan", "bob", "joey", "nathan").stream()
                .map(value -> new FixedTuple(new Values(value)))
                .collect(Collectors.toList());
            MockedSources mockedSources = new MockedSources(Collections.singletonMap("1", testTuples));
            CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
            completeTopologyParam.setMockedSources(mockedSources);
            completeTopologyParam.setStormConf(Collections.singletonMap(Config.TOPOLOGY_WORKERS, 2));
            Testing.completeTopology(cluster, topology, completeTopologyParam);
            return false;
        } catch (InvalidTopologyException e) {
            return true;
        }
    }

    @Test
    public void testValidateTopologystructure() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withDaemonConf(Collections.singletonMap(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true))
            .build()) {
            assertThat(tryCompleteWordCountTopology(cluster, mkValidateTopology()), is(false));
            assertThat(tryCompleteWordCountTopology(cluster, mkInvalidateTopology1()), is(true));
            assertThat(tryCompleteWordCountTopology(cluster, mkInvalidateTopology2()), is(true));
            assertThat(tryCompleteWordCountTopology(cluster, mkInvalidateTopology3()), is(true));
        }
    }

    @Test
    public void testSystemStream() throws Exception {
        //this test works because mocking a spout splits up the tuples evenly among the tasks
        try (LocalCluster cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .build()) {
            Map<String, SpoutDetails> spoutMap = Collections.singletonMap("1", Thrift.prepareSpoutDetails(new TestWordSpout(true), 3));

            Map<GlobalStreamId, Grouping> boltInputs = new HashMap<>();
            boltInputs.put(Utils.getGlobalStreamId("1", null), Thrift.prepareFieldsGrouping(Collections.singletonList("word")));
            boltInputs.put(Utils.getGlobalStreamId("1", "__system"), Thrift.prepareGlobalGrouping());
            Map<String, BoltDetails> boltMap = Collections.singletonMap("2",
                Thrift.prepareBoltDetails(
                    boltInputs,
                    new IdentityBolt(), 1));
            StormTopology topology = Thrift.buildTopology(spoutMap, boltMap);

            Map<String, Object> stormConf = new HashMap<>();
            stormConf.put(Config.TOPOLOGY_WORKERS, 2);

            List<FixedTuple> testTuples = Arrays.asList("a", "b", "c").stream()
                .map(value -> new FixedTuple(new Values(value)))
                .collect(Collectors.toList());

            MockedSources mockedSources = new MockedSources(Collections.singletonMap("1", testTuples));

            CompleteTopologyParam completeTopologyParams = new CompleteTopologyParam();
            completeTopologyParams.setMockedSources(mockedSources);
            completeTopologyParams.setStormConf(stormConf);

            Map<String, List<FixedTuple>> results = Testing.completeTopology(cluster, topology, completeTopologyParams);

            assertThat(Testing.readTuples(results, "2"), containsInAnyOrder(
                new Values("a"),
                new Values("b"),
                new Values("c")
            ));
        }
    }

    private static class SpoutAndChecker {

        private final FeederSpout spout;
        private final Consumer<Integer> checker;

        public SpoutAndChecker(FeederSpout spout, Consumer<Integer> checker) {
            this.spout = spout;
            this.checker = checker;
        }
    }

    private static class BranchingBolt extends BaseRichBolt {

        private final int branches;
        private OutputCollector collector;

        public BranchingBolt(int branches) {
            this.branches = branches;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("num"));
        }

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            IntStream.range(0, branches)
                .forEach(i -> collector.emit(input, new Values(i)));
            collector.ack(input);
        }
    }

    private static class AckBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            collector.ack(input);
        }
    }

    @Test
    public void testAcking() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withTracked()
            .build()) {
            AckTrackingFeeder feeder1 = new AckTrackingFeeder("num");
            AckTrackingFeeder feeder2 = new AckTrackingFeeder("num");
            AckTrackingFeeder feeder3 = new AckTrackingFeeder("num");

            Map<String, SpoutDetails> spoutMap = new HashMap<>();
            spoutMap.put("1", Thrift.prepareSpoutDetails(feeder1.getSpout()));
            spoutMap.put("2", Thrift.prepareSpoutDetails(feeder2.getSpout()));
            spoutMap.put("3", Thrift.prepareSpoutDetails(feeder3.getSpout()));

            Map<String, BoltDetails> boltMap = new HashMap<>();
            boltMap.put("4", Thrift.prepareBoltDetails(Collections.singletonMap(Utils.getGlobalStreamId("1", null), Thrift.prepareShuffleGrouping()), new BranchingBolt(2)));
            boltMap.put("5", Thrift.prepareBoltDetails(Collections.singletonMap(Utils.getGlobalStreamId("2", null), Thrift.prepareShuffleGrouping()), new BranchingBolt(4)));
            boltMap.put("6", Thrift.prepareBoltDetails(Collections.singletonMap(Utils.getGlobalStreamId("3", null), Thrift.prepareShuffleGrouping()), new BranchingBolt(1)));

            Map<GlobalStreamId, Grouping> aggregatorInputs = new HashMap<>();
            aggregatorInputs.put(Utils.getGlobalStreamId("4", null), Thrift.prepareShuffleGrouping());
            aggregatorInputs.put(Utils.getGlobalStreamId("5", null), Thrift.prepareShuffleGrouping());
            aggregatorInputs.put(Utils.getGlobalStreamId("6", null), Thrift.prepareShuffleGrouping());
            boltMap.put("7", Thrift.prepareBoltDetails(aggregatorInputs, new AggBolt(3)));

            boltMap.put("8", Thrift.prepareBoltDetails(Collections.singletonMap(Utils.getGlobalStreamId("7", null), Thrift.prepareShuffleGrouping()), new BranchingBolt(2)));
            boltMap.put("9", Thrift.prepareBoltDetails(Collections.singletonMap(Utils.getGlobalStreamId("8", null), Thrift.prepareShuffleGrouping()), new AckBolt()));

            TrackedTopology tracked = new TrackedTopology(Thrift.buildTopology(spoutMap, boltMap), cluster);;

            cluster.submitTopology("acking-test1", Collections.emptyMap(), tracked);

            cluster.advanceClusterTime(11);
            feeder1.feed(new Values(1));
            Testing.trackedWait(tracked, 1);
            feeder1.assertNumAcks(0);
            feeder2.feed(new Values(1));
            Testing.trackedWait(tracked, 1);
            feeder1.assertNumAcks(1);
            feeder2.assertNumAcks(1);
            feeder1.feed(new Values(1));
            Testing.trackedWait(tracked, 1);
            feeder1.assertNumAcks(0);
            feeder1.feed(new Values(1));
            Testing.trackedWait(tracked, 1);
            feeder1.assertNumAcks(1);
            feeder3.feed(new Values(1));
            Testing.trackedWait(tracked, 1);
            feeder1.assertNumAcks(0);
            feeder3.assertNumAcks(0);
            feeder2.feed(new Values(1));
            Testing.trackedWait(tracked, 1);
            feeder1.feed(new Values(1));
            feeder2.feed(new Values(1));
            feeder3.feed(new Values(1));
        }
    }

    @Test
    public void testAckBranching() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withTracked()
            .build()) {
            AckTrackingFeeder feeder = new AckTrackingFeeder("num");

            Map<String, SpoutDetails> spoutMap = new HashMap<>();
            spoutMap.put("1", Thrift.prepareSpoutDetails(feeder.getSpout()));

            Map<String, BoltDetails> boltMap = new HashMap<>();
            boltMap.put("2", Thrift.prepareBoltDetails(Collections.singletonMap(Utils.getGlobalStreamId("1", null), Thrift.prepareShuffleGrouping()), new IdentityBolt()));
            boltMap.put("3", Thrift.prepareBoltDetails(Collections.singletonMap(Utils.getGlobalStreamId("1", null), Thrift.prepareShuffleGrouping()), new IdentityBolt()));

            Map<GlobalStreamId, Grouping> aggregatorInputs = new HashMap<>();
            aggregatorInputs.put(Utils.getGlobalStreamId("2", null), Thrift.prepareShuffleGrouping());
            aggregatorInputs.put(Utils.getGlobalStreamId("3", null), Thrift.prepareShuffleGrouping());
            boltMap.put("4", Thrift.prepareBoltDetails(aggregatorInputs, new AggBolt(4)));

            TrackedTopology tracked = new TrackedTopology(Thrift.buildTopology(spoutMap, boltMap), cluster);;

            cluster.submitTopology("test-acking2", Collections.emptyMap(), tracked);

            cluster.advanceClusterTime(11);
            feeder.feed(new Values(1));
            Testing.trackedWait(tracked, 1);
            feeder.assertNumAcks(0);
            feeder.feed(new Values(1));
            Testing.trackedWait(tracked, 1);
            feeder.assertNumAcks(2);
        }
    }

    private static class DupAnchorBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("num"));
        }

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            ArrayList<Tuple> anchors = new ArrayList<>();
            anchors.add(input);
            anchors.add(input);
            collector.emit(anchors, new Values(1));
            collector.ack(input);
        }
    }

    private static boolean boltPrepared = false;

    private static class PrepareTrackedBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            boltPrepared = true;
            collector.ack(input);
        }
    }

    private static boolean spoutOpened = false;

    private static class OpenTrackedSpout extends BaseRichSpout {

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("val"));
        }

        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        }

        @Override
        public void nextTuple() {
            spoutOpened = true;
        }

    }

    @Test
    public void testSubmitInactiveTopology() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withDaemonConf(Collections.singletonMap(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true))
            .build()) {
            FeederSpout feeder = new FeederSpout(new Fields("field1"));
            AckFailMapTracker tracker = new AckFailMapTracker();
            feeder.setAckFailDelegate(tracker);

            Map<String, SpoutDetails> spoutMap = new HashMap<>();
            spoutMap.put("1", Thrift.prepareSpoutDetails(feeder));
            spoutMap.put("2", Thrift.prepareSpoutDetails(new OpenTrackedSpout()));

            Map<String, BoltDetails> boltMap = new HashMap<>();
            boltMap.put("3", Thrift.prepareBoltDetails(Collections.singletonMap(Utils.getGlobalStreamId("1", null), Thrift.prepareGlobalGrouping()), new PrepareTrackedBolt()));

            boltPrepared = false;
            spoutOpened = false;

            StormTopology topology = Thrift.buildTopology(spoutMap, boltMap);

            cluster.submitTopologyWithOpts("test", Collections.singletonMap(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10), topology, new SubmitOptions(TopologyInitialStatus.INACTIVE));

            cluster.advanceClusterTime(11);
            feeder.feed(new Values("a"), 1);
            cluster.advanceClusterTime(9);
            assertThat(boltPrepared, is(false));
            assertThat(spoutOpened, is(false));
            cluster.getNimbus().activate("test");

            cluster.advanceClusterTime(12);
            assertAcked(tracker, 1);
            assertThat(boltPrepared, is(true));
            assertThat(spoutOpened, is(true));
        }
    }

    @Test
    public void testAckingSelfAnchor() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withTracked()
            .build()) {
            AckTrackingFeeder feeder = new AckTrackingFeeder("num");

            Map<String, SpoutDetails> spoutMap = new HashMap<>();
            spoutMap.put("1", Thrift.prepareSpoutDetails(feeder.getSpout()));

            Map<String, BoltDetails> boltMap = new HashMap<>();
            boltMap.put("2", Thrift.prepareBoltDetails(Collections.singletonMap(Utils.getGlobalStreamId("1", null), Thrift.prepareShuffleGrouping()), new DupAnchorBolt()));
            boltMap.put("3", Thrift.prepareBoltDetails(Collections.singletonMap(Utils.getGlobalStreamId("2", null), Thrift.prepareShuffleGrouping()), new AckBolt()));

            TrackedTopology tracked = new TrackedTopology(Thrift.buildTopology(spoutMap, boltMap), cluster);;

            cluster.submitTopology("test", Collections.emptyMap(), tracked);

            cluster.advanceClusterTime(11);
            feeder.feed(new Values(1));
            Testing.trackedWait(tracked, 1);
            feeder.assertNumAcks(1);
            feeder.feed(new Values(1));
            feeder.feed(new Values(1));
            feeder.feed(new Values(1));
            Testing.trackedWait(tracked, 3);
            feeder.assertNumAcks(3);
        }
    }

    private Map<Object, Object> listToMap(List<Object> list) {
        assertThat(list.size() % 2, is(0));
        Map<Object, Object> res = new HashMap<>();
        for (int i = 0; i < list.size(); i += 2) {
            res.put(list.get(i), list.get(i + 1));
        }
        return res;
    }

    @Test
    public void testKryoDecoratorsConfig() throws Exception {
        Map<String, Object> daemonConf = new HashMap<>();
        daemonConf.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, true);
        daemonConf.put(Config.TOPOLOGY_KRYO_DECORATORS, "this-is-overridden");
        try (LocalCluster cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withDaemonConf(daemonConf)
            .build()) {
            TopologyBuilder topologyBuilder = new TopologyBuilder();
            topologyBuilder.setSpout("1", new TestPlannerSpout(new Fields("conf")));
            topologyBuilder.setBolt("2", new TestConfBolt(Collections.singletonMap(Config.TOPOLOGY_KRYO_DECORATORS, Arrays.asList("one", "two"))))
                .shuffleGrouping("1");

            List<FixedTuple> testTuples = Arrays.asList(new Values(Config.TOPOLOGY_KRYO_DECORATORS)).stream()
                .map(value -> new FixedTuple(value))
                .collect(Collectors.toList());

            MockedSources mockedSources = new MockedSources(Collections.singletonMap("1", testTuples));

            CompleteTopologyParam completeTopologyParams = new CompleteTopologyParam();
            completeTopologyParams.setMockedSources(mockedSources);
            completeTopologyParams.setStormConf(Collections.singletonMap(Config.TOPOLOGY_KRYO_DECORATORS, Arrays.asList("one", "three")));

            Map<String, List<FixedTuple>> results = Testing.completeTopology(cluster, topologyBuilder.createTopology(), completeTopologyParams);

            List<Object> concatValues = Testing.readTuples(results, "2").stream()
                .flatMap(values -> values.stream())
                .collect(Collectors.toList());
            assertThat(concatValues.get(0), is(Config.TOPOLOGY_KRYO_DECORATORS));
            assertThat(concatValues.get(1), is(Arrays.asList("one", "two", "three")));
        }
    }

    @Test
    public void testComponentSpecificConfig() throws Exception {
        Map<String, Object> daemonConf = new HashMap<>();
        daemonConf.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, true);
        try (LocalCluster cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withDaemonConf(daemonConf)
            .build()) {
            TopologyBuilder topologyBuilder = new TopologyBuilder();
            topologyBuilder.setSpout("1", new TestPlannerSpout(new Fields("conf")));
            Map<String, Object> componentConf = new HashMap<>();
            componentConf.put("fake.config", 123);
            componentConf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 20);
            componentConf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 30);
            componentConf.put(Config.TOPOLOGY_KRYO_REGISTER, Arrays.asList(Collections.singletonMap("fake.type", "bad.serializer"), Collections.singletonMap("fake.type2", "a.serializer")));
            topologyBuilder.setBolt("2", new TestConfBolt(componentConf))
                .shuffleGrouping("1")
                .setMaxTaskParallelism(2)
                .addConfiguration("fake.config2", 987);

            List<FixedTuple> testTuples = Arrays.asList("fake.config", Config.TOPOLOGY_MAX_TASK_PARALLELISM, Config.TOPOLOGY_MAX_SPOUT_PENDING, "fake.config2", Config.TOPOLOGY_KRYO_REGISTER).stream()
                .map(value -> new FixedTuple(new Values(value)))
                .collect(Collectors.toList());
            Map<String, String> kryoRegister = new HashMap<>();
            kryoRegister.put("fake.type", "good.serializer");
            kryoRegister.put("fake.type3", "a.serializer3");
            Map<String, Object> stormConf = new HashMap<>();
            stormConf.put(Config.TOPOLOGY_KRYO_REGISTER, Arrays.asList(kryoRegister));

            MockedSources mockedSources = new MockedSources(Collections.singletonMap("1", testTuples));

            CompleteTopologyParam completeTopologyParams = new CompleteTopologyParam();
            completeTopologyParams.setMockedSources(mockedSources);
            completeTopologyParams.setStormConf(stormConf);

            Map<String, List<FixedTuple>> results = Testing.completeTopology(cluster, topologyBuilder.createTopology(), completeTopologyParams);

            Map<String, Object> expectedValues = new HashMap<>();
            expectedValues.put("fake.config", 123L);
            expectedValues.put("fake.config2", 987L);
            expectedValues.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 2L);
            expectedValues.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 30L);
            Map<String, String> expectedKryoRegister = new HashMap<>();
            expectedKryoRegister.putAll(kryoRegister);
            expectedKryoRegister.put("fake.type2", "a.serializer");
            expectedValues.put(Config.TOPOLOGY_KRYO_REGISTER, expectedKryoRegister);
            List<Object> concatValues = Testing.readTuples(results, "2").stream()
                .flatMap(values -> values.stream())
                .collect(Collectors.toList());
            assertThat(listToMap(concatValues), is(expectedValues));
        }
    }
    
    private static class HooksBolt extends BaseRichBolt {

        private int acked = 0;
        private int failed = 0;
        private int executed = 0;
        private int emitted = 0;
        private OutputCollector collector;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("emit", "ack", "fail", "executed"));
        }

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            context.addTaskHook(new BaseTaskHook() {
                @Override
                public void boltExecute(BoltExecuteInfo info) {
                    executed++;
                }

                @Override
                public void boltFail(BoltFailInfo info) {
                    failed++;
                }

                @Override
                public void boltAck(BoltAckInfo info) {
                    acked++;
                }

                @Override
                public void emit(EmitInfo info) {
                    emitted++;
                }
                
            });
        }

        @Override
        public void execute(Tuple input) {
            collector.emit(new Values(emitted, acked, failed, executed));
            if (acked - failed == 0) {
                collector.ack(input);
            } else {
                collector.fail(input);
            }
        }
    }
    
    @Test
    public void testHooks() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .build()) {
            Map<String, SpoutDetails> spoutMap = Collections.singletonMap("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(new Fields("conf"))));

            Map<String, BoltDetails> boltMap = Collections.singletonMap("2",
                Thrift.prepareBoltDetails(
                    Collections.singletonMap(Utils.getGlobalStreamId("1", null), Thrift.prepareShuffleGrouping()),
                    new HooksBolt()));
            StormTopology topology = Thrift.buildTopology(spoutMap, boltMap);

            List<FixedTuple> testTuples = Arrays.asList(1, 1, 1, 1).stream()
                .map(value -> new FixedTuple(new Values(value)))
                .collect(Collectors.toList());

            MockedSources mockedSources = new MockedSources(Collections.singletonMap("1", testTuples));

            CompleteTopologyParam completeTopologyParams = new CompleteTopologyParam();
            completeTopologyParams.setMockedSources(mockedSources);

            Map<String, List<FixedTuple>> results = Testing.completeTopology(cluster, topology, completeTopologyParams);
            
            List<List<Object>> expectedTuples = Arrays.asList(
                Arrays.asList(0, 0, 0, 0),
                Arrays.asList(2, 1, 0, 1),
                Arrays.asList(4, 1, 1, 2),
                Arrays.asList(6, 2, 1, 3));

            assertThat(Testing.readTuples(results, "2"), is(expectedTuples));
        }
    }

}
