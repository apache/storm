/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.grouping;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.Thrift;
import org.apache.storm.daemon.GrouperFactory;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.JavaObject;
import org.apache.storm.generated.JavaObjectArg;
import org.apache.storm.generated.NullStruct;
import org.apache.storm.testing.CompleteTopologyParam;
import org.apache.storm.testing.FixedTuple;
import org.apache.storm.testing.MockedSources;
import org.apache.storm.testing.NGrouping;
import org.apache.storm.testing.TestWordBytesCounter;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.tuple.Values;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for shuffle, field, and custom grouping behaviors.
 *
 * Ported from storm-core/test/clj/org/apache/storm/grouping_test.clj
 */
public class GroupingIntegrationTest {

    static class IdBolt extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            collector.emit(tuple.getValues());
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("val"));
        }
    }

    @Test
    public void testShuffle() {
        Grouping shuffleGrouping = Grouping.shuffle(new NullStruct());
        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING, true);

        LoadAwareCustomStreamGrouping shuffler = GrouperFactory.mkGrouper(
            null, "comp", "stream", null, shuffleGrouping,
            Arrays.asList(1, 2), topoConf);

        int numMessages = 100000;
        int minPrcnt = (int) (numMessages * 0.49);
        int maxPrcnt = (int) (numMessages * 0.51);

        Map<List<Integer>, Integer> freq = new HashMap<>();
        List<Object> data = Arrays.asList(1, 2);
        for (int i = 0; i < numMessages; i++) {
            List<Integer> result = shuffler.chooseTasks(1, data);
            freq.merge(result, 1, Integer::sum);
        }

        int load1 = freq.getOrDefault(List.of(1), 0);
        int load2 = freq.getOrDefault(List.of(2), 0);
        assertTrue(load1 >= minPrcnt, "load1=" + load1 + " should be >= " + minPrcnt);
        assertTrue(load1 <= maxPrcnt, "load1=" + load1 + " should be <= " + maxPrcnt);
        assertTrue(load2 >= minPrcnt, "load2=" + load2 + " should be >= " + minPrcnt);
        assertTrue(load2 <= maxPrcnt, "load2=" + load2 + " should be <= " + maxPrcnt);
    }

    @Test
    public void testFieldGrouping() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(4)
                .build()) {

            int spoutPhint = 4;
            int boltPhint = 6;

            var topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestWordSpout(true), spoutPhint)),
                Map.of("2", Thrift.prepareBoltDetails(
                    Map.of(Utils.getGlobalStreamId("1", null), Thrift.prepareFieldsGrouping(List.of("word"))),
                    new TestWordBytesCounter(), boltPhint)));

            // Build mocked source data: repeat [bytes("a"), bytes("b")] spoutPhint*boltPhint times
            MockedSources mockedSources = new MockedSources();
            Values[] sourceData = new Values[spoutPhint * boltPhint * 2];
            for (int i = 0; i < spoutPhint * boltPhint; i++) {
                sourceData[i * 2] = new Values((Object) "a".getBytes());
                sourceData[i * 2 + 1] = new Values((Object) "b".getBytes());
            }
            mockedSources.addMockData("1", sourceData);

            CompleteTopologyParam param = new CompleteTopologyParam();
            param.setMockedSources(mockedSources);

            Map<String, List<FixedTuple>> results = Testing.completeTopology(cluster, topology, param);

            // Expected: for each word, counts from 1 to spoutPhint*boltPhint
            List<List<Object>> expected = new ArrayList<>();
            for (String value : List.of("a", "b")) {
                for (int sum = 1; sum <= spoutPhint * boltPhint; sum++) {
                    expected.add(List.of(value, sum));
                }
            }

            assertTrue(Testing.multiseteq(expected, Testing.readTuples(results, "2")));
        }
    }

    @Test
    public void testCustomGroupings() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(4)
                .build()) {

            var topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestWordSpout(true))),
                Map.of(
                    "2", Thrift.prepareBoltDetails(
                        Map.of(Utils.getGlobalStreamId("1", null),
                            Thrift.prepareCustomStreamGrouping(new NGrouping(2))),
                        new IdBolt(), 4),
                    "3", Thrift.prepareBoltDetails(
                        Map.of(Utils.getGlobalStreamId("1", null),
                            Thrift.prepareCustomJavaObjectGrouping(
                                new JavaObject("org.apache.storm.testing.NGrouping",
                                    List.of(JavaObjectArg.int_arg(3))))),
                        new IdBolt(), 6)));

            MockedSources mockedSources = new MockedSources();
            mockedSources.addMockData("1", new Values("a"), new Values("b"));

            CompleteTopologyParam param = new CompleteTopologyParam();
            param.setMockedSources(mockedSources);

            Map<String, List<FixedTuple>> results = Testing.completeTopology(cluster, topology, param);

            // NGrouping(2) sends each tuple to 2 tasks
            assertTrue(Testing.multiseteq(
                List.of(List.of("a"), List.of("a"), List.of("b"), List.of("b")),
                Testing.readTuples(results, "2")));

            // NGrouping(3) sends each tuple to 3 tasks
            assertTrue(Testing.multiseteq(
                List.of(List.of("a"), List.of("a"), List.of("a"),
                    List.of("b"), List.of("b"), List.of("b")),
                Testing.readTuples(results, "3")));
        }
    }
}
