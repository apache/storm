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

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.storm.testing.CompleteTopologyParam;
import org.apache.storm.testing.FixedTuple;
import org.apache.storm.testing.IntegrationTest;
import org.apache.storm.testing.MkClusterParam;
import org.apache.storm.testing.MockedSources;
import org.apache.storm.testing.TestAggregatesCounter;
import org.apache.storm.testing.TestGlobalCount;
import org.apache.storm.testing.TestJob;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.Test;
/**
 * Test that the testing class does what it should do.
 */
@IntegrationTest
public class TestingTest {

    private static final TestJob COMPLETE_TOPOLOGY_TESTJOB = (cluster) -> {
        TopologyBuilder tb = new TopologyBuilder();
        tb.setSpout("spout", new TestWordSpout(true), 3);
        tb.setBolt("2", new TestWordCounter(), 4)
          .fieldsGrouping("spout", new Fields("word"));
        tb.setBolt("3", new TestGlobalCount())
          .globalGrouping("spout");
        tb.setBolt("4", new TestAggregatesCounter())
          .globalGrouping("2");

        MockedSources mocked = new MockedSources();
        mocked.addMockData("spout",
                           new Values("nathan"),
                           new Values("bob"),
                           new Values("joey"),
                           new Values("nathan"));

        Config topoConf = new Config();
        topoConf.setNumWorkers(2);

        CompleteTopologyParam ctp = new CompleteTopologyParam();
        ctp.setMockedSources(mocked);
        ctp.setStormConf(topoConf);

        Map<String, List<FixedTuple>> results = Testing.completeTopology(cluster, tb.createTopology(), ctp);
        List<List<Object>> spoutTuples = Testing.readTuples(results, "spout");
        List<List<Object>> expectedSpoutTuples = Arrays.asList(Arrays.asList("nathan"), Arrays.asList("bob"), Arrays.asList("joey"),
                                                               Arrays.asList("nathan"));
        assertTrue(expectedSpoutTuples + " expected, but found " + spoutTuples,
                   Testing.multiseteq(expectedSpoutTuples, spoutTuples));

        List<List<Object>> twoTuples = Testing.readTuples(results, "2");
        List<List<Object>> expectedTwoTuples = Arrays.asList(Arrays.asList("nathan", 1), Arrays.asList("nathan", 2),
                                                             Arrays.asList("bob", 1), Arrays.asList("joey", 1));
        assertTrue(expectedTwoTuples + " expected, but found " + twoTuples,
                   Testing.multiseteq(expectedTwoTuples, twoTuples));

        List<List<Object>> threeTuples = Testing.readTuples(results, "3");
        List<List<Object>> expectedThreeTuples = Arrays.asList(Arrays.asList(1), Arrays.asList(2),
                                                               Arrays.asList(3), Arrays.asList(4));
        assertTrue(expectedThreeTuples + " expected, but found " + threeTuples,
                   Testing.multiseteq(expectedThreeTuples, threeTuples));

        List<List<Object>> fourTuples = Testing.readTuples(results, "4");
        List<List<Object>> expectedFourTuples = Arrays.asList(Arrays.asList(1), Arrays.asList(2),
                                                              Arrays.asList(3), Arrays.asList(4));
        assertTrue(expectedFourTuples + " expected, but found " + fourTuples,
                   Testing.multiseteq(expectedFourTuples, fourTuples));
    };

    @Test
    public void testCompleteTopologyNettySimulated() throws Exception {
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, true);
        MkClusterParam param = new MkClusterParam();
        param.setSupervisors(4);
        param.setDaemonConf(daemonConf);

        Testing.withSimulatedTimeLocalCluster(param, COMPLETE_TOPOLOGY_TESTJOB);
    }

    @Test
    public void testCompleteTopologyNetty() throws Exception {
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, true);
        MkClusterParam param = new MkClusterParam();
        param.setSupervisors(4);
        param.setDaemonConf(daemonConf);

        Testing.withLocalCluster(param, COMPLETE_TOPOLOGY_TESTJOB);
    }

    @Test
    public void testCompleteTopologyLocalSimulated() throws Exception {
        MkClusterParam param = new MkClusterParam();
        param.setSupervisors(4);

        Testing.withSimulatedTimeLocalCluster(param, COMPLETE_TOPOLOGY_TESTJOB);
    }

    @Test
    public void testCompleteTopologyLocal() throws Exception {
        MkClusterParam param = new MkClusterParam();
        param.setSupervisors(4);

        Testing.withLocalCluster(param, COMPLETE_TOPOLOGY_TESTJOB);
    }

}