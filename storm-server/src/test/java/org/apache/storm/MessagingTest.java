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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.CompleteTopologyParam;
import org.apache.storm.testing.FixedTuple;
import org.apache.storm.testing.MockedSources;
import org.apache.storm.testing.TestGlobalCount;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.junit.Assert;
import org.junit.Test;

public class MessagingTest {

    @Test
    public void testLocalTransport() throws Exception {
        Config topoConf = new Config();
        topoConf.put(Config.TOPOLOGY_WORKERS, 2);
        topoConf.put(Config.STORM_MESSAGING_TRANSPORT, "org.apache.storm.messaging.netty.Context");

        try (ILocalCluster cluster = new LocalCluster.Builder().withSimulatedTime()
                                                               .withSupervisors(1).withPortsPerSupervisor(2)
                                                               .withDaemonConf(topoConf).build()) {

            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("1", new TestWordSpout(true), 2);
            builder.setBolt("2", new TestGlobalCount(), 6).shuffleGrouping("1");
            StormTopology stormTopology = builder.createTopology();

            List<FixedTuple> fixedTuples = new ArrayList<>();
            for (int i = 0; i < 12; i++) {
                fixedTuples.add(new FixedTuple(Collections.singletonList("a")));
                fixedTuples.add(new FixedTuple(Collections.singletonList("b")));
            }
            Map<String, List<FixedTuple>> data = new HashMap<>();
            data.put("1", fixedTuples);
            MockedSources mockedSources = new MockedSources(data);
            CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
            completeTopologyParam.setMockedSources(mockedSources);
            Map<String, List<FixedTuple>> results = Testing.completeTopology(cluster, stormTopology, completeTopologyParam);
            Assert.assertEquals(6 * 4, Testing.readTuples(results, "2").size());
        }
    }
    
    @Test
    public void testRemoteTransportWithManyTasksInReceivingExecutor() throws Exception {
        //STORM-3141 regression test
        //Verify that remote worker can handle many tasks in one executor
        Config topoConf = new Config();
        topoConf.put(Config.TOPOLOGY_WORKERS, 2);
        topoConf.put(Config.STORM_MESSAGING_TRANSPORT, "org.apache.storm.messaging.netty.Context");

        try (ILocalCluster cluster = new LocalCluster.Builder().withSimulatedTime()
                                                               .withSupervisors(1).withPortsPerSupervisor(2)
                                                               .withDaemonConf(topoConf).build()) {

            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("1", new TestWordSpout(true), 1);
            builder.setBolt("2", new TestGlobalCount(), 1)
                .setNumTasks(10)
                .shuffleGrouping("1");
            StormTopology stormTopology = builder.createTopology();

            List<FixedTuple> fixedTuples = new ArrayList<>();
            for (int i = 0; i < 12; i++) {
                fixedTuples.add(new FixedTuple(Collections.singletonList("a")));
                fixedTuples.add(new FixedTuple(Collections.singletonList("b")));
            }
            Map<String, List<FixedTuple>> data = new HashMap<>();
            data.put("1", fixedTuples);
            MockedSources mockedSources = new MockedSources(data);
            CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
            completeTopologyParam.setMockedSources(mockedSources);
            Map<String, List<FixedTuple>> results = Testing.completeTopology(cluster, stormTopology, completeTopologyParam);
            Assert.assertEquals(6 * 4, Testing.readTuples(results, "2").size());
        }
    }
}
