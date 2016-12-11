/**
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

package org.apache.storm;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * Created by ruchi on 29-11-2016.
 */
public class MessagingTest {

    @Test
    public void testLocalTransport() throws Exception {
        Config stormConf = new Config();
        stormConf.putAll(Utils.readDefaultConfig());
        stormConf.put(Config.TOPOLOGY_WORKERS, 2);
        stormConf.put(Config.STORM_LOCAL_MODE_ZMQ, true);
        List<String> seeds = new ArrayList<>();
        seeds.add("localhost");
        stormConf.put(Config.NIMBUS_HOST, "localhost");
        stormConf.put(Config.NIMBUS_SEEDS, seeds);
        stormConf.put("storm.cluster.mode", "local");
        stormConf.put(Config.STORM_LOCAL_HOSTNAME, "localhost");
        stormConf.put(Config.STORM_MESSAGING_TRANSPORT , "org.apache.storm.messaging.netty.Context");
        ILocalCluster cluster = new LocalCluster.Builder().withSimulatedTime().withSupervisors(1).withPortsPerSupervisor(2)
                .withDaemonConf(stormConf).withNimbusDaemon(true).build();
        Thrift.SpoutDetails spoutDetails = Thrift.prepareSpoutDetails(new TestWordSpout(false), 2);
        Map<GlobalStreamId, Grouping> inputs = new HashMap<>();
        inputs.put(Utils.getGlobalStreamId("1", null), Thrift.prepareShuffleGrouping());
        Thrift.BoltDetails boltDetails = Thrift.prepareBoltDetails(inputs, new TestGlobalCount(), 6);
        Map<String, Thrift.SpoutDetails> spoutMap = new HashMap<>();
        spoutMap.put("1", spoutDetails);
        Map<String, Thrift.BoltDetails> boltMap = new HashMap<>();
        boltMap.put("2", boltDetails);
        //StormTopology stormTopology = Thrift.buildTopology(spoutMap, boltMap);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("1", new TestWordSpout(false), 2);
        builder.setBolt("2", new TestGlobalCount(), 6).shuffleGrouping("1");
        StormTopology stormTopology = builder.createTopology();
        FixedTuple[] fixedTuple = {new FixedTuple(( List<Object>)Arrays.asList((Object)"a")),new FixedTuple(( List<Object>)Arrays.asList((Object)"b")),
                new FixedTuple(( List<Object>)Arrays.asList((Object)"a")),new FixedTuple(( List<Object>)Arrays.asList((Object)"b")),
                new FixedTuple(( List<Object>)Arrays.asList((Object)"a")),new FixedTuple(( List<Object>)Arrays.asList((Object)"b")),
                new FixedTuple(( List<Object>)Arrays.asList((Object)"a")),new FixedTuple(( List<Object>)Arrays.asList((Object)"b")),
                new FixedTuple(( List<Object>)Arrays.asList((Object)"a")),new FixedTuple(( List<Object>)Arrays.asList((Object)"b")),
                new FixedTuple(( List<Object>)Arrays.asList((Object)"a")),new FixedTuple(( List<Object>)Arrays.asList((Object)"b")),
                new FixedTuple(( List<Object>)Arrays.asList((Object)"a")),new FixedTuple(( List<Object>)Arrays.asList((Object)"b")),
                new FixedTuple(( List<Object>)Arrays.asList((Object)"a")),new FixedTuple(( List<Object>)Arrays.asList((Object)"b")),
                new FixedTuple(( List<Object>)Arrays.asList((Object)"a")),new FixedTuple(( List<Object>)Arrays.asList((Object)"b")),
                new FixedTuple(( List<Object>)Arrays.asList((Object)"a")),new FixedTuple(( List<Object>)Arrays.asList((Object)"b")),
                new FixedTuple(( List<Object>)Arrays.asList((Object)"a")),new FixedTuple(( List<Object>)Arrays.asList((Object)"b")),
                new FixedTuple(( List<Object>)Arrays.asList((Object)"a")),new FixedTuple(( List<Object>)Arrays.asList((Object)"b"))};
        Map<String, List<FixedTuple>> data = new HashMap<>();
        data.put("1", (List<FixedTuple>)Arrays.asList(fixedTuple));
        MockedSources mockedSources = new MockedSources(data);
        CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
        completeTopologyParam.setMockedSources(mockedSources);
        Map<String, List<FixedTuple>> results = Testing.completeTopology(cluster, stormTopology, completeTopologyParam);
        Assert.assertEquals(6*4, Testing.readTuples(results,"2").size());

    }

}
