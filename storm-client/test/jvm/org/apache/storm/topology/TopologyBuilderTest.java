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

package org.apache.storm.topology;

import java.util.Map;
import java.util.Set;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.com.google.common.collect.ImmutableSet;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.state.State;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Tuple;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class TopologyBuilderTest {
    private final TopologyBuilder builder = new TopologyBuilder();

    @Test
    public void testSetRichBolt() {
        assertThrows(IllegalArgumentException.class, () -> builder.setBolt("bolt", mock(IRichBolt.class), 0));
    }

    @Test
    public void testSetBasicBolt() {
        assertThrows(IllegalArgumentException.class,
            () -> builder.setBolt("bolt", mock(IBasicBolt.class), 0));
    }

    @Test
    public void testSetSpout() {
        assertThrows(IllegalArgumentException.class,
            () -> builder.setSpout("spout", mock(IRichSpout.class), 0));
    }

    @Test
    public void testAddWorkerHook() {
        assertThrows(IllegalArgumentException.class, () -> builder.addWorkerHook(null));
    }

    @Test
    public void testStatefulTopology() {
        builder.setSpout("spout1", makeDummySpout());
        builder.setSpout("spout2", makeDummySpout());
        builder.setBolt("bolt1", makeDummyStatefulBolt(), 1)
               .shuffleGrouping("spout1").shuffleGrouping("spout2");
        builder.setBolt("bolt2", makeDummyStatefulBolt(), 1).shuffleGrouping("spout1");
        builder.setBolt("bolt3", makeDummyStatefulBolt(), 1)
               .shuffleGrouping("bolt1").shuffleGrouping("bolt2");
        StormTopology topology = builder.createTopology();

        assertNotNull(topology);
        Set<String> spouts = topology.get_spouts().keySet();
        // checkpoint spout should 've been added
        assertEquals(ImmutableSet.of("spout1", "spout2", "$checkpointspout"), spouts);
        // bolt1, bolt2 should also receive from checkpoint spout
        assertEquals(ImmutableSet.of(new GlobalStreamId("spout1", "default"),
                                            new GlobalStreamId("spout2", "default"),
                                            new GlobalStreamId("$checkpointspout", "$checkpoint")),
                            topology.get_bolts().get("bolt1").get_common().get_inputs().keySet());
        assertEquals(ImmutableSet.of(new GlobalStreamId("spout1", "default"),
                                            new GlobalStreamId("$checkpointspout", "$checkpoint")),
                            topology.get_bolts().get("bolt2").get_common().get_inputs().keySet());
        // bolt3 should also receive from checkpoint streams of bolt1, bolt2
        assertEquals(ImmutableSet.of(new GlobalStreamId("bolt1", "default"),
                                            new GlobalStreamId("bolt1", "$checkpoint"),
                                            new GlobalStreamId("bolt2", "default"),
                                            new GlobalStreamId("bolt2", "$checkpoint")),
                            topology.get_bolts().get("bolt3").get_common().get_inputs().keySet());
    }

    private IRichSpout makeDummySpout() {
        return new BaseRichSpout() {
            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {}

            @Override
            public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {}

            @Override
            public void nextTuple() {}

            private void writeObject(java.io.ObjectOutputStream stream) {}
        };
    }

    private IStatefulBolt makeDummyStatefulBolt() {
        return new BaseStatefulBolt() {
            @Override
            public void execute(Tuple input) {}

            @Override
            public void initState(State state) {}

            private void writeObject(java.io.ObjectOutputStream stream) {}
        };
    }
}
