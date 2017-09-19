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

import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TickTupleTest {

    @Test
    public void testTickTupleWorksWithSystemBolt() throws Exception {
        ILocalCluster cluster = null;
        try {
            cluster =  new LocalCluster.Builder().withSimulatedTime().withNimbusDaemon(true).build();
            StormTopology topology = createNoOpTopology();
            Config topoConf = new Config();
            topoConf.putAll(Utils.readDefaultConfig());
            topoConf.put("storm.cluster.mode", "local");
            topoConf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
            cluster.submitTopology("test", topoConf,  topology);
            cluster.advanceClusterTime(2);
            Assert.assertTrue("Test is passed", true);
        } finally {
            cluster.close();
        }

    }

    private IRichSpout makeNoOpSpout() {
        return new BaseRichSpout() {
            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
                declarer.declare(new Fields("tuple"));
            }

            @Override
            public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            }

            @Override
            public void nextTuple() {
            }

            private void writeObject(java.io.ObjectOutputStream stream) {
            }
        };
    }

    private BaseRichBolt makeNoOpBolt() {
        return new BaseRichBolt() {
            @Override
            public void prepare(Map<String, Object> conf, TopologyContext topologyContext, OutputCollector outputCollector) {}
            @Override
            public void execute(Tuple tuple) {}

            @Override
            public void cleanup() { }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer ofd) {}

            private void writeObject(java.io.ObjectOutputStream stream) {}
        };
    }

    private StormTopology createNoOpTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("1", makeNoOpSpout());
        builder.setBolt("2", makeNoOpBolt()).fieldsGrouping("1", new Fields("tuple"));
        return builder.createTopology();
    }
}
