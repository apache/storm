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

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.ILocalCluster.ILocalTopology;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.TupleUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TickTupleTest {
    private final static Logger LOG = LoggerFactory.getLogger(TickTupleTest.class);
    private static LinkedBlockingQueue<Long> tickTupleTimes = new LinkedBlockingQueue<>();
    private static AtomicReference<Tuple> nonTickTuple = new AtomicReference<>(null);

    @Test
    public void testTickTupleWorksWithSystemBolt() throws Exception {
        try (ILocalCluster cluster = new LocalCluster.Builder().withSimulatedTime().withNimbusDaemon(true).build()){
            StormTopology topology = createNoOpTopology();
            Config topoConf = new Config();
            topoConf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
            try (ILocalTopology topo = cluster.submitTopology("test", topoConf,  topology)) {
                //Give the topology some time to come up
                long time = 0;
                while (tickTupleTimes.size() <= 0) {
                    assert time <= 100_000 : "took over " + time + " ms of simulated time to get a message back...";
                    cluster.advanceClusterTime(10);
                    time += 10_000;
                }
                tickTupleTimes.clear();
                for (int i = 0; i < 5; i++) {
                    cluster.advanceClusterTime(1);
                    time += 1_000;
                    assertEquals("Iteration " + i, (Long)time, tickTupleTimes.poll(100, TimeUnit.MILLISECONDS));
                }
            }
            assertNull("The bolt got a tuple that is not a tick tuple " + nonTickTuple.get(), nonTickTuple.get());
        }
    }

    private static class NoopSpout extends BaseRichSpout {
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
    }

    private static class NoopBolt extends BaseRichBolt {
        @Override
        public void prepare(Map<String, Object> conf, TopologyContext topologyContext, OutputCollector outputCollector) {}

        @Override
        public void execute(Tuple tuple) {
            LOG.info("GOT {}", tuple);
            if (TupleUtils.isTick(tuple)) {
                try {
                    tickTupleTimes.put(Time.currentTimeMillis());
                } catch (InterruptedException e) {
                    //Ignored
                }
            } else {
                nonTickTuple.set(tuple);
            }
        }

        @Override
        public void cleanup() { }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {}
    }

    private StormTopology createNoOpTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Spout", new NoopSpout());
        builder.setBolt("Bolt", new NoopBolt()).fieldsGrouping("Spout", new Fields("tuple"));
        return builder.createTopology();
    }
}
