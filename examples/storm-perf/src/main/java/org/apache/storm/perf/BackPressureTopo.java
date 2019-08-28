/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.storm.perf;

import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.perf.spout.ConstSpout;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;
import org.slf4j.LoggerFactory;


public class BackPressureTopo {

    private static final String SPOUT_ID = "ConstSpout";
    private static final String BOLT_ID = "ThrottledBolt";
    private static final Integer SPOUT_COUNT = 1;
    private static final Integer BOLT_COUNT = 1;
    private static final String SLEEP_MS = "sleep";

    static StormTopology getTopology(Map<String, Object> conf) {

        Long sleepMs = ObjectReader.getLong(conf.get(SLEEP_MS));
        // 1 -  Setup Spout   --------
        ConstSpout spout = new ConstSpout("some data").withOutputFields("string");

        // 2 -  Setup DevNull Bolt   --------
        ThrottledBolt bolt = new ThrottledBolt(sleepMs);


        // 3 - Setup Topology  --------
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT_ID, spout, Helper.getInt(conf, SPOUT_COUNT, 1));
        BoltDeclarer bd = builder.setBolt(BOLT_ID, bolt, Helper.getInt(conf, BOLT_COUNT, 1));

        bd.localOrShuffleGrouping(SPOUT_ID);
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        int runTime = -1;
        Config topoConf = new Config();
        topoConf.put(Config.TOPOLOGY_SPOUT_RECVQ_SKIPS, 1);
        topoConf.putAll(Utils.readCommandLineOpts());
        if (args.length > 0) {
            long sleepMs = Integer.parseInt(args[0]);
            topoConf.put(SLEEP_MS, sleepMs);
        }
        if (args.length > 1) {
            runTime = Integer.parseInt(args[1]);
        }
        if (args.length > 2) {
            System.err.println("args: boltSleepMs [runDurationSec] ");
            return;
        }
        //  Submit topology to storm cluster
        Helper.runOnClusterAndPrintMetrics(runTime, "BackPressureTopo", topoConf, getTopology(topoConf));
    }

    private static class ThrottledBolt extends BaseRichBolt {
        private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ThrottledBolt.class);
        private OutputCollector collector;
        private long sleepMs;

        ThrottledBolt(Long sleepMs) {
            this.sleepMs = sleepMs;
        }

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            collector.ack(tuple);
            LOG.debug("Sleeping");
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                //.. ignore
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }
}
