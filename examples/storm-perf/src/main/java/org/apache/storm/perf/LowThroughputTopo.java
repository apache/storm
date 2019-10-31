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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;
import org.slf4j.LoggerFactory;

public class LowThroughputTopo {
    private static final String SPOUT_ID = "ThrottledSpout";
    private static final String BOLT_ID = "LatencyPrintBolt";
    private static final Integer SPOUT_COUNT = 1;
    private static final Integer BOLT_COUNT = 1;
    private static final String SLEEP_MS = "sleep";

    static StormTopology getTopology(Map<String, Object> conf) {

        Long sleepMs = ObjectReader.getLong(conf.get(SLEEP_MS));
        // 1 -  Setup Spout   --------
        ThrottledSpout spout = new ThrottledSpout(sleepMs).withOutputFields(ThrottledSpout.DEFAULT_FIELD_NAME);

        // 2 -  Setup DevNull Bolt   --------
        LatencyPrintBolt bolt = new LatencyPrintBolt();


        // 3 - Setup Topology  --------
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT_ID, spout, Helper.getInt(conf, SPOUT_COUNT, 1));
        BoltDeclarer bd = builder.setBolt(BOLT_ID, bolt, Helper.getInt(conf, BOLT_COUNT, 1));

        bd.localOrShuffleGrouping(SPOUT_ID);
        //        bd.shuffleGrouping(SPOUT_ID);
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        int runTime = -1;
        Map<String, Object> topoConf = Utils.findAndReadConfigFile(args[1]);
        topoConf.put(Config.TOPOLOGY_SPOUT_RECVQ_SKIPS, 1);
        if (args.length > 0) {
            long sleepMs = Integer.parseInt(args[0]);
            topoConf.put(SLEEP_MS, sleepMs);
        }
        if (args.length > 1) {
            runTime = Integer.parseInt(args[1]);
        }
        if (args.length > 2) {
            System.err.println("args: spoutSleepMs [runDurationSec] ");
            return;
        }
        topoConf.putAll(Utils.readCommandLineOpts());
        //  Submit topology to storm cluster
        Helper.runOnClusterAndPrintMetrics(runTime, "LowThroughputTopo", topoConf, getTopology(topoConf));
    }

    private static class ThrottledSpout extends BaseRichSpout {

        static final String DEFAULT_FIELD_NAME = "time";
        private String fieldName = DEFAULT_FIELD_NAME;
        private SpoutOutputCollector collector = null;
        private long sleepTimeMs;

        ThrottledSpout(long sleepMs) {
            this.sleepTimeMs = sleepMs;
        }

        public ThrottledSpout withOutputFields(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(fieldName));
        }

        @Override
        public void open(Map<String, Object> conf, TopologyContext context,
                         SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            Long now = System.currentTimeMillis();
            List<Object> tuple = Collections.singletonList(now);
            collector.emit(tuple, now);
            Utils.sleep(sleepTimeMs);
        }

        @Override
        public void ack(Object msgId) {
            super.ack(msgId);
        }
    }

    private static class LatencyPrintBolt extends BaseRichBolt {
        private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LatencyPrintBolt.class);
        private OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> topoConf,
                            TopologyContext context,
                            OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            Long now = System.currentTimeMillis();
            Long then = (Long) tuple.getValues().get(0);
            LOG.warn("Latency {} ", now - then);
            System.err.println(now - then);
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }
}
