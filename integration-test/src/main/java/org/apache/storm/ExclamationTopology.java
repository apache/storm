/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.st.topology.TestableTopology;
import org.apache.storm.st.utils.TimeUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology {

    public static final String WORD = "word";
    public static final String EXCLAIM_1 = "exclaim1";
    public static final String EXCLAIM_2 = "exclaim2";
    public static final int SPOUT_EXECUTORS = 10;
    public static final int EXCLAIM_2_EXECUTORS = 2;

    public static class ExclamationBolt extends BaseRichBolt {

        OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class FixedOrderWordSpout extends BaseRichSpout {

        public static final List<String> WORDS = Collections.unmodifiableList(Arrays.asList("nathan",
                "mike",
                "jackson",
                "golda",
                "bertels"));

        private SpoutOutputCollector collector;
        private int currentIndex = 0;
        private int numEmitted = 0;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            if (numEmitted >= TestableTopology.MAX_SPOUT_EMITS) {
                //Stop emitting at a certain point, because log rolling breaks the tests.
                return;
            }
            //Sleep a bit to avoid hogging the CPU.
            TimeUtil.sleepMilliSec(1);
            collector.emit(new Values(WORDS.get((currentIndex++) % WORDS.size())));
            ++numEmitted;
        }

    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setDebug(true);
        String topoName = "test";
        if (args.length > 0) {
            topoName = args[0];
        }

        conf.setNumWorkers(3);
        StormTopology topology = getStormTopology();
        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, topology);
    }

    public static StormTopology getStormTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(WORD, new FixedOrderWordSpout(), SPOUT_EXECUTORS);
        builder.setBolt(EXCLAIM_1, new ExclamationTopology.ExclamationBolt(), 3).shuffleGrouping(WORD);
        builder.setBolt(EXCLAIM_2, new ExclamationTopology.ExclamationBolt(), EXCLAIM_2_EXECUTORS).shuffleGrouping(EXCLAIM_1);
        return builder.createTopology();
    }
}
