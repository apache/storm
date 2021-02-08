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

package org.apache.storm.starter;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SpoutStats;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

/**
 * WordCount but the spout does not stop, and the bolts are implemented in
 * java.  This can show how fast the word count can run.
 */
public class FastWordCountTopology {

    public static void printMetrics(Nimbus.Iface client, String name) throws Exception {
        TopologyInfo info = client.getTopologyInfoByName(name);
        int uptime = info.get_uptime_secs();
        long acked = 0;
        long failed = 0;
        double weightedAvgTotal = 0.0;
        for (ExecutorSummary exec : info.get_executors()) {
            if ("spout".equals(exec.get_component_id())) {
                SpoutStats stats = exec.get_stats().get_specific().get_spout();
                Map<String, Long> failedMap = stats.get_failed().get(":all-time");
                Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
                Map<String, Double> avgLatMap = stats.get_complete_ms_avg().get(":all-time");
                for (String key : ackedMap.keySet()) {
                    if (failedMap != null) {
                        Long tmp = failedMap.get(key);
                        if (tmp != null) {
                            failed += tmp;
                        }
                    }
                    long ackVal = ackedMap.get(key);
                    double latVal = avgLatMap.get(key) * ackVal;
                    acked += ackVal;
                    weightedAvgTotal += latVal;
                }
            }
        }
        double avgLatency = weightedAvgTotal / acked;
        System.out.println("uptime: " + uptime
                + " acked: " + acked
                + " avgLatency: " + avgLatency
                + " acked/sec: " + (((double) acked) / uptime + " failed: " + failed));
    }

    public static void kill(Nimbus.Iface client, String name) throws Exception {
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        client.killTopologyWithOpts(name, opts);
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new FastRandomSentenceSpout(), 4);

        builder.setBolt("split", new SplitSentence(), 4).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), 4).fieldsGrouping("split", new Fields("word"));

        Config conf = new Config();
        conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class);

        String name = "wc-test";
        if (args != null && args.length > 0) {
            name = args[0];
        }

        conf.setNumWorkers(1);
        StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());

        Map<String, Object> clusterConf = Utils.readStormConfig();
        clusterConf.putAll(Utils.readCommandLineOpts());
        Nimbus.Iface client = NimbusClient.getConfiguredClient(clusterConf).getClient();

        //Sleep for 5 mins
        for (int i = 0; i < 10; i++) {
            Thread.sleep(30 * 1000);
            printMetrics(client, name);
        }
        kill(client, name);
    }

    public static class FastRandomSentenceSpout extends BaseRichSpout {
        private static final String[] CHOICES = {
            "marry had a little lamb whos fleese was white as snow",
            "and every where that marry went the lamb was sure to go",
            "one two three four five six seven eight nine ten",
            "this is a test of the emergency broadcast system this is only a test",
            "peter piper picked a peck of pickeled peppers"
        };
        SpoutOutputCollector collector;
        Random rand;

        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            rand = ThreadLocalRandom.current();
        }

        @Override
        public void nextTuple() {
            String sentence = CHOICES[rand.nextInt(CHOICES.length)];
            collector.emit(new Values(sentence), sentence);
        }

        @Override
        public void ack(Object id) {
            //Ignored
        }

        @Override
        public void fail(Object id) {
            collector.emit(new Values(id), id);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }
    }

    public static class SplitSentence extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split("\\s+")) {
                collector.emit(new Values(word, 1));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static class WordCount extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null) {
                count = 0;
            }
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }
}
