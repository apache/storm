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

package org.apache.storm.hdfs.spout;

import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SpoutStats;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HdfsSpoutTopology {

    public static final String SPOUT_ID = "hdfsspout";
    public static final String BOLT_ID = "constbolt";

    /**
     * Copies text file content from sourceDir to destinationDir. Moves source files into sourceDir after its done consuming
     */
    public static void main(String[] args) throws Exception {
        // 0 - validate args
        if (args.length < 7) {
            System.err.println("Please check command line arguments.");
            System.err.println("Usage :");
            System.err.println(
                HdfsSpoutTopology.class.toString() + " topologyName hdfsUri fileFormat sourceDir sourceArchiveDir badDir destinationDir.");
            System.err.println(" topologyName - topology name.");
            System.err.println(" hdfsUri - hdfs name node URI");
            System.err.println(" fileFormat -  Set to 'TEXT' for reading text files or 'SEQ' for sequence files.");
            System.err.println(" sourceDir  - read files from this HDFS dir using HdfsSpout.");
            System.err.println(" archiveDir - after a file in sourceDir is read completely, it is moved to this HDFS location.");
            System.err.println(" badDir - files that cannot be read properly will be moved to this HDFS location.");
            System.err.println(" spoutCount - Num of spout instances.");
            System.err.println();
            System.exit(-1);
        }

        // 1 - parse cmd line args
        String hdfsUri = args[1];
        String fileFormat = args[2];
        String sourceDir = args[3];
        String archiveDir = args[4];
        String badDir = args[5];

        // 2 - Create and configure topology
        Config conf = new Config();
        conf.setNumWorkers(1);
        conf.setNumAckers(1);
        conf.setMaxTaskParallelism(1);
        conf.setDebug(true);
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class);

        TopologyBuilder builder = new TopologyBuilder();
        HdfsSpout spout = new HdfsSpout().withOutputFields(TextFileReader.defaultFields)
                .setReaderType(fileFormat)
                .setHdfsUri(hdfsUri)
                .setSourceDir(sourceDir)
                .setArchiveDir(archiveDir)
                .setBadFilesDir(badDir);
        int spoutNum = Integer.parseInt(args[6]);
        builder.setSpout(SPOUT_ID, spout, spoutNum);
        ConstBolt bolt = new ConstBolt();
        builder.setBolt(BOLT_ID, bolt, 1).shuffleGrouping(SPOUT_ID);

        // 3 - submit topology, wait for a few min and terminate it
        Map<String, Object> clusterConf = Utils.readStormConfig();
        String topologyName = args[0];
        StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, builder.createTopology());
        Nimbus.Iface client = NimbusClient.getConfiguredClient(clusterConf).getClient();

        // 4 - Print metrics every 30 sec, kill topology after 20 min
        for (int i = 0; i < 40; i++) {
            Thread.sleep(30 * 1000);
            printMetrics(client, topologyName);
        }
        kill(client, topologyName);
    } // main

    private static void kill(Nimbus.Iface client, String topologyName) throws Exception {
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        client.killTopologyWithOpts(topologyName, opts);
    }

    static void printMetrics(Nimbus.Iface client, String name) throws Exception {
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

    public static class ConstBolt extends BaseRichBolt {
        public static final String FIELDS = "message";
        private static final long serialVersionUID = -5313598399155365865L;
        private static final Logger log = LoggerFactory.getLogger(ConstBolt.class);
        int count = 0;
        private OutputCollector collector;

        public ConstBolt() {
        }

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            log.info("Received tuple : {}", tuple.getValue(0));
            count++;
            if (count == 3) {
                collector.fail(tuple);
            } else {
                collector.ack(tuple);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }
    } // class
}
