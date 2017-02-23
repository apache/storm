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


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.perf.bolt.CountBolt;
import org.apache.storm.perf.bolt.SplitSentenceBolt;
import org.apache.storm.perf.spout.FileReadSpout;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;


import java.util.Map;

/***
 * This topo helps measure speed of word count.
 *  Spout loads a file into memory on initialization, then emits the lines in an endless loop.
 */

public class FileReadWordCountTopo {
    public static final String SPOUT_ID =   "spout";
    public static final String COUNT_ID =   "counter";
    public static final String SPLIT_ID =   "splitter";
    public static final String TOPOLOGY_NAME = "FileReadWordCountTopo";

    // Config settings
    public static final String SPOUT_NUM =  "spout.count";
    public static final String SPLIT_NUM =  "splitter.count";
    public static final String COUNT_NUM =  "counter.count";
    public static final String INPUT_FILE = "input.file";

    public static final int DEFAULT_SPOUT_NUM = 1;
    public static final int DEFAULT_SPLIT_BOLT_NUM = 2;
    public static final int DEFAULT_COUNT_BOLT_NUM = 2;


    public static StormTopology getTopology(Map config) {

        final int spoutNum = Helper.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int spBoltNum = Helper.getInt(config, SPLIT_NUM, DEFAULT_SPLIT_BOLT_NUM);
        final int cntBoltNum = Helper.getInt(config, COUNT_NUM, DEFAULT_COUNT_BOLT_NUM);
        final String inputFile = Helper.getStr(config, INPUT_FILE);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new FileReadSpout(inputFile), spoutNum);
        builder.setBolt(SPLIT_ID, new SplitSentenceBolt(), spBoltNum).localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(COUNT_ID, new CountBolt(), cntBoltNum).fieldsGrouping(SPLIT_ID, new Fields(SplitSentenceBolt.FIELDS));

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        if(args.length <= 0) {
            // For IDE based profiling ... submit topology to local cluster
            Config conf = new Config();
            conf.put(INPUT_FILE, "resources/randomwords.txt");
            LocalCluster cluster = Helper.runOnLocalCluster(TOPOLOGY_NAME, getTopology(conf));

            Helper.setupShutdownHook(cluster, TOPOLOGY_NAME);
            while (true) {//  run indefinitely till Ctrl-C
                Thread.sleep(20_000_000);
            }
        } else {
            //  Submit to Storm cluster
            if (args.length !=2) {
                System.err.println("args: runDurationSec  confFile");
                return;
            }
            Integer durationSec = Integer.parseInt(args[0]);
            Map topoConf = Utils.findAndReadConfigFile(args[1]);

            Helper.runOnClusterAndPrintMetrics(durationSec, TOPOLOGY_NAME, topoConf, getTopology(topoConf));

        }
    }
}
