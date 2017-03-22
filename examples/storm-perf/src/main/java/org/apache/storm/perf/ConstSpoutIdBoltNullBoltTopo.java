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
import org.apache.storm.perf.bolt.DevNullBolt;
import org.apache.storm.perf.bolt.IdBolt;
import org.apache.storm.perf.spout.ConstSpout;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * ConstSpout -> IdBolt -> DevNullBolt
 * This topology measures speed of messaging between spouts->bolt  and  bolt->bolt
 *   ConstSpout : Continuously emits a constant string
 *   IdBolt : clones and emits input tuples
 *   DevNullBolt : discards incoming tuples
 */
public class ConstSpoutIdBoltNullBoltTopo {

    public static final String TOPOLOGY_NAME = "ConstSpoutIdBoltNullBoltTopo";
    public static final String SPOUT_ID = "constSpout";
    public static final String BOLT1_ID = "idBolt";
    public static final String BOLT2_ID = "nullBolt";

    // Configs
    public static final String BOLT1_COUNT = "bolt1.count";
    public static final String BOLT2_COUNT = "bolt2.count";
    public static final String SPOUT_COUNT = "spout.count";

    public static StormTopology getTopology(Map conf) {

        // 1 -  Setup Spout   --------
        ConstSpout spout = new ConstSpout("some data").withOutputFields("str");

        // 2 -  Setup IdBolt & DevNullBolt   --------
        IdBolt bolt1 = new IdBolt();
        DevNullBolt bolt2 = new DevNullBolt();


        // 3 - Setup Topology  --------
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT_ID, spout,  Helper.getInt(conf, SPOUT_COUNT, 1) );

        builder.setBolt(BOLT1_ID, bolt1, Helper.getInt(conf, BOLT1_COUNT, 1))
                .localOrShuffleGrouping(SPOUT_ID);

        builder.setBolt(BOLT2_ID, bolt2, Helper.getInt(conf, BOLT2_COUNT, 1))
                .localOrShuffleGrouping(BOLT1_ID);

        return builder.createTopology();
    }


    public static void main(String[] args) throws Exception {

        if (args.length <= 0) {
            // submit to local cluster
            Config conf = new Config();
            LocalCluster cluster = Helper.runOnLocalCluster(TOPOLOGY_NAME, getTopology(conf));

            Helper.setupShutdownHook(cluster, TOPOLOGY_NAME);
            while (true) {//  run indefinitely till Ctrl-C
                Thread.sleep(20_000_000);
            }
        } else {
            // submit to real cluster
            if (args.length >2) {
                System.err.println("args: runDurationSec  [optionalConfFile]");
                return;
            }
            Integer durationSec = Integer.parseInt(args[0]);
            Map topoConf =  (args.length==2) ? Utils.findAndReadConfigFile(args[1])  : new Config();

            //  Submit topology to storm cluster
            Helper.runOnClusterAndPrintMetrics(durationSec, TOPOLOGY_NAME, topoConf, getTopology(topoConf));
        }
    }
}
