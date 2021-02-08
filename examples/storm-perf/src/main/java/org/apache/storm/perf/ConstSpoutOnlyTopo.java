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
import org.apache.storm.generated.StormTopology;
import org.apache.storm.perf.spout.ConstSpout;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * This topo helps measure how fast a spout can produce data (so no bolts are attached).
 *
 * <p>Spout generates a stream of a fixed string.
 */
public class ConstSpoutOnlyTopo {

    public static final String TOPOLOGY_NAME = "ConstSpoutOnlyTopo";
    public static final String SPOUT_ID = "constSpout";


    static StormTopology getTopology() {

        // 1 -  Setup Const Spout   --------
        ConstSpout spout = new ConstSpout("some data").withOutputFields("str");

        // 2 - Setup Topology  --------
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout, 1);
        return builder.createTopology();
    }

    /**
     * ConstSpout only topology  (No bolts).
     */
    public static void main(String[] args) throws Exception {
        int runTime = -1;
        Config topoConf = new Config();
        if (args.length > 0) {
            runTime = Integer.parseInt(args[0]);
        }
        if (args.length > 1) {
            topoConf.putAll(Utils.findAndReadConfigFile(args[1]));
        }
        topoConf.put(Config.TOPOLOGY_SPOUT_RECVQ_SKIPS, 8);
        topoConf.put(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING, true);
        topoConf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 0.0005);
        topoConf.putAll(Utils.readCommandLineOpts());
        if (args.length > 2) {
            System.err.println("args: [runDurationSec]  [optionalConfFile]");
            return;
        }
        //  Submit topology to storm cluster
        Helper.runOnClusterAndPrintMetrics(runTime, TOPOLOGY_NAME, topoConf, getTopology());
    }
}
