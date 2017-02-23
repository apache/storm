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
import org.apache.storm.perf.spout.ConstSpout;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;


/***
 * This topo helps measure how fast a spout can produce data (so no bolts are attached)
 *  Spout generates a stream of a fixed string.
 */

public class ConstSpoutOnlyTopo {

    public static final String TOPOLOGY_NAME = "ConstSpoutOnlyTopo";
    public static final String SPOUT_ID = "constSpout";


    public static StormTopology getTopology() {

        // 1 -  Setup Const Spout   --------
        ConstSpout spout = new ConstSpout("some data").withOutputFields("str");

        // 2 - Setup Topology  --------
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout, 1);
        return builder.createTopology();
    }

    /**
     * ConstSpout only topology  (No bolts)
     */
    public static void main(String[] args) throws Exception {
        if(args.length <= 0) {
            // For IDE based profiling ... submit topology to local cluster
            LocalCluster cluster = Helper.runOnLocalCluster(TOPOLOGY_NAME, getTopology());

            Helper.setupShutdownHook(cluster, TOPOLOGY_NAME);
            while (true) {//  run indefinitely till Ctrl-C
                Thread.sleep(20_000_000);
            }
        } else {
            //  Submit topology to storm cluster
            if (args.length != 1) {
                System.err.println("args: runDurationSec");
                return;
            }
            Integer durationSec = Integer.parseInt(args[0]);

            Helper.runOnClusterAndPrintMetrics(durationSec, TOPOLOGY_NAME, new Config(), getTopology());
        }
    }
}
