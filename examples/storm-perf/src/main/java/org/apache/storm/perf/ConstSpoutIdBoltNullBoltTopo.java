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
import org.apache.storm.perf.bolt.DevNullBolt;
import org.apache.storm.perf.bolt.IdBolt;
import org.apache.storm.perf.spout.ConstSpout;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * ConstSpout -> IdBolt -> DevNullBolt This topology measures speed of messaging between spouts->bolt  and  bolt->bolt ConstSpout :
 * Continuously emits a constant string IdBolt : clones and emits input tuples DevNullBolt : discards incoming tuples.
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

    static StormTopology getTopology(Map<String, Object> conf) {

        // 1 -  Setup Spout   --------
        ConstSpout spout = new ConstSpout("some data").withOutputFields("str");

        // 2 -  Setup IdBolt & DevNullBolt   --------
        IdBolt bolt1 = new IdBolt();
        DevNullBolt bolt2 = new DevNullBolt();


        // 3 - Setup Topology  --------
        TopologyBuilder builder = new TopologyBuilder();

        int numSpouts = Helper.getInt(conf, SPOUT_COUNT, 1);
        builder.setSpout(SPOUT_ID, spout, numSpouts);

        int numBolt1 = Helper.getInt(conf, BOLT1_COUNT, 1);
        builder.setBolt(BOLT1_ID, bolt1, numBolt1)
               .localOrShuffleGrouping(SPOUT_ID);

        int numBolt2 = Helper.getInt(conf, BOLT2_COUNT, 1);
        builder.setBolt(BOLT2_ID, bolt2, numBolt2)
               .localOrShuffleGrouping(BOLT1_ID);
        System.err.printf("====> Using : numSpouts = %d , numBolt1 = %d, numBolt2=%d\n", numSpouts, numBolt1, numBolt2);
        return builder.createTopology();
    }


    public static void main(String[] args) throws Exception {
        int runTime = -1;
        Config topoConf = new Config();
        // Configure for achieving max throughput in single worker mode (empirically found).
        //     -- Expect ~5.3 mill/sec (3.2 mill/sec with batchSz=1)
        //     -- ~1 mill/sec, lat= ~20 microsec  with acker=1 & batchSz=1
        topoConf.put(Config.TOPOLOGY_SPOUT_RECVQ_SKIPS, 8);
        topoConf.put(Config.TOPOLOGY_PRODUCER_BATCH_SIZE, 500);
        topoConf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 50_000);
        topoConf.put(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING, true);
        topoConf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 0.0005);

        if (args.length > 0) {
            runTime = Integer.parseInt(args[0]);
        }
        if (args.length > 1) {
            topoConf.putAll(Utils.findAndReadConfigFile(args[1]));
        }
        topoConf.putAll(Utils.readCommandLineOpts());

        if (args.length > 2) {
            System.err.println("args: [runDurationSec]  [optionalConfFile]");
            return;
        }
        //  Submit topology to storm cluster
        Helper.runOnClusterAndPrintMetrics(runTime, TOPOLOGY_NAME, topoConf, getTopology(topoConf));
    }
}
