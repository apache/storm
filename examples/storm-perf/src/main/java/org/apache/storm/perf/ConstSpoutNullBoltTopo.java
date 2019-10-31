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
import org.apache.storm.perf.spout.ConstSpout;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * This topo helps measure the messaging peak throughput between a spout and a bolt.
 *
 * <p>Spout generates a stream of a fixed string.
 *
 * <p>Bolt will simply ack and discard the tuple received.
 */
public class ConstSpoutNullBoltTopo {

    public static final String TOPOLOGY_NAME = "ConstSpoutNullBoltTopo";
    public static final String SPOUT_ID = "constSpout";
    public static final String BOLT_ID = "nullBolt";

    // Configs
    public static final String BOLT_COUNT = "bolt.count";
    public static final String SPOUT_COUNT = "spout.count";
    public static final String GROUPING = "grouping"; // can be 'local' or 'shuffle'

    public static final String LOCAL_GROPING = "local";
    public static final String SHUFFLE_GROUPING = "shuffle";
    public static final String DEFAULT_GROUPING = LOCAL_GROPING;

    static StormTopology getTopology(Map<String, Object> conf) {

        // 1 -  Setup Spout   --------
        ConstSpout spout = new ConstSpout("some data").withOutputFields("str");

        // 2 -  Setup DevNull Bolt   --------
        DevNullBolt bolt = new DevNullBolt();


        // 3 - Setup Topology  --------
        TopologyBuilder builder = new TopologyBuilder();

        int numSpouts = Helper.getInt(conf, SPOUT_COUNT, 1);
        builder.setSpout(SPOUT_ID, spout, numSpouts);

        int numBolts = Helper.getInt(conf, BOLT_COUNT, 1);
        BoltDeclarer bd = builder.setBolt(BOLT_ID, bolt, numBolts);

        System.err.printf("====> Using : numSpouts = %d , numBolts = %d\n", numSpouts, numBolts);

        String groupingType = Helper.getStr(conf, GROUPING);
        if (groupingType == null || groupingType.equalsIgnoreCase(DEFAULT_GROUPING)) {
            bd.localOrShuffleGrouping(SPOUT_ID);
        } else if (groupingType.equalsIgnoreCase(SHUFFLE_GROUPING)) {
            bd.shuffleGrouping(SPOUT_ID);
        }
        return builder.createTopology();
    }

    /**
     * ConstSpout -> DevNullBolt with configurable grouping (default localOrShuffle).
     */
    public static void main(String[] args) throws Exception {
        int runTime = -1;
        Config topoConf = new Config();
        // Configured for achieving max throughput in single worker mode (empirically found).
        //  For reference : numbers taken on MacBook Pro mid 2015
        //    -- ACKer=0:  ~8 mill/sec (batchSz=2k & recvQsize=50k).  6.7 mill/sec (batchSz=1 & recvQsize=1k)
        //    -- ACKer=1:  ~1 mill/sec,   lat= ~1 microsec  (batchSz=1 & bolt.wait.strategy=Park bolt.wait.park.micros=0)
        //    -- ACKer=1:  ~1.3 mill/sec, lat= ~11 micros   (batchSz=1 & receive.buffer.size=1k, bolt.wait & bp.wait =
        // Progressive[defaults])
        //    -- ACKer=1:  ~1.6 mill/sec, lat= ~300 micros  (batchSz=500 & bolt.wait.strategy=Park bolt.wait.park.micros=0)
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

