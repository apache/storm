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

/***
 * This topo helps measure the messaging speed between a spout and a bolt.
 *  Spout generates a stream of a fixed string.
 *  Bolt will simply ack and discard the tuple received
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

    public static StormTopology getTopology(Map<String, Object> conf) {

        // 1 -  Setup Spout   --------
        ConstSpout spout = new ConstSpout("some data").withOutputFields("str");

        // 2 -  Setup DevNull Bolt   --------
        DevNullBolt bolt = new DevNullBolt();


        // 3 - Setup Topology  --------
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT_ID, spout,  Helper.getInt(conf, SPOUT_COUNT, 1) );
        BoltDeclarer bd = builder.setBolt(BOLT_ID, bolt, Helper.getInt(conf, BOLT_COUNT, 1));

        String groupingType = Helper.getStr(conf, GROUPING);
        if(groupingType==null || groupingType.equalsIgnoreCase(DEFAULT_GROUPING) )
            bd.localOrShuffleGrouping(SPOUT_ID);
        else if(groupingType.equalsIgnoreCase(SHUFFLE_GROUPING) )
            bd.shuffleGrouping(SPOUT_ID);
        return builder.createTopology();
    }

    /**
     * ConstSpout -> DevNullBolt with configurable grouping (default localOrShuffle)
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
        if (args.length > 2) {
            System.err.println("args: [runDurationSec]  [optionalConfFile]");
            return;
        }
        //  Submit topology to storm cluster
        Helper.runOnClusterAndPrintMetrics(runTime, TOPOLOGY_NAME, topoConf, getTopology(topoConf));
    }

}

