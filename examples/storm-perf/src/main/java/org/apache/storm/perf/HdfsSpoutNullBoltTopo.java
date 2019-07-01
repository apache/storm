/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.perf;

import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hdfs.spout.HdfsSpout;
import org.apache.storm.hdfs.spout.TextFileReader;
import org.apache.storm.perf.bolt.DevNullBolt;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * This topo helps measure speed of reading from Hdfs.
 *
 * <p>Spout Reads from Hdfs.
 *
 * <p>Bolt acks and discards tuples.
 */
public class HdfsSpoutNullBoltTopo {
    public static final int DEFAULT_SPOUT_NUM = 1;
    public static final int DEFAULT_BOLT_NUM = 1;
    // names
    static final String TOPOLOGY_NAME = "HdfsSpoutNullBoltTopo";
    static final String SPOUT_ID = "hdfsSpout";
    static final String BOLT_ID = "devNullBolt";
    // configs
    static final String SPOUT_NUM = "spout.count";
    static final String BOLT_NUM = "bolt.count";
    static final String HDFS_URI = "hdfs.uri";
    static final String SOURCE_DIR = "hdfs.source.dir";
    static final String ARCHIVE_DIR = "hdfs.archive.dir";
    static final String BAD_DIR = "hdfs.bad.dir";

    static StormTopology getTopology(Map<String, Object> config) {

        final int spoutNum = Helper.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int boltNum = Helper.getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);
        final String fileFormat = Helper.getStr(config, "text");
        final String hdfsUri = Helper.getStr(config, HDFS_URI);
        final String sourceDir = Helper.getStr(config, SOURCE_DIR);
        final String archiveDir = Helper.getStr(config, ARCHIVE_DIR);
        final String badDir = Helper.getStr(config, BAD_DIR);


        // 1 -  Setup Hdfs Spout   --------
        HdfsSpout spout = new HdfsSpout()
            .setReaderType(fileFormat)
            .setHdfsUri(hdfsUri)
            .setSourceDir(sourceDir)
            .setArchiveDir(archiveDir)
            .setBadFilesDir(badDir)
            .withOutputFields(TextFileReader.defaultFields);

        // 2 -   DevNull Bolt   --------
        DevNullBolt bolt = new DevNullBolt();

        // 3 - Setup Topology  --------
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout, spoutNum);
        builder.setBolt(BOLT_ID, bolt, boltNum)
               .localOrShuffleGrouping(SPOUT_ID);

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("args: runDurationSec topConfFile");
            return;
        }

        final Integer durationSec = Integer.parseInt(args[0]);
        Config topoConf = new Config();
        topoConf.putAll(Utils.findAndReadConfigFile(args[1]));
        topoConf.put(Config.TOPOLOGY_PRODUCER_BATCH_SIZE, 1000);
        topoConf.put(Config.TOPOLOGY_BOLT_WAIT_STRATEGY, "org.apache.storm.policy.WaitStrategyPark");
        topoConf.put(Config.TOPOLOGY_BOLT_WAIT_PARK_MICROSEC, 0);
        topoConf.put(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING, true);
        topoConf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 0.0005);

        topoConf.putAll(Utils.readCommandLineOpts());
        // Submit to Storm cluster
        Helper.runOnClusterAndPrintMetrics(durationSec, TOPOLOGY_NAME, topoConf, getTopology(topoConf));
    }
}
