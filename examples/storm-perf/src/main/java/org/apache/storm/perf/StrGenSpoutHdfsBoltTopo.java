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
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.perf.spout.StringGenSpout;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

/**
 * This topo helps measure speed of writing to Hdfs.
 *
 * <p>Spout generates fixed length random strings.
 *
 * <p>Bolt writes to Hdfs.
 */
public class StrGenSpoutHdfsBoltTopo {

    // configs - topo parallelism
    public static final String SPOUT_NUM = "spout.count";
    public static final String BOLT_NUM = "bolt.count";

    // configs - hdfs bolt
    public static final String HDFS_URI = "hdfs.uri";
    public static final String HDFS_PATH = "hdfs.dir";
    public static final String HDFS_BATCH = "hdfs.batch";

    public static final int DEFAULT_SPOUT_NUM = 1;
    public static final int DEFAULT_BOLT_NUM = 1;
    public static final int DEFAULT_HDFS_BATCH = 1000;

    // names
    public static final String TOPOLOGY_NAME = "StrGenSpoutHdfsBoltTopo";
    public static final String SPOUT_ID = "GenSpout";
    public static final String BOLT_ID = "hdfsBolt";


    static StormTopology getTopology(Map<String, Object> topoConf) {
        final int hdfsBatch = Helper.getInt(topoConf, HDFS_BATCH, DEFAULT_HDFS_BATCH);

        // 1 -  Setup StringGen Spout   --------
        StringGenSpout spout = new StringGenSpout(100).withFieldName("str");


        // 2 -  Setup HFS Bolt   --------
        String hdfsUrl = Helper.getStr(topoConf, HDFS_URI);
        RecordFormat format = new LineWriter("str");
        SyncPolicy syncPolicy = new CountSyncPolicy(hdfsBatch);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0f, FileSizeRotationPolicy.Units.GB);
        final int spoutNum = Helper.getInt(topoConf, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int boltNum = Helper.getInt(topoConf, BOLT_NUM, DEFAULT_BOLT_NUM);

        // Use default, Storm-generated file names
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(Helper.getStr(topoConf, HDFS_PATH));

        // Instantiate the HdfsBolt
        HdfsBolt bolt = new HdfsBolt()
            .withFsUrl(hdfsUrl)
            .withFileNameFormat(fileNameFormat)
            .withRecordFormat(format)
            .withRotationPolicy(rotationPolicy)
            .withSyncPolicy(syncPolicy);


        // 3 - Setup Topology  --------

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout, spoutNum);
        builder.setBolt(BOLT_ID, bolt, boltNum)
               .localOrShuffleGrouping(SPOUT_ID);

        return builder.createTopology();
    }


    /**
     * Spout generates random strings and HDFS bolt writes them to a text file.
     */
    public static void main(String[] args) throws Exception {
        String confFile = "conf/HdfsSpoutTopo.yaml";
        int runTime = -1; //Run until Ctrl-C
        if (args.length > 0) {
            runTime = Integer.parseInt(args[0]);
        }

        if (args.length > 1) {
            confFile = args[1];
        }

        //  Submit to Storm cluster
        if (args.length > 2) {
            System.err.println("args: [runDurationSec] [confFile]");
            return;
        }

        Map<String, Object> topoConf = Utils.findAndReadConfigFile(confFile);
        topoConf.put(Config.TOPOLOGY_PRODUCER_BATCH_SIZE, 1000);
        topoConf.put(Config.TOPOLOGY_BOLT_WAIT_STRATEGY, "org.apache.storm.policy.WaitStrategyPark");
        topoConf.put(Config.TOPOLOGY_BOLT_WAIT_PARK_MICROSEC, 0);
        topoConf.put(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING, true);
        topoConf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 0.0005);

        topoConf.putAll(Utils.readCommandLineOpts());
        Helper.runOnClusterAndPrintMetrics(runTime, TOPOLOGY_NAME, topoConf, getTopology(topoConf));
    }


    public static class LineWriter implements RecordFormat {
        private static final long serialVersionUID = 7524288317405514146L;
        private String lineDelimiter = System.lineSeparator();
        private String fieldName;

        public LineWriter(String fieldName) {
            this.fieldName = fieldName;
        }

        /**
         * Overrides the default record delimiter.
         */
        public LineWriter withLineDelimiter(String delimiter) {
            this.lineDelimiter = delimiter;
            return this;
        }

        @Override
        public byte[] format(Tuple tuple) {
            return (tuple.getValueByField(fieldName).toString() + this.lineDelimiter).getBytes();
        }
    }

}
