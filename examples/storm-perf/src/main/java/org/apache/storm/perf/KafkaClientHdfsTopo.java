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
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;

/**
 * This topo helps measure speed of reading from Kafka and writing to Hdfs.
 *
 * <p>Spout Reads from Kafka.
 *
 * <p>Bolt writes to Hdfs.
 */
public class KafkaClientHdfsTopo {

    // configs - topo parallelism
    public static final String SPOUT_NUM = "spout.count";
    public static final String BOLT_NUM = "bolt.count";
    // configs - kafka spout
    public static final String KAFKA_TOPIC = "kafka.topic";
    public static final String KAFKA_BOOTSTRAP_HOSTS = "kafka.bootstrap.hosts";
    // configs - hdfs bolt
    public static final String HDFS_URI = "hdfs.uri";
    public static final String HDFS_PATH = "hdfs.dir";
    public static final String HDFS_BATCH = "hdfs.batch";


    public static final int DEFAULT_SPOUT_NUM = 1;
    public static final int DEFAULT_BOLT_NUM = 1;
    public static final int DEFAULT_HDFS_BATCH = 1000;

    // names
    public static final String TOPOLOGY_NAME = "KafkaHdfsTopo";
    public static final String SPOUT_ID = "kafkaSpout";
    public static final String BOLT_ID = "hdfsBolt";


    static StormTopology getTopology(Map<String, Object> config) {

        final int spoutNum = getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int boltNum = getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);

        final int hdfsBatch = getInt(config, HDFS_BATCH, DEFAULT_HDFS_BATCH);

        // 1 -  Setup Kafka Spout   --------
        String bootstrapHosts = getStr(config, KAFKA_BOOTSTRAP_HOSTS);
        String topicName = getStr(config, KAFKA_TOPIC);

        KafkaSpoutConfig<String, String> spoutConfig = KafkaSpoutConfig.builder(bootstrapHosts, topicName)
                                                                       .setFirstPollOffsetStrategy(
                                                                           FirstPollOffsetStrategy.EARLIEST)
                                                                       .build();

        KafkaSpout<String, String> spout = new KafkaSpout<>(spoutConfig);

        // 2 -  Setup HFS Bolt   --------
        String hdfsUrls = getStr(config, HDFS_URI);
        RecordFormat format = new LineWriter("value");
        SyncPolicy syncPolicy = new CountSyncPolicy(hdfsBatch);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0f, FileSizeRotationPolicy.Units.GB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(getStr(config, HDFS_PATH));

        // Instantiate the HdfsBolt
        HdfsBolt bolt = new HdfsBolt()
            .withFsUrl(hdfsUrls)
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


    public static int getInt(Map map, Object key, int def) {
        return ObjectReader.getInt(Utils.get(map, key, def));
    }

    public static String getStr(Map map, Object key) {
        return (String) map.get(key);
    }


    /**
     * Copies text file content from sourceDir to destinationDir. Moves source files into sourceDir after its done consuming.
     */
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("args: runDurationSec topConfFile");
            return;
        }

        String confFile = args[1];
        Map<String, Object> topoConf = Utils.findAndReadConfigFile(confFile);
        topoConf.put(Config.TOPOLOGY_PRODUCER_BATCH_SIZE, 1000);
        topoConf.put(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING, true);
        topoConf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 0.0005);
        topoConf.put(Config.TOPOLOGY_BOLT_WAIT_STRATEGY, "org.apache.storm.policy.WaitStrategyPark");
        topoConf.put(Config.TOPOLOGY_BOLT_WAIT_PARK_MICROSEC, 0);

        topoConf.putAll(Utils.readCommandLineOpts());
        //  Submit topology to Storm cluster
        Integer durationSec = Integer.parseInt(args[0]);
        Helper.runOnClusterAndPrintMetrics(durationSec, TOPOLOGY_NAME, topoConf, getTopology(topoConf));
    }

    public static class LineWriter implements RecordFormat {
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
