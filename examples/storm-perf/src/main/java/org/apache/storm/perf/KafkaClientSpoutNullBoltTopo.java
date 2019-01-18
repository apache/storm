/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.perf;

import java.util.Map;
import java.util.Optional;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.ProcessingGuarantee;
import org.apache.storm.perf.bolt.DevNullBolt;
import org.apache.storm.perf.utils.Helper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Benchmark topology for measuring spout read/emit/ack performance. The spout reads and emits tuples. The bolt acks and discards received
 * tuples.
 */
public class KafkaClientSpoutNullBoltTopo {

    // configs - topo parallelism
    public static final String SPOUT_NUM = "spout.count";
    public static final String BOLT_NUM = "bolt.count";

    // configs - kafka spout
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String KAFKA_TOPIC = "kafka.topic";
    public static final String PROCESSING_GUARANTEE = "processing.guarantee";
    public static final String OFFSET_COMMIT_PERIOD_MS = "offset.commit.period.ms";

    public static final int DEFAULT_SPOUT_NUM = 1;
    public static final int DEFAULT_BOLT_NUM = 1;

    // names
    public static final String TOPOLOGY_NAME = KafkaClientSpoutNullBoltTopo.class.getSimpleName();
    public static final String SPOUT_ID = "kafkaSpout";
    public static final String BOLT_ID = "devNullBolt";

    /**
     * Create and configure the topology.
     */
    public static StormTopology getTopology(Map<String, Object> config) {

        final int spoutNum = Helper.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int boltNum = Helper.getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);
        // 1 -  Setup Kafka Spout   --------

        String bootstrapServers = Optional.ofNullable(Helper.getStr(config, BOOTSTRAP_SERVERS)).orElse("127.0.0.1:9092");
        String kafkaTopic = Optional.ofNullable(Helper.getStr(config, KAFKA_TOPIC)).orElse("storm-perf-null-bolt-topic");
        ProcessingGuarantee processingGuarantee = ProcessingGuarantee.valueOf(
            Optional.ofNullable(Helper.getStr(config, PROCESSING_GUARANTEE))
                    .orElse(ProcessingGuarantee.AT_LEAST_ONCE.name()));
        int offsetCommitPeriodMs = Helper.getInt(config, OFFSET_COMMIT_PERIOD_MS, 30_000);

        KafkaSpoutConfig<String, String> kafkaSpoutConfig = KafkaSpoutConfig.builder(bootstrapServers, kafkaTopic)
                                                                            .setProcessingGuarantee(processingGuarantee)
                                                                            .setOffsetCommitPeriodMs(offsetCommitPeriodMs)
                                                                            .setFirstPollOffsetStrategy(
                                                                                FirstPollOffsetStrategy.EARLIEST)
                                                                            .setTupleTrackingEnforced(true)
                                                                            .build();

        KafkaSpout<String, String> spout = new KafkaSpout<>(kafkaSpoutConfig);

        // 2 -   DevNull Bolt   --------
        DevNullBolt bolt = new DevNullBolt();

        // 3 - Setup Topology  --------
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout, spoutNum);
        builder.setBolt(BOLT_ID, bolt, boltNum)
               .localOrShuffleGrouping(SPOUT_ID);

        return builder.createTopology();
    }

    /**
     * Start the topology.
     */
    public static void main(String[] args) throws Exception {
        int durationSec = -1;
        Config topoConf = new Config();
        if (args.length > 0) {
            durationSec = Integer.parseInt(args[0]);
        }
        if (args.length > 1) {
            topoConf.putAll(Utils.findAndReadConfigFile(args[1]));
        }
        if (args.length > 2) {
            System.err.println("args: [runDurationSec]  [optionalConfFile]");
            return;
        }

        //  Submit to Storm cluster
        Helper.runOnClusterAndPrintMetrics(durationSec, TOPOLOGY_NAME, topoConf, getTopology(topoConf));
    }

}
