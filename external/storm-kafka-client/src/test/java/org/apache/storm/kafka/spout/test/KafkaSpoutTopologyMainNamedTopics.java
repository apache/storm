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

package org.apache.storm.kafka.spout.test;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.kafka.spout.KafkaSpoutStreams;
import org.apache.storm.kafka.spout.KafkaSpoutStreamsNamedTopics;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilder;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilderNamedTopics;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

public class KafkaSpoutTopologyMainNamedTopics {
    private static final String[] STREAMS = new String[]{"test_stream","test1_stream","test2_stream"};
    private static final String[] TOPICS = new String[]{"test","test1","test2"};


    public static void main(String[] args) throws Exception {
        new KafkaSpoutTopologyMainNamedTopics().runMain(args);
    }

    protected void runMain(String[] args) throws Exception {
        if (args.length == 0) {
            submitTopologyLocalCluster(getTopolgyKafkaSpout(), getConfig());
        } else {
            submitTopologyRemoteCluster(args[0], getTopolgyKafkaSpout(), getConfig());
        }

    }

    protected void submitTopologyLocalCluster(StormTopology topology, Config config) throws InterruptedException {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, topology);
        stopWaitingForInput();
    }

    protected void submitTopologyRemoteCluster(String arg, StormTopology topology, Config config) throws Exception {
        StormSubmitter.submitTopology(arg, config, topology);
    }

    protected void stopWaitingForInput() {
        try {
            System.out.println("PRESS ENTER TO STOP");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
            System.exit(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    protected StormTopology getTopolgyKafkaSpout() {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig(getKafkaSpoutStreams())), 1);
        tp.setBolt("kafka_bolt", new KafkaSpoutTestBolt()).shuffleGrouping("kafka_spout", STREAMS[0]);
        tp.setBolt("kafka_bolt_1", new KafkaSpoutTestBolt()).shuffleGrouping("kafka_spout", STREAMS[2]);
        return tp.createTopology();
    }

    protected KafkaSpoutConfig<String,String> getKafkaSpoutConfig(KafkaSpoutStreams kafkaSpoutStreams) {
        return new KafkaSpoutConfig.Builder<String, String>(getKafkaConsumerProps(), kafkaSpoutStreams, getTuplesBuilder(), getRetryService())
                .setOffsetCommitPeriodMs(10_000)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }

    protected KafkaSpoutRetryService getRetryService() {
            return new KafkaSpoutRetryExponentialBackoff(TimeInterval.microSeconds(500),
                    TimeInterval.milliSeconds(2), Integer.MAX_VALUE, TimeInterval.seconds(10));
    }

    protected Map<String,Object> getKafkaConsumerProps() {
        Map<String, Object> props = new HashMap<>();
//        props.put(KafkaSpoutConfig.Consumer.ENABLE_AUTO_COMMIT, "true");
        props.put(KafkaSpoutConfig.Consumer.BOOTSTRAP_SERVERS, "127.0.0.1:9092");
        props.put(KafkaSpoutConfig.Consumer.GROUP_ID, "kafkaSpoutTestGroup");
        props.put(KafkaSpoutConfig.Consumer.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(KafkaSpoutConfig.Consumer.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    protected KafkaSpoutTuplesBuilder<String, String> getTuplesBuilder() {
        return new KafkaSpoutTuplesBuilderNamedTopics.Builder<>(
                new TopicsTest0Test1TupleBuilder<String, String>(TOPICS[0], TOPICS[1]),
                new TopicTest2TupleBuilder<String, String>(TOPICS[2]))
                .build();
    }

    protected KafkaSpoutStreams getKafkaSpoutStreams() {
        final Fields outputFields = new Fields("topic", "partition", "offset", "key", "value");
        final Fields outputFields1 = new Fields("topic", "partition", "offset");
        return new KafkaSpoutStreamsNamedTopics.Builder(outputFields, STREAMS[0], new String[]{TOPICS[0], TOPICS[1]})  // contents of topics test, test1, sent to test_stream
                .addStream(outputFields, STREAMS[0], new String[]{TOPICS[2]})  // contents of topic test2 sent to test_stream
                .addStream(outputFields1, STREAMS[2], new String[]{TOPICS[2]})  // contents of topic test2 sent to test2_stream
                .build();
    }
}
