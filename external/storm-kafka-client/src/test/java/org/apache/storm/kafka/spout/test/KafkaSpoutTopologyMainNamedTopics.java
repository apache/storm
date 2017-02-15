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

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.Func;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class KafkaSpoutTopologyMainNamedTopics {
    private static final String TOPIC_2_STREAM = "test_2_stream";
    private static final String TOPIC_0_1_STREAM = "test_0_1_stream";
    private static final String[] TOPICS = new String[]{"test","test1","test2"};


    public static void main(String[] args) throws Exception {
        new KafkaSpoutTopologyMainNamedTopics().runMain(args);
    }

    protected void runMain(String[] args) throws Exception {
        if (args.length == 0) {
            submitTopologyLocalCluster(getTopologyKafkaSpout(), getConfig());
        } else {
            submitTopologyRemoteCluster(args[0], getTopologyKafkaSpout(), getConfig());
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

    protected StormTopology getTopologyKafkaSpout() {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig()), 1);
        tp.setBolt("kafka_bolt", new KafkaSpoutTestBolt())
          .shuffleGrouping("kafka_spout", TOPIC_0_1_STREAM)
          .shuffleGrouping("kafka_spout", TOPIC_2_STREAM);
        tp.setBolt("kafka_bolt_1", new KafkaSpoutTestBolt()).shuffleGrouping("kafka_spout", TOPIC_2_STREAM);
        return tp.createTopology();
    }

    public static Func<ConsumerRecord<String, String>, List<Object>> TOPIC_PART_OFF_KEY_VALUE_FUNC = new Func<ConsumerRecord<String, String>, List<Object>>() {
        @Override
        public List<Object> apply(ConsumerRecord<String, String> r) {
            return new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value());
        }
    };
    
    protected KafkaSpoutConfig<String,String> getKafkaSpoutConfig() {
        ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
                TOPIC_PART_OFF_KEY_VALUE_FUNC,
                new Fields("topic", "partition", "offset", "key", "value"), TOPIC_0_1_STREAM);
        trans.forTopic(TOPICS[2], 
                TOPIC_PART_OFF_KEY_VALUE_FUNC,
                new Fields("topic", "partition", "offset", "key", "value"), TOPIC_2_STREAM);
        return KafkaSpoutConfig.builder("127.0.0.1:9092", TOPICS)
                .setGroupId("kafkaSpoutTestGroup")
                .setRetry(getRetryService())
                .setRecordTranslator(trans)
                .setOffsetCommitPeriodMs(10_000)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }

    protected KafkaSpoutRetryService getRetryService() {
            return new KafkaSpoutRetryExponentialBackoff(TimeInterval.microSeconds(500),
                    TimeInterval.milliSeconds(2), Integer.MAX_VALUE, TimeInterval.seconds(10));
    }
}
