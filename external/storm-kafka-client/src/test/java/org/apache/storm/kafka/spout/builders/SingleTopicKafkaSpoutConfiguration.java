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
package org.apache.storm.kafka.spout.builders;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.Func;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.kafka.spout.test.KafkaSpoutTestBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SingleTopicKafkaSpoutConfiguration {

    public static final String STREAM = "test_stream";
    public static final String TOPIC = "test";

    public static Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    public static StormTopology getTopologyKafkaSpout(int port) {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig(port)), 1);
        tp.setBolt("kafka_bolt", new KafkaSpoutTestBolt()).shuffleGrouping("kafka_spout", STREAM);
        return tp.createTopology();
    }

    static public KafkaSpoutConfig<String, String> getKafkaSpoutConfig(int port) {
        return getKafkaSpoutConfig(port, 10_000);
    }

    static public KafkaSpoutConfig<String, String> getKafkaSpoutConfig(int port, long offsetCommitPeriodMs) {
        return getKafkaSpoutConfig(port, offsetCommitPeriodMs, getRetryService());
    }

    private static Func<ConsumerRecord<String, String>, List<Object>> TOPIC_KEY_VALUE_FUNC = new Func<ConsumerRecord<String, String>, List<Object>>() {
        @Override
        public List<Object> apply(ConsumerRecord<String, String> r) {
            return new Values(r.topic(), r.key(), r.value());
        }
    };
    
    static public KafkaSpoutConfig<String,String> getKafkaSpoutConfig(int port, long offsetCommitPeriodMs, KafkaSpoutRetryService retryService) {
        return KafkaSpoutConfig.builder("127.0.0.1:" + port, TOPIC)
                .setRecordTranslator(TOPIC_KEY_VALUE_FUNC,
                        new Fields("topic", "key", "value"), STREAM)
                .setGroupId("kafkaSpoutTestGroup")
                .setMaxPollRecords(5)
                .setRetry(retryService)
                .setOffsetCommitPeriodMs(offsetCommitPeriodMs)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250)
                .setPollTimeoutMs(1000)
                .build();
    }
        
    protected static KafkaSpoutRetryService getRetryService() {
        return KafkaSpoutConfig.UNIT_TEST_RETRY_SERVICE;
    }
}
