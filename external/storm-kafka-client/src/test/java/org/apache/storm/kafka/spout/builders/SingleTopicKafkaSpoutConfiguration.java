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

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.kafka.spout.test.KafkaSpoutTestBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

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
        tp.setSpout("kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig(getKafkaSpoutStreams(), port)), 1);
        tp.setBolt("kafka_bolt", new KafkaSpoutTestBolt()).shuffleGrouping("kafka_spout", STREAM);
        return tp.createTopology();
    }

    public static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(KafkaSpoutStreams kafkaSpoutStreams, int port) {
        return getKafkaSpoutConfig(kafkaSpoutStreams, port, 10_000);
    }

    public static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(KafkaSpoutStreams kafkaSpoutStreams, int port, long offsetCommitPeriodMs) {
        return getKafkaSpoutConfig(kafkaSpoutStreams, port, offsetCommitPeriodMs, getRetryService());
    }

    public static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(KafkaSpoutStreams kafkaSpoutStreams, int port, long offsetCommitPeriodMs, KafkaSpoutRetryService retryService) {
        return new KafkaSpoutConfig.Builder<>(getKafkaConsumerProps(port), kafkaSpoutStreams, getTuplesBuilder(), retryService)
            .setOffsetCommitPeriodMs(offsetCommitPeriodMs)
            .setFirstPollOffsetStrategy(EARLIEST)
            .setMaxUncommittedOffsets(250)
            .setPollTimeoutMs(1000)
            .build();
    }

    protected static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(0),
            KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(0), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(0));

    }

    protected static Map<String, Object> getKafkaConsumerProps(int port) {
        Map<String, Object> props = new HashMap<>();
        props.put(KafkaSpoutConfig.Consumer.BOOTSTRAP_SERVERS, "127.0.0.1:" + port);
        props.put(KafkaSpoutConfig.Consumer.GROUP_ID, "kafkaSpoutTestGroup");
        props.put(KafkaSpoutConfig.Consumer.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(KafkaSpoutConfig.Consumer.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("max.poll.records", "5");
        return props;
    }

    protected static KafkaSpoutTuplesBuilder<String, String> getTuplesBuilder() {
        return new KafkaSpoutTuplesBuilderNamedTopics.Builder<>(
            new TopicKeyValueTupleBuilder<String, String>(TOPIC))
            .build();
    }

    public static KafkaSpoutStreams getKafkaSpoutStreams() {
        final Fields outputFields = new Fields("topic", "key", "value");
        return new KafkaSpoutStreamsNamedTopics.Builder(outputFields, STREAM, new String[]{TOPIC}) // contents of topics test sent to test_stream
            .build();
    }
}
