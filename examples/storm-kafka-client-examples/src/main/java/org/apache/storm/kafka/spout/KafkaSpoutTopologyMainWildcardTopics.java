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

package org.apache.storm.kafka.spout;

import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;

import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * This example is similar to {@link KafkaSpoutTopologyMainNamedTopics}, but demonstrates subscribing to Kafka topics with a regex.
 */
public class KafkaSpoutTopologyMainWildcardTopics extends KafkaSpoutTopologyMainNamedTopics {

    private static final String STREAM = "test_wildcard_stream";
    private static final Pattern TOPIC_WILDCARD_PATTERN = Pattern.compile("kafka-spout-test-[1|2]");

    public static void main(String[] args) throws Exception {
        new KafkaSpoutTopologyMainWildcardTopics().runMain(args);
    }

    @Override
    protected StormTopology getTopologyKafkaSpout(KafkaSpoutConfig<String, String> spoutConfig) {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(spoutConfig), 1);
        tp.setBolt("kafka_bolt", new KafkaSpoutTestBolt()).shuffleGrouping("kafka_spout", STREAM);
        return tp.createTopology();
    }

    @Override
    protected KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers) {
        return KafkaSpoutConfig.builder(bootstrapServers, TOPIC_WILDCARD_PATTERN)
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup")
            .setRetry(getRetryService())
            .setRecordTranslator((r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
                new Fields("topic", "partition", "offset", "key", "value"), STREAM)
            .setOffsetCommitPeriodMs(10_000)
            .setFirstPollOffsetStrategy(EARLIEST)
            .setMaxUncommittedOffsets(250)
            .build();
    }
}
