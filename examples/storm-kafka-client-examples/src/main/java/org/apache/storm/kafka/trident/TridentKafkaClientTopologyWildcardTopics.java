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

package org.apache.storm.kafka.trident;

import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;

import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutConfig;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * This example is similar to {@link TridentKafkaClientTopologyWildcardTopics}, but demonstrates subscribing to Kafka topics with a regex.
 */
public class TridentKafkaClientTopologyWildcardTopics extends TridentKafkaClientTopologyNamedTopics {
    private static final Pattern TOPIC_WILDCARD_PATTERN = Pattern.compile("test-trident(-1)?");

    @Override
    protected KafkaTridentSpoutConfig<String, String> newKafkaSpoutConfig(String bootstrapServers) {
        return KafkaTridentSpoutConfig.builder(bootstrapServers, TOPIC_WILDCARD_PATTERN)
                .setProp(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 200)
                .setRecordTranslator((r) -> new Values(r.value()), new Fields("str"))
                .setFirstPollOffsetStrategy(EARLIEST)
                .build();
    }

    public static void main(String[] args) throws Exception {
        new TridentKafkaClientTopologyWildcardTopics().run(args);
    }
}
