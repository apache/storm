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

package org.apache.storm.kafka.bolt;

import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.lambda.LambdaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class KafkaProducerTopology {

    /**
     * Create a new topology that writes random UUIDs to Kafka.
     *
     * @param brokerUrl Kafka broker URL
     * @param topicName Topic to which publish sentences
     * @return A Storm topology that produces random UUIDs using a {@link LambdaSpout} and uses a {@link KafkaBolt} to publish the UUIDs to
     *     the kafka topic specified
     */
    public static StormTopology newTopology(String brokerUrl, String topicName) {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", () -> {
            Utils.sleep(1000); //Throttle this spout a bit to avoid maxing out CPU
            return UUID.randomUUID().toString();
        });

        /* The output field of the spout ("lambda") is provided as the boltMessageField
          so that this gets written out as the message in the kafka topic.
          The tuples have no key field, so the messages are written to Kafka without a key.*/
        final KafkaBolt<String, String> bolt = new KafkaBolt<String, String>()
            .withProducerProperties(newProps(brokerUrl, topicName))
            .withTopicSelector(new DefaultTopicSelector(topicName))
            .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("key", "lambda"));

        builder.setBolt("forwardToKafka", bolt, 1).shuffleGrouping("spout");

        return builder.createTopology();
    }

    /**
     * Create the Storm config.
     * @return the Storm config for the topology that publishes random UUIDs to Kafka using a Kafka bolt.
     */
    private static Properties newProps(final String brokerUrl, final String topicName) {
        return new Properties() {
            {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                put(ProducerConfig.CLIENT_ID_CONFIG, topicName);
            }
        };
    }
}
