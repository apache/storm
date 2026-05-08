/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.kafka.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test — requires a Docker daemon. Skipped automatically when Docker is unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
class KafkaOffsetLagUtilTest {

    private static final String TOPIC = "lag-test-topic";
    private static final String GROUP_ID = "lag-test-group";
    private static final int PARTITIONS = 2;
    private static final long COMMITTED_OFFSET_PARTITION_0 = 7L;

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.0.0"));

    @BeforeAll
    static void seedKafka() throws Exception {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (Admin admin = Admin.create(adminProps)) {
            admin.createTopics(Collections.singletonList(new NewTopic(TOPIC, PARTITIONS, (short) 1))).all().get();
        }
        produceOneRecordToEachPartition();
        commitOffsetForPartitionZeroOnly();
    }

    @Test
    void getOffsetLagsReportsCommittedOffsetForCommittedPartitionsAndMinusOneForUncommittedPartitions() throws Exception {
        NewKafkaSpoutOffsetQuery query = new NewKafkaSpoutOffsetQuery(
                TOPIC, KAFKA.getBootstrapServers(), GROUP_ID, null, null, null);

        List<KafkaOffsetLagResult> results = KafkaOffsetLagUtil.getOffsetLags(query);

        assertEquals(PARTITIONS, results.size(), "Expected one result per partition");
        Map<Integer, Long> committedByPartition = new HashMap<>();
        for (KafkaOffsetLagResult r : results) {
            committedByPartition.put(r.getPartition(), r.getConsumerCommittedOffset());
        }
        assertNotNull(committedByPartition.get(0));
        assertEquals(COMMITTED_OFFSET_PARTITION_0, committedByPartition.get(0),
                "Partition with a committed offset should report it");
        // Regression assertion: pre-fix this NPE'd inside the monitor and surfaced as ClassCastException upstream.
        assertEquals(-1L, committedByPartition.get(1),
                "Partition with no committed offset should report -1, not throw");
    }

    private static void produceOneRecordToEachPartition() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (int p = 0; p < PARTITIONS; p++) {
                producer.send(new ProducerRecord<>(TOPIC, p, "k", "v"));
            }
            producer.flush();
        }
    }

    private static void commitOffsetForPartitionZeroOnly() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            TopicPartition p0 = new TopicPartition(TOPIC, 0);
            consumer.commitSync(Collections.singletonMap(p0, new OffsetAndMetadata(COMMITTED_OFFSET_PARTITION_0)));
        }
    }
}
