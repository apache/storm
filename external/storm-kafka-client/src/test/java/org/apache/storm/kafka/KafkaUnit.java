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
package org.apache.storm.kafka;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.nio.file.Files;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.testing.TmpPath;

public class KafkaUnit {
    private TestingServer zookeeper;
    private KafkaServer kafkaServer;
    private KafkaProducer<String, String> producer;
    private AdminClient kafkaAdminClient;
    private TmpPath kafkaDir;
    private static final String KAFKA_HOST = "127.0.0.1";
    private static final int KAFKA_PORT = 9092;

    public KafkaUnit() {
    }

    public void setUp() throws Exception {
        // setup ZK
        zookeeper = new TestingServer(true);

        // setup Broker
        kafkaDir = new TmpPath(Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zookeeper.getConnectString());
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", kafkaDir.getPath());
        brokerProps.setProperty("listeners", String.format("PLAINTEXT://%s:%d", KAFKA_HOST, KAFKA_PORT));
        brokerProps.setProperty("offsets.topic.replication.factor", "1");
        KafkaConfig config = new KafkaConfig(brokerProps);
        MockTime mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);

        // setup default Producer
        createProducer();
        kafkaAdminClient = AdminClient.create(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST + ":" + KAFKA_PORT));
    }

    public void tearDown() throws Exception {
        kafkaAdminClient.close();
        closeProducer();
        kafkaServer.shutdown();
        kafkaDir.close();
        zookeeper.close();
    }

    public void createTopic(String topicName) throws Exception {
        kafkaAdminClient.createTopics(Collections.singleton(new NewTopic(topicName, 1, (short)1)))
            .all()
            .get(30, TimeUnit.SECONDS);
    }

    public int getKafkaPort() {
        return KAFKA_PORT;
    }

    private void createProducer() {
        Properties producerProps = new Properties();
        producerProps.setProperty(BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST + ":" + KAFKA_PORT);
        producerProps.setProperty(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(producerProps);
    }

    public void sendMessage(ProducerRecord<String, String> producerRecord) throws InterruptedException, ExecutionException, TimeoutException {
        producer.send(producerRecord).get(10, TimeUnit.SECONDS);
    }

    private void closeProducer() {
        producer.close();
    }
}
