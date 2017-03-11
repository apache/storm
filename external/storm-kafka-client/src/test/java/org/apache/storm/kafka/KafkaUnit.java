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

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class KafkaUnit {
    private KafkaServer kafkaServer;
    private EmbeddedZookeeper zkServer;
    private ZkUtils zkUtils;
    private KafkaProducer<String, String> producer;
    private static final String ZK_HOST = "127.0.0.1";
    private static final String KAFKA_HOST = "127.0.0.1";
    private static final int KAFKA_PORT = 9092;

    public KafkaUnit() {
    }

    public void setUp() throws IOException {
        // setup ZK
        zkServer = new EmbeddedZookeeper();
        String zkConnect = ZK_HOST + ":" + zkServer.port();
        ZkClient zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        zkUtils = ZkUtils.apply(zkClient, false);

        // setup Broker
        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", String.format("PLAINTEXT://%s:%d", KAFKA_HOST, KAFKA_PORT));
        KafkaConfig config = new KafkaConfig(brokerProps);
        MockTime mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);

        // setup default Producer
        createProducer();
    }

    public void tearDown() {
        closeProducer();
        kafkaServer.shutdown();
        zkUtils.close();
        zkServer.shutdown();
    }

    public void createTopic(String topicName) {
        AdminUtils.createTopic(zkUtils, topicName, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
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

    public void createProducer(Serializer keySerializer, Serializer valueSerializer) {
        Properties producerProps = new Properties();
        producerProps.setProperty(BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST + ":" + KAFKA_PORT);
        producer = new KafkaProducer<>(producerProps, keySerializer, valueSerializer);
    }

    public void sendMessage(ProducerRecord producerRecord) throws InterruptedException, ExecutionException, TimeoutException {
        producer.send(producerRecord).get(10, TimeUnit.SECONDS);
    }

    private void closeProducer() {
        producer.close();
    }
}