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
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;
import kafka.server.Server;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.storm.testing.TmpPath;

public class KafkaUnit {
    private TestingServer zookeeper;
    private Server kafkaServer;
    private KafkaProducer<String, String> producer;
    private AdminClient kafkaAdminClient;
    private TmpPath kafkaDir,metadata;
    private static final String KAFKA_HOST = "127.0.0.1";
    private static final int KAFKA_BROKER_PORT = 9092;
    private static final int KAFKA_CONTROLLER_PORT = 9093;

    public KafkaUnit() {
    }

    public void setUp() throws Exception {

        // setup Broker
        kafkaDir = new TmpPath(Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        metadata = new TmpPath(Paths.get(kafkaDir.getPath() + "/" + "meta.properties").toString());
        Files.createFile(metadata.getFile().toPath());
        String content ="node.id=0" + System.lineSeparator() + "version=1" + System.lineSeparator() + "cluster.id="+ RandomStringUtils.randomAlphanumeric(10);
        Files.writeString(metadata.getFile().toPath(), content, StandardOpenOption.APPEND);

        Properties brokerProps = new Properties();
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", kafkaDir.getPath());
        String listeners = String.format("BROKER://%s:%d", KAFKA_HOST, KAFKA_BROKER_PORT) + "," + String.format("CONTROLLER://%s:%d", KAFKA_HOST, KAFKA_CONTROLLER_PORT);
        brokerProps.setProperty("advertised.listeners", listeners);
        brokerProps.setProperty("listeners", listeners);
        brokerProps.setProperty("offsets.topic.replication.factor", "1");
        brokerProps.setProperty("process.roles","broker,controller");
        brokerProps.setProperty("controller.quorum.bootstrap.servers",String.format("%s:%d", KAFKA_HOST, KAFKA_CONTROLLER_PORT));
        brokerProps.setProperty("controller.listener.names","CONTROLLER");
        brokerProps.setProperty("listener.security.protocol.map","CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT");
        brokerProps.setProperty("inter.broker.listener.name","BROKER");
        brokerProps.setProperty("controller.quorum.voters","0@"+String.format("%s:%d", KAFKA_HOST, KAFKA_CONTROLLER_PORT));

        KafkaConfig config = new KafkaConfig(brokerProps);
        kafkaServer= new KafkaRaftServer(config, Time.SYSTEM);
        kafkaServer.startup();

        // setup default Producer
        createProducer();
        kafkaAdminClient = AdminClient.create(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST + ":" + KAFKA_BROKER_PORT));
    }

    public void tearDown() throws Exception {
        kafkaAdminClient.close();
        closeProducer();
        kafkaServer.shutdown();
        kafkaDir.close();
        metadata.close();
    }

    public void createTopic(String topicName) throws Exception {
        kafkaAdminClient.createTopics(Collections.singleton(new NewTopic(topicName, 1, (short)1)))
            .all()
            .get(30, TimeUnit.SECONDS);
    }

    public int getKafkaPort() {
        return KAFKA_BROKER_PORT;
    }

    private void createProducer() {
        Properties producerProps = new Properties();
        producerProps.setProperty(BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST + ":" + KAFKA_BROKER_PORT);
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
