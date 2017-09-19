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


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;

import java.io.Serializable;

/**
 * This example sets up a few topologies to put random strings in the "test" Kafka topic via the KafkaBolt,
 * and shows how to set up a Trident topology that reads from the "test" topic using the KafkaSpout.
 * Please ensure you have a Kafka instance running on localhost:9092 before you deploy this example.
 */
public class TridentKafkaRandomStrings implements Serializable {
    public static void main(String[] args) throws Exception {
        final String[] zkBrokerUrl = parseUrl(args);
        final String topicName = "test";
        Config tpConf = new Config();
        tpConf.setMaxSpoutPending(20);
        String prodTpName = "kafkaBolt";
        String consTpName = "kafka-topology-random-strings";

        if (args.length == 3)  {
            prodTpName = args[2] + "-producer";
            consTpName = args[2] + "-consumer";
        }
        // Producer
        StormSubmitter.submitTopology(prodTpName, tpConf, KafkaProducerTopology.newTopology(zkBrokerUrl[1], topicName));
        // Consumer
        StormSubmitter.submitTopology(consTpName, tpConf, TridentKafkaConsumerTopology.newTopology(
                new TransactionalTridentKafkaSpout(newTridentKafkaConfig(zkBrokerUrl[0]))));
    }

    private static String[] parseUrl(String[] args) {
        String zkUrl = "localhost:2181";        // the defaults.
        String brokerUrl = "localhost:9092";

        if (args.length > 3 || (args.length == 1 && args[0].matches("^-h|--help$"))) {
            System.out.println("Usage: TridentKafkaWordCount [kafka zookeeper url] [kafka broker url] [topology name]");
            System.out.println("   E.g TridentKafkaWordCount [" + zkUrl + "]" + " [" + brokerUrl + "] [wordcount]");
            System.exit(1);
        } else if (args.length == 1) {
            zkUrl = args[0];
        } else if (args.length == 2) {
            zkUrl = args[0];
            brokerUrl = args[1];
        }

        System.out.println("Using Kafka zookeeper uHrl: " + zkUrl + " broker url: " + brokerUrl);
        return new String[]{zkUrl, brokerUrl};
    }

    private static TridentKafkaConfig newTridentKafkaConfig(String zkUrl) {
        ZkHosts hosts = new ZkHosts(zkUrl);
        TridentKafkaConfig config = new TridentKafkaConfig(hosts, "test");
        config.scheme = new SchemeAsMultiScheme(new StringScheme());

        // Consume new data from the topic
        config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        return config;
    }
}
