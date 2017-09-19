/*
 * Copyright 2017 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kafka.spout;

import static org.apache.storm.kafka.spout.KafkaSpoutTopologyMainNamedTopics.TOPIC_0;
import static org.apache.storm.kafka.spout.KafkaSpoutTopologyMainNamedTopics.TOPIC_1;
import static org.apache.storm.kafka.spout.KafkaSpoutTopologyMainNamedTopics.TOPIC_2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.bolt.KafkaProducerTopology;

public class KafkaSpoutTopologyMainNamedTopicsLocal {

    public static void main(String[] args) throws Exception {
        new KafkaSpoutTopologyMainNamedTopicsLocal().runExample();
    }
    
    protected void runExample() throws Exception {
        String brokerUrl = "localhost:9092";
        KafkaSpoutTopologyMainNamedTopics example = getTopology();
        Config tpConf = example.getConfig();
        
        LocalCluster localCluster = new LocalCluster();
        // Producers. This is just to get some data in Kafka, normally you would be getting this data from elsewhere
        localCluster.submitTopology(TOPIC_0 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, TOPIC_0));
        localCluster.submitTopology(TOPIC_1 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, TOPIC_1));
        localCluster.submitTopology(TOPIC_2 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, TOPIC_2));

        //Consumer. Sets up a topology that reads the given Kafka spouts and logs the received messages
        localCluster.submitTopology("storm-kafka-client-spout-test", tpConf, example.getTopologyKafkaSpout(example.getKafkaSpoutConfig(brokerUrl)));
        
        stopWaitingForInput();
    }
    
    protected KafkaSpoutTopologyMainNamedTopics getTopology() {
        return new KafkaSpoutTopologyMainNamedTopics();
    }
    
    protected void stopWaitingForInput() {
        try {
            System.out.println("PRESS ENTER TO STOP");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
            System.exit(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
