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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.jms.example;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.jms.JmsMessageProducer;
import org.apache.storm.jms.JmsProvider;
import org.apache.storm.jms.JmsTupleProducer;
import org.apache.storm.jms.bolt.JmsBolt;
import org.apache.storm.jms.spout.JmsSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.utils.Utils;

public class ExampleJmsTopology {
    public static final String JMS_QUEUE_SPOUT = "JMS_QUEUE_SPOUT";
    public static final String INTERMEDIATE_BOLT = "INTERMEDIATE_BOLT";
    public static final String FINAL_BOLT = "FINAL_BOLT";
    public static final String JMS_TOPIC_BOLT = "JMS_TOPIC_BOLT";
    public static final String JMS_TOPIC_SPOUT = "JMS_TOPIC_SPOUT";
    public static final String ANOTHER_BOLT = "ANOTHER_BOLT";

    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {

        // JMS Queue Provider
        JmsProvider jmsQueueProvider = new SpringJmsProvider(
                "jms-activemq.xml", "jmsConnectionFactory",
                "notificationQueue");

        // JMS Topic provider
        JmsProvider jmsTopicProvider = new SpringJmsProvider(
                "jms-activemq.xml", "jmsConnectionFactory",
                "notificationTopic");

        // JMS Producer
        JmsTupleProducer producer = new JsonTupleProducer();

        // JMS Queue Spout
        JmsSpout queueSpout = new JmsSpout();
        queueSpout.setJmsProvider(jmsQueueProvider);
        queueSpout.setJmsTupleProducer(producer);
        queueSpout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        queueSpout.setDistributed(true); // allow multiple instances

        TopologyBuilder builder = new TopologyBuilder();

        // spout with 5 parallel instances
        builder.setSpout(JMS_QUEUE_SPOUT, queueSpout, 5);

        // intermediate bolt, subscribes to jms spout, anchors on tuples, and auto-acks
        builder.setBolt(INTERMEDIATE_BOLT,
                new GenericBolt("INTERMEDIATE_BOLT", true, true, new Fields("json")), 3).shuffleGrouping(
                JMS_QUEUE_SPOUT);

        // bolt that subscribes to the intermediate bolt, and auto-acks
        // messages.
        builder.setBolt(FINAL_BOLT, new GenericBolt("FINAL_BOLT", true, true), 3).shuffleGrouping(
                INTERMEDIATE_BOLT);

        // bolt that subscribes to the intermediate bolt, and publishes to a JMS Topic
        JmsBolt jmsBolt = new JmsBolt();
        jmsBolt.setJmsProvider(jmsTopicProvider);

        // anonymous message producer just calls toString() on the tuple to create a jms message
        jmsBolt.setJmsMessageProducer(new JmsMessageProducer() {
            @Override
            public Message toMessage(Session session, ITuple input) throws JMSException {
                System.out.println("Sending JMS Message:" + input.toString());
                TextMessage tm = session.createTextMessage(input.toString());
                return tm;
            }
        });

        builder.setBolt(JMS_TOPIC_BOLT, jmsBolt).shuffleGrouping(INTERMEDIATE_BOLT);

        // JMS Topic spout
        JmsSpout topicSpout = new JmsSpout();
        topicSpout.setJmsProvider(jmsTopicProvider);
        topicSpout.setJmsTupleProducer(producer);
        topicSpout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        topicSpout.setDistributed(false);

        builder.setSpout(JMS_TOPIC_SPOUT, topicSpout);

        builder.setBolt(ANOTHER_BOLT, new GenericBolt("ANOTHER_BOLT", true, true), 1).shuffleGrouping(
                JMS_TOPIC_SPOUT);

        Config conf = new Config();

        if (args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf,
                    builder.createTopology());
        } else {

            conf.setDebug(true);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("storm-jms-example", conf, builder.createTopology());
            Utils.sleep(60000);
            cluster.killTopology("storm-jms-example");
            cluster.shutdown();
        }
    }

}
