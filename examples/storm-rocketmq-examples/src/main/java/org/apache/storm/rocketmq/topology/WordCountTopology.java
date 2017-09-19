/**
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
package org.apache.storm.rocketmq.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalCluster.LocalTopology;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.rocketmq.RocketMqConfig;
import org.apache.storm.rocketmq.SpoutConfig;
import org.apache.storm.rocketmq.bolt.RocketMqBolt;
import org.apache.storm.rocketmq.common.mapper.FieldNameBasedTupleToMessageMapper;
import org.apache.storm.rocketmq.common.mapper.TupleToMessageMapper;
import org.apache.storm.rocketmq.common.selector.DefaultTopicSelector;
import org.apache.storm.rocketmq.common.selector.TopicSelector;
import org.apache.storm.rocketmq.spout.RocketMqSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Properties;

public class WordCountTopology {
    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String INSERT_BOLT = "INSERT_BOLT";

    private static final String CONSUMER_GROUP = "wordcount";
    private static final String CONSUMER_TOPIC = "wordcountsource";

    public static StormTopology buildTopology(String nameserverAddr, String topic){
        Properties properties = new Properties();
        properties.setProperty(SpoutConfig.NAME_SERVER_ADDR, nameserverAddr);
        properties.setProperty(SpoutConfig.CONSUMER_GROUP, CONSUMER_GROUP);
        properties.setProperty(SpoutConfig.CONSUMER_TOPIC, CONSUMER_TOPIC);

        RocketMqSpout spout = new RocketMqSpout(properties);

        WordCounter bolt = new WordCounter();

        TupleToMessageMapper mapper = new FieldNameBasedTupleToMessageMapper("word", "count");
        TopicSelector selector = new DefaultTopicSelector(topic);

        properties = new Properties();
        properties.setProperty(RocketMqConfig.NAME_SERVER_ADDR, nameserverAddr);

        RocketMqBolt insertBolt = new RocketMqBolt()
                .withMapper(mapper)
                .withSelector(selector)
                .withProperties(properties);

        // wordSpout ==> countBolt ==> insertBolt
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(WORD_SPOUT, spout, 1);
        builder.setBolt(COUNT_BOLT, bolt, 1).shuffleGrouping(WORD_SPOUT);
        builder.setBolt(INSERT_BOLT, insertBolt, 1).fieldsGrouping(COUNT_BOLT, new Fields("word"));

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(5);
        if (args.length == 2) {
            try (LocalCluster cluster = new LocalCluster();
                 LocalTopology topo = cluster.submitTopology("wordCounter", conf, buildTopology(args[0], args[1]));) {
                Thread.sleep(120 * 1000);
            }
            System.exit(0);
        }
        else if(args.length == 3) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[2], conf, buildTopology(args[0], args[1]));
        } else{
            System.out.println("Usage: WordCountTopology <nameserver addr> <topic> [topology name]");
        }
    }

}
