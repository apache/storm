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
package storm.kafka.bolt.test.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.google.common.collect.Lists;
import storm.kafka.Broker;
import storm.kafka.KafkaConfig;
import storm.kafka.KafkaSpout;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class KafkaWordCount {
    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String HBASE_BOLT = "HBASE_BOLT";


    public static void main(String[] args) throws Exception {
        Config config = getConfig(args[0]);

        WordSpout spout = new WordSpout();
        WordCounter bolt = new WordCounter();
        KafkaBolt<String, String> kafkaBolt = new KafkaBolt<String, String>()
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>())
                .withTopicSelector(new DefaultTopicSelector("test"));

        KafkaConfig kafkaConfig = new KafkaConfig(Lists.newArrayList(new Broker("localhost")), "test");
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

        // wordSpout ==> countBolt ==> kafkaBolt => KafkaSpout => WordCount
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(WORD_SPOUT, spout, 1);
        builder.setBolt(COUNT_BOLT, bolt, 1).shuffleGrouping(WORD_SPOUT);
        builder.setBolt(HBASE_BOLT, kafkaBolt, 1).fieldsGrouping(COUNT_BOLT, new Fields(FieldNameBasedTupleToKafkaMapper.BOLT_KEY));



        builder.setSpout("KAFKA_SPOUT", kafkaSpout, 2);
        builder.setBolt("WORD_COUNT", new WordCounter1(), 1).allGrouping("KAFKA_SPOUT");

        if (args.length == 1) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", config, builder.createTopology());
            Thread.sleep(30000);
            cluster.killTopology("test");
            cluster.shutdown();
            System.exit(0);
        } else if (args.length == 2) {
            StormSubmitter.submitTopology(args[1], config, builder.createTopology());
        } else {
            System.out.println("Usage: HdfsFileTopology <hdfs url> [topology name]");
        }
    }

    private  static Config getConfig(String brokerConnectionString) {
        Config conf = new Config();
        Map config = new HashMap();
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerConnectionString);
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
        return conf;
    }
}
