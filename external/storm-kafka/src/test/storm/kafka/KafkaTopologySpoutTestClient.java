/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class KafkaTopologySpoutTestClient {

    private static StormTopology buildTopology(String zKeeperHost) {
        ZkHosts zkHosts = new ZkHosts(zKeeperHost);
        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, "test", "/test", UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout);

        Properties props = new Properties();
        props.put("zookeeper.connect", zKeeperHost);
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaBolt bolt = new KafkaBolt();
        builder.setBolt("kafkaBolt", bolt).shuffleGrouping("kafkaSpout");

        return builder.createTopology();
    }

    /**
     * To run this topology ensure you have a kafka broker running and provide connection string to broker as argument.
     * Create a topic test with command line,
     * kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partition 1 --topic test
     * <p>
     * run this program and run the kafka consumer:
     * kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning
     * <p>
     * you should see the messages flowing through.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Please provide zookeeper host,e.g. localhost:2181");
            System.exit(1);
        }

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafkaTopologySpoutTest", new Config(), buildTopology(args[0]));
        Thread.sleep(30 * 1000);
        cluster.killTopology("kafkaTopologySpoutTest");

        cluster.shutdown();
    }

    private static class KafkaBolt extends BaseRichBolt {
        public static final Logger LOG = LoggerFactory.getLogger(KafkaBolt.class);

        private OutputCollector collector;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            LOG.info("RECEIVED A TUPLE " + tuple.getString(0));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        }
    }
}
