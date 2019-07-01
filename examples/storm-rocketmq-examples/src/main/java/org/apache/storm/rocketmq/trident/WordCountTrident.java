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

package org.apache.storm.rocketmq.trident;

import java.util.Properties;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.rocketmq.RocketMqConfig;
import org.apache.storm.rocketmq.common.mapper.FieldNameBasedTupleToMessageMapper;
import org.apache.storm.rocketmq.common.mapper.TupleToMessageMapper;
import org.apache.storm.rocketmq.common.selector.DefaultTopicSelector;
import org.apache.storm.rocketmq.common.selector.TopicSelector;
import org.apache.storm.rocketmq.trident.state.RocketMqState;
import org.apache.storm.rocketmq.trident.state.RocketMqStateFactory;
import org.apache.storm.rocketmq.trident.state.RocketMqStateUpdater;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class WordCountTrident {

    public static StormTopology buildTopology(String nameserverAddr, String topic) {
        Fields fields = new Fields("word", "count");
        FixedBatchSpout spout = new FixedBatchSpout(fields, 4,
                new Values("storm", 1),
                new Values("trident", 1),
                new Values("needs", 1),
                new Values("javadoc", 1)
        );
        spout.setCycle(true);

        TupleToMessageMapper mapper = new FieldNameBasedTupleToMessageMapper("word", "count");
        TopicSelector selector = new DefaultTopicSelector(topic);

        Properties properties = new Properties();
        properties.setProperty(RocketMqConfig.NAME_SERVER_ADDR, nameserverAddr);

        RocketMqState.Options options = new RocketMqState.Options()
                .withMapper(mapper)
                .withSelector(selector)
                .withProperties(properties);

        StateFactory factory = new RocketMqStateFactory(options);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);

        stream.partitionPersist(factory, fields,
                new RocketMqStateUpdater(), new Fields());

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(5);
        conf.setNumWorkers(3);

        String topologyName = "wordCounter";
        if (args.length < 2) {
            System.out.println("Usage: WordCountTrident <nameserver addr> <topic> [topology name]");
        } else {
            if (args.length > 3) {
                topologyName = args[2];
            }
            StormSubmitter.submitTopology(topologyName, conf, buildTopology(args[0], args[1]));
        }
    }
}
