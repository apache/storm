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

package org.apache.storm.kafka.spout.test;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutStream;
import org.apache.storm.kafka.spout.KafkaSpoutStreams;
import org.apache.storm.kafka.spout.KafkaSpoutStreamsWildcardTopics;
import org.apache.storm.kafka.spout.KafkaSpoutTupleBuilder;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilder;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilderWildcardTopics;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.regex.Pattern;

public class KafkaSpoutTopologyMainWildcardTopics extends KafkaSpoutTopologyMainNamedTopics {
    private static final String STREAM = "test_wildcard_stream";
    private static final String TOPIC_WILDCARD_PATTERN = "test[1|2]";

    public static void main(String[] args) throws Exception {
        new KafkaSpoutTopologyMainWildcardTopics().runMain(args);
    }

    protected StormTopology getTopolgyKafkaSpout() {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig(getKafkaSpoutStreams())), 1);
        tp.setBolt("kafka_bolt", new KafkaSpoutTestBolt()).shuffleGrouping("kafka_spout", STREAM);
        return tp.createTopology();
    }

    protected KafkaSpoutTuplesBuilder<String, String> getTuplesBuilder() {
        return new KafkaSpoutTuplesBuilderWildcardTopics<>(getTupleBuilder());
    }

    protected KafkaSpoutTupleBuilder<String, String> getTupleBuilder() {
        return new TopicsTest0Test1TupleBuilder<>(TOPIC_WILDCARD_PATTERN);
    }

    protected KafkaSpoutStreams getKafkaSpoutStreams() {
        final Fields outputFields = new Fields("topic", "partition", "offset", "key", "value");
        final KafkaSpoutStream kafkaSpoutStream = new KafkaSpoutStream(outputFields, STREAM, Pattern.compile(TOPIC_WILDCARD_PATTERN));
        return new KafkaSpoutStreamsWildcardTopics(kafkaSpoutStream);
    }
}
