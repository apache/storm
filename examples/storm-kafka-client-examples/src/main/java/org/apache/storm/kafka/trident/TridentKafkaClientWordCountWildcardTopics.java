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

import org.apache.storm.kafka.spout.KafkaSpoutStream;
import org.apache.storm.kafka.spout.KafkaSpoutStreams;
import org.apache.storm.kafka.spout.KafkaSpoutStreamsWildcardTopics;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilder;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilderWildcardTopics;
import org.apache.storm.tuple.Fields;

import java.util.regex.Pattern;

public class TridentKafkaClientWordCountWildcardTopics extends TridentKafkaClientWordCountNamedTopics {
    private static final String TOPIC_WILDCARD_PATTERN = "test-trident(-1)?";

    protected KafkaSpoutTuplesBuilder<String, String> newTuplesBuilder() {
        return new KafkaSpoutTuplesBuilderWildcardTopics<>(new TopicsTupleBuilder<String, String>(TOPIC_WILDCARD_PATTERN));
    }

    protected KafkaSpoutStreams newKafkaSpoutStreams() {
        final Fields outputFields = new Fields("str");
        final KafkaSpoutStream kafkaSpoutStream = new KafkaSpoutStream(outputFields, Pattern.compile(TOPIC_WILDCARD_PATTERN));
        return new KafkaSpoutStreamsWildcardTopics(kafkaSpoutStream);
    }

    public static void main(String[] args) throws Exception {
        new TridentKafkaClientWordCountWildcardTopics().run(args);
    }
}
