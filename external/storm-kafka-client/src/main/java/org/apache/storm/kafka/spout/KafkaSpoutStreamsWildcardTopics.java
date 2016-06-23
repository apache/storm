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

package org.apache.storm.kafka.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;

import java.util.List;
import java.util.regex.Pattern;

public class KafkaSpoutStreamsWildcardTopics implements KafkaSpoutStreams {
    private KafkaSpoutStream kafkaSpoutStream;

    public KafkaSpoutStreamsWildcardTopics(KafkaSpoutStream kafkaSpoutStream) {
        this.kafkaSpoutStream = kafkaSpoutStream;
        if (kafkaSpoutStream.getTopicWildcardPattern() == null) {
            throw new IllegalStateException("KafkaSpoutStream must be configured for wildcard topic");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        kafkaSpoutStream.declareOutputFields(declarer);
    }

    @Override
    public void emit(SpoutOutputCollector collector, List<Object> tuple, KafkaSpoutMessageId messageId) {
        kafkaSpoutStream.emit(collector, tuple, messageId);
    }

    public KafkaSpoutStream getStream() {
        return kafkaSpoutStream;
    }

    public Pattern getTopicWildcardPattern() {
        return kafkaSpoutStream.getTopicWildcardPattern();
    }

    @Override
    public String toString() {
        return "KafkaSpoutStreamsWildcardTopics{" +
                "kafkaSpoutStream=" + kafkaSpoutStream +
                '}';
    }
}
