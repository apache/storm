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

package org.apache.storm.kafka.spout.internal.partition;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.KafkaSpoutStreams;
import org.apache.storm.kafka.spout.KafkaSpoutStreamsNamedTopics;
import org.apache.storm.kafka.spout.KafkaSpoutStreamsWildcardTopics;

import java.util.HashSet;

public final class KafkaPartitionReaders {
    public static KafkaPartitionReader create(KafkaSpoutStreams kafkaSpoutStreams) {
        if (kafkaSpoutStreams instanceof KafkaSpoutStreamsNamedTopics) {
            return new NamedTopicPartitionReader(new HashSet<>(
                KafkaSpoutStreamsNamedTopics.class.cast(kafkaSpoutStreams).getTopics()));
        } else if (kafkaSpoutStreams instanceof KafkaSpoutStreamsWildcardTopics) {
            return new WildcardTopicPartitionReader(
                KafkaSpoutStreamsWildcardTopics.class.cast(kafkaSpoutStreams).getTopicWildcardPattern());
        } else {
            throw new IllegalArgumentException("Unrecognized kafka spout stream: " + kafkaSpoutStreams.getClass());
        }
    }

    public static TopicPartition toTopicPartition(PartitionInfo partitionInfo) {
        return new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
    }
}
