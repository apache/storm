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

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public class KafkaSpoutTuplesBuilderWildcardTopics<K,V> implements KafkaSpoutTuplesBuilder<K,V> {
    private KafkaSpoutTupleBuilder<K, V> tupleBuilder;

    public KafkaSpoutTuplesBuilderWildcardTopics(KafkaSpoutTupleBuilder<K, V> tupleBuilder) {
        this.tupleBuilder = tupleBuilder;
    }

    @Override
    public List<Object> buildTuple(ConsumerRecord<K, V> consumerRecord) {
        return tupleBuilder.buildTuple(consumerRecord);
    }
}
