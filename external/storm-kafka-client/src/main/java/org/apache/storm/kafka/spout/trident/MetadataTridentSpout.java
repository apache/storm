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

package org.apache.storm.kafka.spout.trident;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.Serializable;

class MetadataTridentSpout<K,V> implements Serializable {
    private String topic;
    private int partition = Integer.MIN_VALUE;

    private long firstOffset = Long.MIN_VALUE;
    private long lastOffset = Long.MIN_VALUE;

    public MetadataTridentSpout(ConsumerRecords<K,V> consumerRecords) {
        for (ConsumerRecord<K, V> consumerRecord : consumerRecords) {
            if (topic == null ) {
                topic = consumerRecord.topic();
            }
            if(partition == Integer.MIN_VALUE) {
                partition = consumerRecord.partition();
            }

            long offset = consumerRecord.offset();

            if (firstOffset == Long.MIN_VALUE || firstOffset > offset) {
                firstOffset = offset;
            }

            if (lastOffset == Long.MIN_VALUE || lastOffset < offset) {
                lastOffset = offset;
            }
        }
    }

    @Override
    public String toString() {
        return "MyMeta{" +
                "firstOffset=" + firstOffset +
                ", lastOffset=" + lastOffset +
                '}';
    }
}
