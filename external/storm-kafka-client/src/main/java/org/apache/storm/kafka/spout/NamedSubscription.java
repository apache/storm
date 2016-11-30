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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Subscribe to all topics that follow a given list of values
 */
public class NamedSubscription extends Subscription {
    private static final Logger LOG = LoggerFactory.getLogger(NamedSubscription.class);
    private static final long serialVersionUID = 3438543305215813839L;
    protected final Collection<String> topics;
    
    public NamedSubscription(Collection<String> topics) {
        this.topics = Collections.unmodifiableCollection(new ArrayList<>(topics));
    }
    
    public NamedSubscription(String ... topics) {
        this(Arrays.asList(topics));
    }

    @Override
    public <K, V> void subscribe(KafkaConsumer<K, V> consumer, ConsumerRebalanceListener listener, TopologyContext unused) {
        consumer.subscribe(topics, listener);
        LOG.info("Kafka consumer subscribed topics {}", topics);
        
        // Initial poll to get the consumer registration process going.
        // KafkaSpoutConsumerRebalanceListener will be called following this poll, upon partition registration
        consumer.poll(0);
    }

    @Override
    public String getTopicsString() {
        return String.valueOf(topics);
    }
}
