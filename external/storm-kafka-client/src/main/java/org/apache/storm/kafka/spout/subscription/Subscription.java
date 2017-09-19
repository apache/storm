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

package org.apache.storm.kafka.spout.subscription;

import java.io.Serializable;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.task.TopologyContext;

/**
 * A subscription to kafka.
 */
public abstract class Subscription implements Serializable {
    private static final long serialVersionUID = -216136367240198716L;

    /**
     * Subscribe the KafkaConsumer to the proper topics.
     * Implementations must ensure that a given topic partition is always assigned to the same spout task.
     * Adding and removing partitions as necessary is fine, but partitions must not move from one task to another.
     * This constraint is only important for use with the Trident spout.
     * @param consumer the Consumer to get.
     * @param listener the rebalance listener to include in the subscription
     */
    public abstract <K, V> void subscribe(KafkaConsumer<K,V> consumer, ConsumerRebalanceListener listener, TopologyContext context);
    
    /**
     * Get the topics string.
     * @return A human-readable string representing the subscribed topics.
     */
    public abstract String getTopicsString();
    
    /**
     * NOOP is the default behavior, which means that Kafka will internally handle partition assignment.
     * If you wish to do manual partition management, you must provide an implementation of this method
     * that will check with kafka for any changes and call the ConsumerRebalanceListener from subscribe
     * to inform the rest of the system of those changes.
     */
    public void refreshAssignment() {
        //NOOP
    }
}
