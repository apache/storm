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

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.task.TopologyContext;


/**
 * The KafkaTupleListener handles state changes of a kafka tuple inside a KafkaSpout.
 */
public interface KafkaTupleListener extends Serializable {


    /**
     * Called during the initialization of the kafka spout.
     *
     * @param conf The storm configuration.
     * @param context The {@link TopologyContext}
     */
    void open(Map<String, Object> conf, TopologyContext context);

    /**
     * Called when the tuple is emitted and auto commit is disabled.
     * If kafka auto commit is enabled, the kafka consumer will periodically (depending on the commit interval)
     * commit the offsets. Therefore, storm disables anchoring for tuples when auto commit is enabled and the spout will
     * not receive acks and fails for those tuples.
     *
     * @param tuple the storm tuple.
     * @param msgId The id of the tuple in the spout.
     */
    void onEmit(List<Object> tuple, KafkaSpoutMessageId msgId);

    /**
     * Called when a tuple is acked.
     *
     * @param msgId The id of the tuple in the spout.
     */
    void onAck(KafkaSpoutMessageId msgId);

    /**
     * Called when kafka partitions are rebalanced.
     *
     * @param partitions The list of partitions that are now assigned to the consumer (may include partitions previously
     *                   assigned to the consumer)
     */
    void onPartitionsReassigned(Collection<TopicPartition> partitions);

    /**
     * Called when the Kafka spout sets a record for retry.
     *
     * @param msgId The id of the tuple in the spout.
     */
    void onRetry(KafkaSpoutMessageId msgId);

    /**
     * Called when the maximum number of retries have been reached.
     *
     * @param msgId The id of the tuple in the spout.
     */
    void onMaxRetryReached(KafkaSpoutMessageId msgId);
}
