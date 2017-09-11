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
import java.util.List;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.task.TopologyContext;

/**
 * A function used to assign partitions to this spout.
 * WARNING if this is not done correctly you can really mess things up, like not reading data in some partitions.
 * The complete TopologyContext is passed in, but it is suggested that you use the index of the spout and the total
 * number of spouts to avoid missing partitions or double assigning partitions.
 */
@FunctionalInterface
public interface ManualPartitioner extends Serializable {
    /**
     * Get the partitions for this assignment.
     * @param allPartitions all of the partitions that the set of spouts want to subscribe to, in a strict ordering
     * @param context the context of the topology
     * @return the subset of the partitions that this spout should use.
     */
    public List<TopicPartition> partition(List<TopicPartition> allPartitions, TopologyContext context);
}
