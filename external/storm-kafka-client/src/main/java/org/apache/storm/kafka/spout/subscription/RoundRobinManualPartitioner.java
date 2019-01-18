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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.task.TopologyContext;

/**
 * Assign partitions in a round robin fashion for all spouts,
 * not just the ones that are alive. Because the parallelism of 
 * the spouts does not typically change while running this makes
 * the assignments more stable in the face of crashing spouts.
 * <p/>
 * Round Robin means that first spout of N spouts will get the first
 * partition, and the N+1th partition... The second spout will get the second partition and
 * N+2th partition etc.
 */
public class RoundRobinManualPartitioner implements ManualPartitioner {

    @Override
    public Set<TopicPartition> getPartitionsForThisTask(List<TopicPartition> allPartitionsSorted, TopologyContext context) {
        int thisTaskIndex = context.getThisTaskIndex();
        int totalTaskCount = context.getComponentTasks(context.getThisComponentId()).size();
        Set<TopicPartition> myPartitions = new HashSet<>(allPartitionsSorted.size() / totalTaskCount + 1);
        for (int i = thisTaskIndex; i < allPartitionsSorted.size(); i += totalTaskCount) {
            myPartitions.add(allPartitionsSorted.get(i));
        }
        return myPartitions;
    }
}
