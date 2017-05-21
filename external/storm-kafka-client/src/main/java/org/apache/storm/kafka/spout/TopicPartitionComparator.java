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

import java.util.Comparator;
import org.apache.kafka.common.TopicPartition;

/**
 * Singleton comparator of TopicPartitions.  Topics have precedence over partitions.
 * Topics are compared through String.compare and partitions are compared
 * numerically.
 * <p/>
 * Use INSTANCE for all sorting.
 */
public class TopicPartitionComparator implements Comparator<TopicPartition> {
    public static final TopicPartitionComparator INSTANCE = new TopicPartitionComparator();
    
    /**
     * Private to make it a singleton.
     */
    private TopicPartitionComparator() {
        //Empty
    }
    
    @Override
    public int compare(TopicPartition o1, TopicPartition o2) {
        if (!o1.topic().equals(o2.topic())) {
            return o1.topic().compareTo(o2.topic());
        } else {
            return o1.partition() - o2.partition();
        }
    }
}
