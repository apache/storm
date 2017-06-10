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

import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.IPartitionedTridentSpout;
import org.apache.storm.trident.spout.ISpoutPartition;
import org.apache.storm.tuple.Fields;

// TODO
public class KafkaTridentSpoutTransactional<PartitionsT, P extends ISpoutPartition, T> 
        implements IPartitionedTridentSpout<PartitionsT, P, T> {
    @Override
    public Coordinator<PartitionsT> getCoordinator(Map<String, Object> conf, TopologyContext context) {
        return null;
    }

    @Override
    public Emitter<PartitionsT, P, T> getEmitter(Map<String, Object> conf, TopologyContext context) {
        return null;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return null;
    }
}
