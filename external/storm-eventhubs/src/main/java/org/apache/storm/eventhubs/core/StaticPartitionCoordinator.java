/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.apache.storm.eventhubs.core;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.storm.eventhubs.state.IStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticPartitionCoordinator implements IPartitionCoordinator {
    private static final Logger logger = LoggerFactory.getLogger(StaticPartitionCoordinator.class);

    protected final Map<String, IPartitionManager> partitionManagerMap;
    protected final List<IPartitionManager> partitionManagers;

    public StaticPartitionCoordinator(EventHubConfig ehConfig, int taskIndex, int totalTasks,
            IStateStore stateStore, IPartitionManagerFactory pmFactory, IEventHubReceiverFactory recvFactory) {
        logger.info("Computing partition managers for TaskIndex: " + taskIndex + ", Total tasks: " +
                totalTasks + ", Total Partitions: " + ehConfig.getPartitionCount());
        
    	this.partitionManagerMap = new TreeMap<String, IPartitionManager>();
    	this.partitionManagers = new LinkedList<IPartitionManager>();
    	
        for (int partitionId = taskIndex; partitionId < ehConfig.getPartitionCount(); partitionId += totalTasks) {
        	String partitionIdStr = String.valueOf(partitionId);
            IEventHubReceiver receiver = recvFactory.create(ehConfig, partitionIdStr);
            IPartitionManager partitionManager = pmFactory.create(ehConfig, partitionIdStr, stateStore, receiver);
            this.partitionManagerMap.put(partitionIdStr, partitionManager);
            this.partitionManagers.add(partitionManager);
            logger.info("taskIndex " + taskIndex + " owns partitionId " + partitionIdStr);
        }
    }

    @Override
    public List<IPartitionManager> getMyPartitionManagers() {
        return this.partitionManagers;
    }

    @Override
    public IPartitionManager getPartitionManager(String partitionId) {
        return partitionManagerMap.get(partitionId);
    }
}
