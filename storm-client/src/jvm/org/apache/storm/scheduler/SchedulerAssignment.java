/**
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
 */
package org.apache.storm.scheduler;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.WorkerResources;

public interface SchedulerAssignment {
    /**
     * Does this slot occupied by this assignment?
     * @param slot
     * @return true if the slot is occupied else false
     */
    public boolean isSlotOccupied(WorkerSlot slot);

    /**
     * Is the executor assigned?
     * 
     * @param executor
     * @return true if it is assigned else false
     */
    public boolean isExecutorAssigned(ExecutorDetails executor);
    
    /**
     * Return the ID of the topology.
     * @return the topology-id this assignment is for.
     */
    public String getTopologyId();

    /**
     * @return the executor -> slot map.
     */
    public Map<ExecutorDetails, WorkerSlot> getExecutorToSlot();

    /**
     * @return  the executors covered by this assignments
     */
    public Set<ExecutorDetails> getExecutors();
    
    public Set<WorkerSlot> getSlots();

    public Map<WorkerSlot, Collection<ExecutorDetails>> getSlotToExecutors();
    
    /**
     * @return The slot to resource mapping
     */
    public Map<WorkerSlot, WorkerResources> getScheduledResources();
    
    /**
     * @return host to total shared off heap memory mapping.
     */
    public Map<String, Double> getNodeIdToTotalSharedOffHeapMemory();
}
