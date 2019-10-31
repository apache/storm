/*
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
import org.apache.storm.generated.WorkerResources;

public interface SchedulerAssignment {
    /**
     * Is this slot part of this assignment or not.
     *
     * @param slot the slot to check.
     * @return true if the slot is occupied by this assignment else false.
     */
    boolean isSlotOccupied(WorkerSlot slot);

    /**
     * Is the executor assigned or not.
     *
     * @param executor the executor to check it if is assigned.
     * @return true if it is assigned else false
     */
    boolean isExecutorAssigned(ExecutorDetails executor);

    /**
     * Return the ID of the topology.
     *
     * @return the topology-id this assignment is for.
     */
    String getTopologyId();

    /**
     * Get the map of executor to WorkerSlot.
     *
     * @return the executor -> slot map.
     */
    Map<ExecutorDetails, WorkerSlot> getExecutorToSlot();

    /**
     * Get the set of all executors.
     *
     * @return the executors covered by this assignments
     */
    Set<ExecutorDetails> getExecutors();

    /**
     * Get the set of all slots that are a part of this.
     *
     * @return the set of all slots.
     */
    Set<WorkerSlot> getSlots();

    /**
     * Get the mapping of slot to executors on that slot.
     *
     * @return the slot to the executors assigned to that slot.
     */
    Map<WorkerSlot, Collection<ExecutorDetails>> getSlotToExecutors();

    /**
     * Get the slot to resource mapping.
     *
     * @return The slot to resource mapping
     */
    Map<WorkerSlot, WorkerResources> getScheduledResources();

    /**
     * Get the total shared off heap node memory mapping.
     *
     * @return host to total shared off heap node memory mapping.
     */
    Map<String, Double> getNodeIdToTotalSharedOffHeapNodeMemory();
}
