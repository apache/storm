/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.scheduler.blacklist.strategies;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.Topologies;

public interface IBlacklistStrategy {

    void prepare(Map<String, Object> conf);

    /**
     * Get blacklist by blacklist strategy.
     *
     * @param badSupervisorsToleranceSlidingWindow bad supervisors buffered in sliding window
     * @param sendAssignmentFailureCount supervisors with failed assignment calls in sliding window
     * @param cluster the cluster these topologies are running in. `cluster` contains everything user
     *       need to develop a new scheduling logic. e.g. supervisors information, available slots, current
     *       assignments for all the topologies etc. User can set the new assignment for topologies using
     *       cluster.setAssignmentById()`
     * @param topologies all the topologies in the cluster, some of them need schedule. Topologies object here
     *       only contain static information about topologies. Information like assignments, slots are all in
     *       the `cluster` object.
     * @return blacklisted supervisors' id set
     */
    Set<String> getBlacklist(List<Map<String, Set<Integer>>> badSupervisorsToleranceSlidingWindow,
                             List<Map<String, Integer>> sendAssignmentFailureCount,
                             Cluster cluster, Topologies topologies);

    /**
     * resume supervisors form blacklist. Blacklist is just a temporary list for supervisors,
     * or there will be less and less available resources.
     * This will be called every time before getBlacklist() and schedule.
     */
    void resumeFromBlacklist();

}
