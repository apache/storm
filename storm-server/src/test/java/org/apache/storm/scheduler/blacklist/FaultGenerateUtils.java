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
package org.apache.storm.scheduler.blacklist;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.resource.normalization.ResourceMetrics;

public class FaultGenerateUtils {

    public static List<Map<String, SupervisorDetails>> getSupervisorsList(int supervisorCount, int slotCount, List<Map<Integer, List<Integer>>> faultList) {
        List<Map<String, SupervisorDetails>> supervisorsList = new ArrayList<>(faultList.size());
        for (Map<Integer, List<Integer>> faults : faultList) {
            Map<String, SupervisorDetails> supervisors = TestUtilsForBlacklistScheduler.genSupervisors(supervisorCount, slotCount);
            for (Map.Entry<Integer, List<Integer>> fault : faults.entrySet()) {
                int supervisor = fault.getKey();
                List<Integer> slots = fault.getValue();
                if (slots.isEmpty()) {
                    supervisors = TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supervisors, "sup-" + supervisor);
                } else {
                    for (int slot : slots) {
                        supervisors = TestUtilsForBlacklistScheduler.removePortFromSupervisors(supervisors, "sup-" + supervisor, slot);
                    }
                }
            }
            supervisorsList.add(supervisors);
        }
        return supervisorsList;
    }

    public static Cluster nextCluster(Cluster cluster, Map<String, SupervisorDetails> supervisors, INimbus iNimbus, Map<String, Object> config,
                                      Topologies topologies) {
        Map<String, SchedulerAssignmentImpl> assignment;
        if (cluster == null) {
            assignment = new HashMap<>();
        } else {
            assignment = TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments());
        }
        return new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supervisors, assignment, topologies, config);
    }
}
