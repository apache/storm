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

package org.apache.storm.scheduler.resource;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.storm.daemon.nimbus.TopologyResources;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ISchedulingState;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.TopologyDetails;

public class User {
    //Topologies that were deemed to be invalid
    private final Set<TopologyDetails> unsuccess = new HashSet<>();
    private final double cpuGuarantee;
    private final double memoryGuarantee;
    private final Map<String, Double> genericGuarantee;
    private String userId;

    public User(String userId) {
        this(userId, 0, 0, Collections.emptyMap());
    }

    public User(String userId, Map<String, Double> resourcePool) {
        this(
            userId,
            resourcePool == null ? 0.0 : resourcePool.getOrDefault("cpu", 0.0),
            resourcePool == null ? 0.0 : resourcePool.getOrDefault("memory", 0.0),
            resourcePool == null ? Collections.emptyMap() : extractGenericResourceEntries(resourcePool));
    }

    private User(String userId, double cpuGuarantee, double memoryGuarantee, Map<String, Double> genericGuarantee) {
        this.userId = userId;
        this.cpuGuarantee = cpuGuarantee;
        this.memoryGuarantee = memoryGuarantee;
        this.genericGuarantee = genericGuarantee;
    }

    public String getId() {
        return this.userId;
    }

    public TreeSet<TopologyDetails> getRunningTopologies(ISchedulingState cluster) {
        TreeSet<TopologyDetails> ret =
            new TreeSet<>(new PQsortByPriorityAndSubmittionTime());
        for (TopologyDetails td : cluster.getTopologies().getTopologiesOwnedBy(userId)) {
            if (!cluster.needsSchedulingRas(td)) {
                ret.add(td);
            }
        }
        return ret;
    }

    public TreeSet<TopologyDetails> getPendingTopologies(ISchedulingState cluster) {
        TreeSet<TopologyDetails> ret =
            new TreeSet<>(new PQsortByPriorityAndSubmittionTime());
        for (TopologyDetails td : cluster.getTopologies().getTopologiesOwnedBy(userId)) {
            if (cluster.needsSchedulingRas(td) && !unsuccess.contains(td)) {
                ret.add(td);
            }
        }
        return ret;
    }

    public void markTopoUnsuccess(TopologyDetails topo, Cluster cluster, String msg) {
        unsuccess.add(topo);
        if (cluster != null) {
            if (msg == null) {
                msg = "Scheduling Attempted but topology is invalid";
            }
            cluster.setStatus(topo.getId(), msg);
        }
    }

    public void markTopoUnsuccess(TopologyDetails topo) {
        this.markTopoUnsuccess(topo, null, null);
    }

    public double getResourcePoolAverageUtilization(ISchedulingState cluster) {
        double cpuResourcePoolUtilization = getCpuResourcePoolUtilization(cluster);
        double memoryResourcePoolUtilization = getMemoryResourcePoolUtilization(cluster);

        //cannot be (cpuResourcePoolUtilization + memoryResourcePoolUtilization)/2
        //since memoryResourcePoolUtilization or cpuResourcePoolUtilization can be Double.MAX_VALUE
        //Should not return infinity in that case
        return ((cpuResourcePoolUtilization) / 2.0) + ((memoryResourcePoolUtilization) / 2.0);
    }

    public double getCpuResourcePoolUtilization(ISchedulingState cluster) {
        if (cpuGuarantee == 0.0) {
            return Double.MAX_VALUE;
        }
        return getCpuResourceUsedByUser(cluster) / cpuGuarantee;
    }

    public double getMemoryResourcePoolUtilization(ISchedulingState cluster) {
        if (memoryGuarantee == 0.0) {
            return Double.MAX_VALUE;
        }
        return getMemoryResourceUsedByUser(cluster) / memoryGuarantee;
    }

    public double getMemoryResourceRequest(ISchedulingState cluster) {
        double sum = 0.0;
        Set<TopologyDetails> topologyDetailsSet = new HashSet<>(cluster.getTopologies().getTopologiesOwnedBy(userId));
        for (TopologyDetails topo : topologyDetailsSet) {
            sum += topo.getTotalRequestedMemOnHeap() + topo.getTotalRequestedMemOffHeap();
        }
        return sum;
    }

    public double getCpuResourceRequest(ISchedulingState cluster) {
        double sum = 0.0;
        Set<TopologyDetails> topologyDetailsSet = new HashSet<>(cluster.getTopologies().getTopologiesOwnedBy(userId));
        for (TopologyDetails topo : topologyDetailsSet) {
            sum += topo.getTotalRequestedCpu();
        }
        return sum;
    }

    public double getCpuResourceUsedByUser(ISchedulingState cluster) {
        double sum = 0.0;
        for (TopologyDetails td : cluster.getTopologies().getTopologiesOwnedBy(userId)) {
            SchedulerAssignment assignment = cluster.getAssignmentById(td.getId());
            if (assignment != null) {
                TopologyResources tr = new TopologyResources(td, assignment);
                sum += tr.getAssignedCpu();
            }
        }
        return sum;
    }

    public double getMemoryResourceUsedByUser(ISchedulingState cluster) {
        double sum = 0.0;
        for (TopologyDetails td : cluster.getTopologies().getTopologiesOwnedBy(userId)) {
            SchedulerAssignment assignment = cluster.getAssignmentById(td.getId());
            if (assignment != null) {
                TopologyResources tr = new TopologyResources(td, assignment);
                sum += tr.getAssignedMemOnHeap() + tr.getAssignedMemOffHeap();
            }
        }
        return sum;
    }

    public double getMemoryResourceGuaranteed() {
        return memoryGuarantee;
    }

    public double getCpuResourceGuaranteed() {
        return cpuGuarantee;
    }

    public Map<String, Double> getGenericGuaranteed() {
        return genericGuarantee;
    }

    public TopologyDetails getNextTopologyToSchedule(ISchedulingState cluster) {
        for (TopologyDetails topo : getPendingTopologies(cluster)) {
            return topo;
        }
        return null;
    }

    public boolean hasTopologyNeedSchedule(ISchedulingState cluster) {
        return (!this.getPendingTopologies(cluster).isEmpty());
    }

    public TopologyDetails getRunningTopologyWithLowestPriority(ISchedulingState cluster) {
        TreeSet<TopologyDetails> queue = getRunningTopologies(cluster);
        if (queue.isEmpty()) {
            return null;
        }
        return queue.last();
    }

    private static Map<String, Double> extractGenericResourceEntries(Map<String, Double> resourcePool) {
        Map<String, Double> ret = new HashMap<>();
        for (Map.Entry<String, Double> entry : resourcePool.entrySet()) {
            String key = entry.getKey();
            Double value = entry.getValue();
            if (key != null && !key.equals("cpu") && !key.equals("memory")) {
                ret.put(key, value);
            }
        }
        return ret;
    }

    @Override
    public int hashCode() {
        return this.userId.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof User)) {
            return false;
        }
        return this.getId().equals(((User) other).getId());
    }

    @Override
    public String toString() {
        return this.userId;
    }

    /**
     * Comparator that sorts topologies by priority and then by submission time First sort by Topology Priority, if there is a tie for
     * topology priority, topology uptime is used to sort.
     */
    static class PQsortByPriorityAndSubmittionTime implements Comparator<TopologyDetails> {

        @Override
        public int compare(TopologyDetails topo1, TopologyDetails topo2) {
            if (topo1.getTopologyPriority() > topo2.getTopologyPriority()) {
                return 1;
            } else if (topo1.getTopologyPriority() < topo2.getTopologyPriority()) {
                return -1;
            } else {
                if (topo1.getUpTime() > topo2.getUpTime()) {
                    return -1;
                } else if (topo1.getUpTime() < topo2.getUpTime()) {
                    return 1;
                } else {
                    return topo1.getId().compareTo(topo2.getId());
                }
            }
        }
    }
}
