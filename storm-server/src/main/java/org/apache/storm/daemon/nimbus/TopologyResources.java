/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.daemon.nimbus;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceRequest;

public final class TopologyResources {
    private final double requestedMemOnHeap;
    private final double requestedMemOffHeap;
    private final double requestedSharedMemOnHeap;
    private final double requestedSharedMemOffHeap;
    private final double requestedNonSharedMemOnHeap;
    private final double requestedNonSharedMemOffHeap;
    private final double requestedCpu;
    private Map<String, Double> requestedGenericResources;
    private double assignedMemOnHeap;
    private double assignedMemOffHeap;
    private double assignedSharedMemOnHeap;
    private double assignedSharedMemOffHeap;
    private double assignedNonSharedMemOnHeap;
    private double assignedNonSharedMemOffHeap;
    private double assignedCpu;
    private Map<String, Double> assignedGenericResources;

    private TopologyResources(TopologyDetails td, Collection<WorkerResources> workers,
                              Map<String, Double> nodeIdToSharedOffHeapNode) {
        requestedMemOnHeap = td.getTotalRequestedMemOnHeap();
        requestedMemOffHeap = td.getTotalRequestedMemOffHeap();
        requestedSharedMemOnHeap = td.getRequestedSharedOnHeap();
        requestedSharedMemOffHeap = td.getRequestedSharedOffHeap();
        requestedNonSharedMemOnHeap = td.getRequestedNonSharedOnHeap();
        requestedNonSharedMemOffHeap = td.getRequestedNonSharedOffHeap();
        requestedCpu = td.getTotalRequestedCpu();
        requestedGenericResources = td.getTotalRequestedGenericResources();
        assignedMemOnHeap = 0.0;
        assignedMemOffHeap = 0.0;
        assignedSharedMemOnHeap = 0.0;
        assignedSharedMemOffHeap = 0.0;
        assignedNonSharedMemOnHeap = 0.0;
        assignedNonSharedMemOffHeap = 0.0;
        assignedCpu = 0.0;
        assignedGenericResources = new HashMap<>();

        if (workers != null) {
            for (WorkerResources resources : workers) {
                assignedMemOnHeap += resources.get_mem_on_heap();
                assignedMemOffHeap += resources.get_mem_off_heap();
                assignedCpu += resources.get_cpu();
                assignedNonSharedMemOnHeap += resources.get_mem_on_heap();
                assignedNonSharedMemOffHeap += resources.get_mem_off_heap();

                if (resources.is_set_mem_on_heap()) {
                    assignedNonSharedMemOnHeap -= resources.get_shared_mem_on_heap();
                    assignedSharedMemOnHeap += resources.get_shared_mem_on_heap();
                }

                if (resources.is_set_shared_mem_off_heap()) {
                    assignedSharedMemOffHeap += resources.get_shared_mem_off_heap();
                    assignedNonSharedMemOffHeap -= resources.get_shared_mem_off_heap();
                }
            }
            assignedGenericResources = computeAssignedGenericResources(workers);
        }

        if (nodeIdToSharedOffHeapNode != null) {
            double sharedOff = nodeIdToSharedOffHeapNode.values().stream().reduce(0.0, (sum, val) -> sum + val);
            assignedSharedMemOffHeap += sharedOff;
            assignedMemOffHeap += sharedOff;
        }
    }

    private Map<String, Double> computeAssignedGenericResources(Collection<WorkerResources> workers) {
        Map<String, Double> genericResources = new HashMap<>();
        for (WorkerResources worker : workers) {
            genericResources = NormalizedResourceRequest.addResourceMap(genericResources, worker.get_resources());
        }
        NormalizedResourceRequest.removeNonGenericResources(genericResources);
        return genericResources;
    }

    public TopologyResources(TopologyDetails td, SchedulerAssignment assignment) {
        this(td, getWorkerResources(assignment), getNodeIdToSharedOffHeapNode(assignment));
    }

    public TopologyResources(TopologyDetails td, Assignment assignment) {
        this(td, getWorkerResources(assignment), getNodeIdToSharedOffHeapNode(assignment));
    }

    public TopologyResources() {
        this(0, 0, 0, 0, 0, 0, 0, new HashMap<>(), 0, 0, 0, 0, 0, 0, 0, new HashMap<>());
    }

    protected TopologyResources(
        double requestedMemOnHeap,
        double requestedMemOffHeap,
        double requestedSharedMemOnHeap,
        double requestedSharedMemOffHeap,
        double requestedNonSharedMemOnHeap,
        double requestedNonSharedMemOffHeap,
        double requestedCpu,
        Map<String, Double> requestedGenericResources,
        double assignedMemOnHeap,
        double assignedMemOffHeap,
        double assignedSharedMemOnHeap,
        double assignedSharedMemOffHeap,
        double assignedNonSharedMemOnHeap,
        double assignedNonSharedMemOffHeap,
        double assignedCpu,
        Map<String, Double> assignedGenericResources) {
        this.requestedMemOnHeap = requestedMemOnHeap;
        this.requestedMemOffHeap = requestedMemOffHeap;
        this.requestedSharedMemOnHeap = requestedSharedMemOnHeap;
        this.requestedSharedMemOffHeap = requestedSharedMemOffHeap;
        this.requestedNonSharedMemOnHeap = requestedNonSharedMemOnHeap;
        this.requestedNonSharedMemOffHeap = requestedNonSharedMemOffHeap;
        this.requestedCpu = requestedCpu;
        this.requestedGenericResources = requestedGenericResources;
        this.assignedMemOnHeap = assignedMemOnHeap;
        this.assignedMemOffHeap = assignedMemOffHeap;
        this.assignedSharedMemOnHeap = assignedSharedMemOnHeap;
        this.assignedSharedMemOffHeap = assignedSharedMemOffHeap;
        this.assignedNonSharedMemOnHeap = assignedNonSharedMemOnHeap;
        this.assignedNonSharedMemOffHeap = assignedNonSharedMemOffHeap;
        this.assignedCpu = assignedCpu;
        this.assignedGenericResources = assignedGenericResources;
    }

    private static Collection<WorkerResources> getWorkerResources(SchedulerAssignment assignment) {
        Collection<WorkerResources> ret = null;
        if (assignment != null) {
            Map<WorkerSlot, WorkerResources> allResources = assignment.getScheduledResources();
            if (allResources != null) {
                ret = allResources.values();
            }
        }
        return ret;
    }

    private static Collection<WorkerResources> getWorkerResources(Assignment assignment) {
        Collection<WorkerResources> ret = null;
        if (assignment != null) {
            Map<NodeInfo, WorkerResources> allResources = assignment.get_worker_resources();
            if (allResources != null) {
                ret = allResources.values();
            }
        }
        return ret;
    }

    private static Map<String, Double> getNodeIdToSharedOffHeapNode(SchedulerAssignment assignment) {
        Map<String, Double> ret = null;
        if (assignment != null) {
            ret = assignment.getNodeIdToTotalSharedOffHeapNodeMemory();
        }
        return ret;
    }

    private static Map<String, Double> getNodeIdToSharedOffHeapNode(Assignment assignment) {
        Map<String, Double> ret = null;
        if (assignment != null) {
            ret = assignment.get_total_shared_off_heap();
        }
        return ret;
    }

    public double getRequestedMemOnHeap() {
        return requestedMemOnHeap;
    }

    public double getRequestedMemOffHeap() {
        return requestedMemOffHeap;
    }

    public double getRequestedCpu() {
        return requestedCpu;
    }

    public double getAssignedMemOnHeap() {
        return assignedMemOnHeap;
    }

    public void setAssignedMemOnHeap(double assignedMemOnHeap) {
        this.assignedMemOnHeap = assignedMemOnHeap;
    }

    public double getAssignedMemOffHeap() {
        return assignedMemOffHeap;
    }

    public void setAssignedMemOffHeap(double assignedMemOffHeap) {
        this.assignedMemOffHeap = assignedMemOffHeap;
    }

    public double getAssignedCpu() {
        return assignedCpu;
    }

    public void setAssignedCpu(double assignedCpu) {
        this.assignedCpu = assignedCpu;
    }

    public double getAssignedSharedMemOnHeap() {
        return assignedSharedMemOnHeap;
    }

    public void setAssignedSharedMemOnHeap(double assignedSharedMemOnHeap) {
        this.assignedSharedMemOnHeap = assignedSharedMemOnHeap;
    }

    public double getRequestedSharedMemOnHeap() {
        return requestedSharedMemOnHeap;
    }

    public double getRequestedSharedMemOffHeap() {
        return requestedSharedMemOffHeap;
    }

    public double getRequestedNonSharedMemOnHeap() {
        return requestedNonSharedMemOnHeap;
    }

    public double getRequestedNonSharedMemOffHeap() {
        return requestedNonSharedMemOffHeap;
    }

    public double getAssignedSharedMemOffHeap() {
        return assignedSharedMemOffHeap;
    }

    public void setAssignedSharedMemOffHeap(double assignedSharedMemOffHeap) {
        this.assignedSharedMemOffHeap = assignedSharedMemOffHeap;
    }

    public double getAssignedNonSharedMemOnHeap() {
        return assignedNonSharedMemOnHeap;
    }

    public void setAssignedNonSharedMemOnHeap(double assignedNonSharedMemOnHeap) {
        this.assignedNonSharedMemOnHeap = assignedNonSharedMemOnHeap;
    }

    public double getAssignedNonSharedMemOffHeap() {
        return assignedNonSharedMemOffHeap;
    }

    public void setAssignedNonSharedMemOffHeap(double assignedNonSharedMemOffHeap) {
        this.assignedNonSharedMemOffHeap = assignedNonSharedMemOffHeap;
    }

    public Map<String, Double> getAssignedGenericResources() {
        return new HashMap<>(assignedGenericResources);
    }

    public Map<String, Double> getRequestedGenericResources() {
        return new HashMap<>(requestedGenericResources);
    }

    /**
     * Add the values in other to this and return a combined resources object.
     * @param other the other resources to add to this
     * @return the combined resources with the sum of the values in each.
     */
    public TopologyResources add(TopologyResources other) {
        return new TopologyResources(
            requestedMemOnHeap + other.requestedMemOnHeap,
            requestedMemOffHeap + other.requestedMemOffHeap,
            requestedSharedMemOnHeap + other.requestedSharedMemOnHeap,
            requestedSharedMemOffHeap + other.requestedSharedMemOffHeap,
            requestedNonSharedMemOnHeap + other.requestedNonSharedMemOnHeap,
            requestedNonSharedMemOffHeap + other.requestedNonSharedMemOffHeap,
            requestedCpu + other.requestedCpu,
            NormalizedResourceRequest.addResourceMap(requestedGenericResources, other.requestedGenericResources),
            assignedMemOnHeap + other.assignedMemOnHeap,
            assignedMemOffHeap + other.assignedMemOffHeap,
            assignedSharedMemOnHeap + other.assignedSharedMemOnHeap,
            assignedSharedMemOffHeap + other.assignedSharedMemOffHeap,
            assignedNonSharedMemOnHeap + other.assignedNonSharedMemOnHeap,
            assignedNonSharedMemOffHeap + other.assignedNonSharedMemOffHeap,
            assignedCpu + other.assignedCpu,
            NormalizedResourceRequest.addResourceMap(assignedGenericResources, other.assignedGenericResources));
    }
}
