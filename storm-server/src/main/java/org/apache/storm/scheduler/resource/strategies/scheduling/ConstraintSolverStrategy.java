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

package org.apache.storm.scheduler.resource.strategies.scheduling;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.RasNode;
import org.apache.storm.scheduler.resource.RasNodes;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.SchedulingStatus;
import org.apache.storm.scheduler.resource.strategies.scheduling.sorter.ExecSorterByConstraintSeverity;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConstraintSolverStrategy extends BaseResourceAwareStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ConstraintSolverStrategy.class);

    /**
     * Instance variables initialized in first step {@link #prepareForScheduling(Cluster, TopologyDetails)} of
     * schedule method {@link #schedule(Cluster, TopologyDetails)}.
     */
    private ConstraintSolverConfig constraintSolverConfig;

    @Override
    protected void prepareForScheduling(Cluster cluster, TopologyDetails topologyDetails) {
        super.prepareForScheduling(cluster, topologyDetails);

        // populate additional instance variables
        constraintSolverConfig = new ConstraintSolverConfig(topologyDetails);
        setExecSorter(new ExecSorterByConstraintSeverity(cluster, topologyDetails));
    }

    @Override
    protected SchedulingResult checkSchedulingFeasibility() {
        SchedulingResult res = super.checkSchedulingFeasibility();
        if (res != null) {
            return res;
        }
        if (!isSchedulingFeasible()) {
            return SchedulingResult.failure(SchedulingStatus.FAIL_OTHER, "Scheduling not feasible!");
        }
        return null;
    }

    /**
     * Check if any constraints are violated if exec is scheduled on worker.
     * @return true if scheduling exec on worker does not violate any constraints, returns false if it does
     */
    @Override
    protected boolean isExecAssignmentToWorkerValid(ExecutorDetails exec, WorkerSlot worker) {
        if (!super.isExecAssignmentToWorkerValid(exec, worker)) {
            return false;
        }
        // check if executor can be on worker based on component exclusions
        String execComp = execToComp.get(exec);
        Map<String, Integer> compAssignmentCnts = searcherState.getCompAssignmentCntMapForWorker(worker);
        Set<String> incompatibleComponents;
        if (compAssignmentCnts != null
                && (incompatibleComponents = constraintSolverConfig.getIncompatibleComponentSets().get(execComp)) != null
                && !incompatibleComponents.isEmpty()) {
            for (String otherComp : compAssignmentCnts.keySet()) {
                if (incompatibleComponents.contains(otherComp)) {
                    LOG.debug("Topology {}, exec={} with comp={} has constraint violation with comp={} on worker={}",
                            topoName, exec, execComp, otherComp, worker);
                    return false;
                }
            }
        }

        // check if executor can be on worker based on component node co-location constraint
        Map<String, Integer> maxNodeCoLocationCnts = constraintSolverConfig.getMaxNodeCoLocationCnts();
        if (maxNodeCoLocationCnts.containsKey(execComp)) {
            int coLocationMaxCnt = maxNodeCoLocationCnts.get(execComp);
            RasNode node = nodes.getNodeById(worker.getNodeId());
            int compCntOnNode = searcherState.getComponentCntOnNode(node, execComp);
            if (compCntOnNode >= coLocationMaxCnt) {
                LOG.debug("Topology {}, exec={} with comp={} has MaxCoLocationCnt violation on node {}, count {} >= colocation count {}",
                        topoName, exec, execComp, node.getId(), compCntOnNode, coLocationMaxCnt);
                return false;
            }
        }
        return true;
    }

    /**
     * Determines if a scheduling is valid and all constraints are satisfied (for use in testing).
     * This is done in three steps.
     *
     * <li>Check if nodeCoLocationCnt-constraints are satisfied. Some components may allow only a certain number of
     * executors to exist on the same node {@link ConstraintSolverConfig#getMaxNodeCoLocationCnts()}.
     * </li>
     *
     * <li>
     * Check if incompatibility-constraints are satisfied. Incompatible components
     * {@link ConstraintSolverConfig#getIncompatibleComponentSets()} should not be put on the same worker.
     * </li>
     *
     * <li>
     * Check if CPU and Memory resources do not exceed availability on the node and total matches what is expected
     * when fully scheduled.
     * </li>
     *
     * @param cluster on which scheduling was done.
     * @param topo TopologyDetails being scheduled.
     * @return true if solution is valid, false otherwise.
     */
    @VisibleForTesting
    public static boolean validateSolution(Cluster cluster, TopologyDetails topo) {
        assert (cluster.getAssignmentById(topo.getId()) != null);
        LOG.debug("Checking for a valid scheduling for topology {}...", topo.getName());

        ConstraintSolverConfig constraintSolverConfig = new ConstraintSolverConfig(topo);

        // First check NodeCoLocationCnt constraints
        Map<ExecutorDetails, String> execToComp = topo.getExecutorToComponent();
        Map<String, Map<String, Integer>> nodeCompMap = new HashMap<>(); // this is the critical count
        Map<WorkerSlot, RasNode> workerToNodes = new HashMap<>();
        RasNodes.getAllNodesFrom(cluster)
                .values()
                .forEach(node -> node.getUsedSlots().forEach(workerSlot -> workerToNodes.put(workerSlot, node)));

        List<String> errors = new ArrayList<>();

        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : cluster.getAssignmentById(topo.getId()).getExecutorToSlot().entrySet()) {
            ExecutorDetails exec = entry.getKey();
            String comp = execToComp.get(exec);
            WorkerSlot worker = entry.getValue();
            RasNode node = workerToNodes.get(worker);
            String nodeId = node.getId();

            if (!constraintSolverConfig.getMaxNodeCoLocationCnts().containsKey(comp)) {
                continue;
            }
            int allowedColocationMaxCnt = constraintSolverConfig.getMaxNodeCoLocationCnts().get(comp);
            Map<String, Integer> oneNodeCompMap = nodeCompMap.computeIfAbsent(nodeId, (k) -> new HashMap<>());
            oneNodeCompMap.put(comp, oneNodeCompMap.getOrDefault(comp, 0) + 1);
            if (allowedColocationMaxCnt < oneNodeCompMap.get(comp)) {
                String err = String.format("MaxNodeCoLocation: Component %s (exec=%s) on node %s, cnt %d > allowed %d",
                        comp, exec, nodeId, oneNodeCompMap.get(comp), allowedColocationMaxCnt);
                errors.add(err);
            }
        }

        // Second check IncompatibileComponent Constraints
        Map<WorkerSlot, Set<String>> workerCompMap = new HashMap<>();
        cluster.getAssignmentById(topo.getId()).getExecutorToSlot()
                .forEach((exec, worker) -> {
                    String comp = execToComp.get(exec);
                    workerCompMap.computeIfAbsent(worker, (k) -> new HashSet<>()).add(comp);
                });
        for (Map.Entry<WorkerSlot, Set<String>> entry : workerCompMap.entrySet()) {
            Set<String> comps = entry.getValue();
            for (String comp1 : comps) {
                for (String comp2 : comps) {
                    if (!comp1.equals(comp2)
                            && constraintSolverConfig.getIncompatibleComponentSets().containsKey(comp1)
                            && constraintSolverConfig.getIncompatibleComponentSets().get(comp1).contains(comp2)) {
                        String err = String.format("IncompatibleComponents: %s and %s on WorkerSlot: %s",
                                comp1, comp2, entry.getKey());
                        errors.add(err);
                    }
                }
            }
        }

        // Third check resources
        SchedulerAssignment schedulerAssignment = cluster.getAssignmentById(topo.getId());
        Map<ExecutorDetails, WorkerSlot> execToWorker = new HashMap<>();
        if (schedulerAssignment.getExecutorToSlot() != null) {
            execToWorker.putAll(schedulerAssignment.getExecutorToSlot());
        }

        Map<String, RasNode> nodes = RasNodes.getAllNodesFrom(cluster);
        Map<RasNode, Collection<ExecutorDetails>> nodeToExecs = new HashMap<>();
        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : execToWorker.entrySet()) {
            ExecutorDetails exec = entry.getKey();
            WorkerSlot worker = entry.getValue();
            RasNode node = nodes.get(worker.getNodeId());

            if (node.getAvailableMemoryResources() < 0.0) {
                String err = String.format("Resource Exhausted: Found node %s with negative available memory %,.2f",
                        node.getId(), node.getAvailableMemoryResources());
                errors.add(err);
                continue;
            }
            if (node.getAvailableCpuResources() < 0.0) {
                String err = String.format("Resource Exhausted: Found node %s with negative available CPU %,.2f",
                        node.getId(), node.getAvailableCpuResources());
                errors.add(err);
                continue;
            }
            nodeToExecs.computeIfAbsent(node, (k) -> new HashSet<>()).add(exec);
        }

        for (Map.Entry<RasNode, Collection<ExecutorDetails>> entry : nodeToExecs.entrySet()) {
            RasNode node = entry.getKey();
            Collection<ExecutorDetails> execs = entry.getValue();
            double cpuUsed = 0.0;
            double memoryUsed = 0.0;
            for (ExecutorDetails exec : execs) {
                cpuUsed += topo.getTotalCpuReqTask(exec);
                memoryUsed += topo.getTotalMemReqTask(exec);
            }
            if (node.getAvailableCpuResources() != (node.getTotalCpuResources() - cpuUsed)) {
                String err = String.format("Incorrect CPU Resources: Node %s CPU available is %,.2f, expected %,.2f, "
                                + "Executors scheduled on node: %s",
                        node.getId(), node.getAvailableCpuResources(), (node.getTotalCpuResources() - cpuUsed), execs);
                errors.add(err);
            }
            if (node.getAvailableMemoryResources() != (node.getTotalMemoryResources() - memoryUsed)) {
                String err = String.format("Incorrect Memory Resources: Node %s Memory available is %,.2f, expected %,.2f, "
                                + "Executors scheduled on node: %s",
                        node.getId(), node.getAvailableMemoryResources(), (node.getTotalMemoryResources() - memoryUsed), execs);
                errors.add(err);
            }
        }

        if (!errors.isEmpty()) {
            LOG.error("Topology {} solution is invalid\n\t{}", topo.getName(), String.join("\n\t", errors));
        }
        return errors.isEmpty();
    }

    /**
     * A quick check to see if scheduling is feasible.
     *
     * @return False if scheduling is infeasible, true otherwise.
     */
    private boolean isSchedulingFeasible() {
        int nodeCnt = nodes.getNodes().size();
        for (Map.Entry<String, Integer> entry : constraintSolverConfig.getMaxNodeCoLocationCnts().entrySet()) {
            String comp = entry.getKey();
            int maxCoLocationCnt = entry.getValue();
            int numExecs = compToExecs.get(comp).size();
            if (numExecs > nodeCnt * maxCoLocationCnt) {
                LOG.error("Unsatisfiable constraint: Component: {} marked as spread has {} executors which is larger than "
                        + "number of nodes * maxCoLocationCnt: {} * {} ", comp, numExecs, nodeCnt, maxCoLocationCnt);
                return false;
            }
        }
        return true;
    }
}
