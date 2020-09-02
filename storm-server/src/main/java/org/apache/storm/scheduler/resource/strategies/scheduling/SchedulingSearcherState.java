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

package org.apache.storm.scheduler.resource.strategies.scheduling;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.RasNode;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.SchedulingStatus;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulingSearcherState {
    private static final Logger LOG = LoggerFactory.getLogger(SchedulingSearcherState.class);

    final long startTimeMillis;
    private final long maxEndTimeMs;
    // A map of the worker to the components in the worker to be able to enforce constraints.
    private final Map<WorkerSlot, Map<String, Integer>> workerCompAssignmentCnts;
    private final boolean[] okToRemoveFromWorker;
    // for the currently tested assignment a Map of the node to the components on it to be able to enforce constraints
    private final Map<RasNode, Map<String, Integer>> nodeCompAssignmentCnts;
    private final boolean[] okToRemoveFromNode;
    // Static State
    // The list of all executors (preferably sorted to make assignments simpler).
    private List<ExecutorDetails> execs;
    //The maximum number of state to search before stopping.
    private final int maxStatesSearched;
    //The topology we are scheduling
    private final TopologyDetails td;
    private final String topoName;
    // Metrics
    // How many states searched so far.
    private int statesSearched = 0;
    // Number of times we had to backtrack.
    private int numBacktrack = 0;
    // Current state
    // The current executor we are trying to schedule
    private int execIndex = 0;
    private final Map<ExecutorDetails, String> execToComp;

    public SchedulingSearcherState(Map<WorkerSlot, Map<String, Integer>> workerCompAssignmentCnts,
                                    Map<RasNode, Map<String, Integer>> nodeCompAssignmentCnts, int maxStatesSearched, long maxTimeMs,
                                    List<ExecutorDetails> execs, TopologyDetails td, Map<ExecutorDetails, String> execToComp) {
        assert execs != null;

        this.workerCompAssignmentCnts = workerCompAssignmentCnts;
        this.nodeCompAssignmentCnts = nodeCompAssignmentCnts;
        this.maxStatesSearched = maxStatesSearched;
        this.execs = execs;
        okToRemoveFromWorker = new boolean[execs.size()];
        okToRemoveFromNode = new boolean[execs.size()];
        this.td = td;
        this.topoName = td.getName();
        startTimeMillis = Time.currentTimeMillis();
        if (maxTimeMs <= 0) {
            maxEndTimeMs = Long.MAX_VALUE;
        } else {
            maxEndTimeMs = startTimeMillis + maxTimeMs;
        }
        this.execToComp = execToComp;
    }

    /**
     * Reassign the list of executors as long as it contains the same executors as before.
     * Executors are normally assigned when this class is instantiated. However, this
     * list may be resorted externally and then reassigned.
     *
     * @param sortedExecs new list to be assigned.
     */
    public void setSortedExecs(List<ExecutorDetails> sortedExecs) {
        if (execs == null || new HashSet<>(execs).equals(new HashSet<>(sortedExecs))) {
            this.execs = sortedExecs;
        } else {
            String err = String.format("executors in sorted list (cnt=%d) are different from initial assignment (cnt=%d), topo=%s)",
                    sortedExecs.size(), execs.size(), topoName);
            throw new IllegalArgumentException(err);
        }
    }

    public void incStatesSearched() {
        statesSearched++;
        if (statesSearched % 1_000 == 0) {
            LOG.debug("Topology {} States Searched: {}", topoName, statesSearched);
            LOG.debug("Topology {} backtrack: {}", topoName, numBacktrack);
        }
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public int getStatesSearched() {
        return statesSearched;
    }

    public int getExecSize() {
        return execs.size();
    }

    public int getNumBacktrack() {
        return numBacktrack;
    }

    public int getExecIndex() {
        return execIndex;
    }

    public boolean areSearchLimitsExceeded() {
        return statesSearched > maxStatesSearched || Time.currentTimeMillis() > maxEndTimeMs;
    }

    public SchedulingSearcherState nextExecutor() {
        execIndex++;
        if (execIndex >= execs.size()) {
            String err = String.format("Internal Error: topology %s: execIndex exceeded limit %d >= %d", topoName, execIndex, execs.size());
            throw new IllegalStateException(err);
        }
        return this;
    }

    public boolean areAllExecsScheduled() {
        return execIndex == execs.size() - 1;
    }

    public ExecutorDetails currentExec() {
        return execs.get(execIndex);
    }

    /**
      * Assign executor to worker and node.
      * Assignment validity check is done before calling this method.
      *
      * @param execToComp Mapping from executor to component name.
      * @param node RasNode on which to schedule.
      * @param workerSlot WorkerSlot on which to schedule.
      */
    public void assignCurrentExecutor(Map<ExecutorDetails, String> execToComp, RasNode node, WorkerSlot workerSlot) {
        ExecutorDetails exec = currentExec();
        String comp = execToComp.get(exec);
        LOG.trace("Topology {} Trying assignment of {} {} to {}", topoName, exec, comp, workerSlot);
        // It is possible that this component is already scheduled on this node or worker.  If so when we backtrack we cannot remove it
        Map<String, Integer> oneMap = workerCompAssignmentCnts.computeIfAbsent(workerSlot, (k) -> new HashMap<>());
        oneMap.put(comp, oneMap.getOrDefault(comp, 0) + 1); // increment assignment count
        okToRemoveFromWorker[execIndex] = true;
        oneMap = nodeCompAssignmentCnts.computeIfAbsent(node, (k) -> new HashMap<>());
        oneMap.put(comp, oneMap.getOrDefault(comp, 0) + 1); // increment assignment count
        okToRemoveFromNode[execIndex] = true;
        node.assignSingleExecutor(workerSlot, exec, td);
    }

    public void backtrack(Map<ExecutorDetails, String> execToComp, RasNode node, WorkerSlot workerSlot) {
        execIndex--;
        if (execIndex < 0) {
            throw new IllegalStateException("Internal Error: Topology " + topoName + " exec index became negative");
        }
        numBacktrack++;
        ExecutorDetails exec = currentExec();
        String comp = execToComp.get(exec);
        LOG.trace("Topology {} Backtracking {} {} from {}", topoName, exec, comp, workerSlot);
        if (okToRemoveFromWorker[execIndex]) {
            Map<String, Integer> oneMap = workerCompAssignmentCnts.get(workerSlot);
            oneMap.put(comp, oneMap.getOrDefault(comp, 0) - 1); // decrement assignment count
            okToRemoveFromWorker[execIndex] = false;
        }
        if (okToRemoveFromNode[execIndex]) {
            Map<String, Integer> oneMap = nodeCompAssignmentCnts.get(node);
            oneMap.put(comp, oneMap.getOrDefault(comp, 0) - 1); // decrement assignment count
            okToRemoveFromNode[execIndex] = false;
        }
        node.freeSingleExecutor(exec, td);
    }

    /**
     * Use this method to log the current component assignments on the Node.
     * Useful for debugging and tests.
     */
    public void logNodeCompAssignments() {
        if (nodeCompAssignmentCnts == null || nodeCompAssignmentCnts.isEmpty()) {
            LOG.info("Topology {} NodeCompAssignment is empty", topoName);
            return;
        }
        StringBuffer sb = new StringBuffer();
        int cntAllNodes = 0;
        int cntFilledNodes = 0;
        for (RasNode node: new TreeSet<>(nodeCompAssignmentCnts.keySet())) {
            cntAllNodes++;
            Map<String, Integer> oneMap = nodeCompAssignmentCnts.get(node);
            if (oneMap.isEmpty()) {
                continue;
            }
            cntFilledNodes++;
            String oneMapJoined = oneMap.entrySet()
                    .stream().map(e -> String.format("%s: %s", e.getKey(), e.getValue()))
                    .collect(Collectors.joining(","));
            sb.append(String.format("\n\t(%d) Node %s: %s", cntFilledNodes, node.getId(), oneMapJoined));
        }
        LOG.info("Topology {} NodeCompAssignments available for {} of {} nodes {}", topoName, cntFilledNodes, cntAllNodes, sb);
        LOG.info("Topology {} Executors assignments attempted (cnt={}) are: \n\t{}",
                topoName, execs.size(), execs.stream().map(ExecutorDetails::toString).collect(Collectors.joining(","))
        );
    }

    /**
     * Get a map of component to count for the specified worker slot.
     *
     * @param workerSlot to check for.
     * @return assignment map of count for components, may be a null.
     */
    public Map<String, Integer> getCompAssignmentCntMapForWorker(WorkerSlot workerSlot) {
        return workerCompAssignmentCnts.get(workerSlot);
    }

    public int getComponentCntOnNode(RasNode rasNode, String comp) {
        Map<String, Integer> map = nodeCompAssignmentCnts.get(rasNode);
        if (map == null) {
            return 0;
        }
        return map.getOrDefault(comp, 0);
    }

    public SchedulingResult createSchedulingResult(boolean success, String schedulerClassSimpleName) {
        String msg;
        if (success) {
            msg = String.format("Fully Scheduled by %s (%d states traversed in %d ms, backtracked %d times)",
                    schedulerClassSimpleName, this.getStatesSearched(),
                    Time.currentTimeMillis() - this.getStartTimeMillis(), this.getNumBacktrack());
            return SchedulingResult.success(msg);
        } else {
            msg = String.format("Cannot schedule by %s (%d states traversed in %d ms, backtracked %d times, %d of %d executors scheduled)",
                    schedulerClassSimpleName, this.getStatesSearched(),
                    Time.currentTimeMillis() - this.getStartTimeMillis(), this.getNumBacktrack(),
                    this.getExecIndex(), this.getExecSize());
            this.logNodeCompAssignments();
            return SchedulingResult.failure(SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES, msg);
        }
    }

    /**
     * Check if the current executor has a different component from the previous one.
     * This flag can be used as a quick way to check if the nodes should be sorted.
     *
     * @return true if first executor or if the component is same as previous executor. False other wise.
     */
    public boolean isExecCompDifferentFromPrior() {
        if (execIndex == 0) {
            return true;
        }
        // did the component change from prior executor
        return execToComp.getOrDefault(execs.get(execIndex), "")
                .equals(execToComp.getOrDefault(execs.get(execIndex - 1), ""));
    }
}
