/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.scheduler.resource.strategies.scheduling;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.storm.Config;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.RAS_Node;
import org.apache.storm.scheduler.resource.RAS_Nodes;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.SchedulingStatus;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConstraintSolverStrategy extends BaseResourceAwareStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ConstraintSolverStrategy.class);

    protected static class SolverResult {
        private final int statesSearched;
        private final boolean success;
        private final long timeTakenMillis;
        private final int backtracked;

        public SolverResult(SearcherState state, boolean success) {
            this.statesSearched = state.getStatesSearched();
            this.success = success;
            timeTakenMillis = Time.currentTimeMillis() - state.startTimeMillis;
            backtracked = state.numBacktrack;
        }

        public SchedulingResult asSchedulingResult() {
            if (success) {
                return SchedulingResult.success("Fully Scheduled by ConstraintSolverStrategy (" + statesSearched
                    + " states traversed in " + timeTakenMillis + "ms, backtracked " + backtracked + " times)");
            }
            return SchedulingResult.failure(SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES,
                "Cannot find scheduling that satisfies all constraints (" + statesSearched
                    + " states traversed in " + timeTakenMillis + "ms, backtracked " + backtracked + " times)");
        }
    }

    protected static class SearcherState {
        // Metrics
        // How many states searched so far.
        private int statesSearched = 0;
        // Number of times we had to backtrack.
        private int numBacktrack = 0;
        final long startTimeMillis;
        private final long maxEndTimeMs;

        // Current state
        // The current executor we are trying to schedule
        private int execIndex = 0;
        // A map of the worker to the components in the worker to be able to enforce constraints.
        private final Map<WorkerSlot, Set<String>> workerCompAssignment;
        private final boolean[] okToRemoveFromWorker;
        // for the currently tested assignment a Map of the node to the components on it to be able to enforce constraints
        private final Map<RAS_Node, Set<String>> nodeCompAssignment;
        private final boolean[] okToRemoveFromNode;

        // Static State
        // The list of all executors (preferably sorted to make assignments simpler).
        private final List<ExecutorDetails> execs;
        //The maximum number of state to search before stopping.
        private final int maxStatesSearched;
        //The topology we are scheduling
        private final TopologyDetails td;

        private SearcherState(Map<WorkerSlot, Set<String>> workerCompAssignment, Map<RAS_Node, Set<String>> nodeCompAssignment,
                              int maxStatesSearched, long maxTimeMs, List<ExecutorDetails> execs, TopologyDetails td) {
            assert !execs.isEmpty();
            assert execs != null;

            this.workerCompAssignment = workerCompAssignment;
            this.nodeCompAssignment = nodeCompAssignment;
            this.maxStatesSearched = maxStatesSearched;
            this.execs = execs;
            okToRemoveFromWorker = new boolean[execs.size()];
            okToRemoveFromNode = new boolean[execs.size()];
            this.td = td;
            startTimeMillis = Time.currentTimeMillis();
            if (maxTimeMs <= 0) {
                maxEndTimeMs = Long.MAX_VALUE;
            } else {
                maxEndTimeMs = startTimeMillis + maxTimeMs;
            }
        }

        public void incStatesSearched() {
            statesSearched++;
            if (LOG.isDebugEnabled() && statesSearched % 1_000 == 0) {
                LOG.debug("States Searched: {}", statesSearched);
                LOG.debug("backtrack: {}", numBacktrack);
            }
        }

        public int getStatesSearched() {
            return statesSearched;
        }

        public boolean areSearchLimitsExceeded() {
            return statesSearched > maxStatesSearched || Time.currentTimeMillis() > maxEndTimeMs;
        }

        public SearcherState nextExecutor() {
            execIndex++;
            if (execIndex >= execs.size()) {
                throw new IllegalStateException("Internal Error: exceeded the exec limit " + execIndex + " >= " + execs.size());
            }
            return this;
        }

        public boolean areAllExecsScheduled() {
            return execIndex == execs.size() - 1;
        }

        public ExecutorDetails currentExec() {
            return execs.get(execIndex);
        }

        public void tryToSchedule(Map<ExecutorDetails, String> execToComp, RAS_Node node, WorkerSlot workerSlot) {
            ExecutorDetails exec = currentExec();
            String comp = execToComp.get(exec);
            LOG.trace("Trying assignment of {} {} to {}", exec, comp, workerSlot);
            //It is possible that this component is already scheduled on this node or worker.  If so when we backtrack we cannot remove it
            okToRemoveFromWorker[execIndex] = workerCompAssignment.computeIfAbsent(workerSlot, (k) -> new HashSet<>()).add(comp);
            okToRemoveFromNode[execIndex] = nodeCompAssignment.computeIfAbsent(node, (k) -> new HashSet<>()).add(comp);
            node.assignSingleExecutor(workerSlot, exec, td);
        }

        public void backtrack(Map<ExecutorDetails, String> execToComp, RAS_Node node, WorkerSlot workerSlot) {
            execIndex--;
            if (execIndex < 0) {
                throw new IllegalStateException("Internal Error: exec index became negative");
            }
            numBacktrack++;
            ExecutorDetails exec = currentExec();
            String comp = execToComp.get(exec);
            LOG.trace("Backtracking {} {} from {}", exec, comp, workerSlot);
            if (okToRemoveFromWorker[execIndex]) {
                workerCompAssignment.get(workerSlot).remove(comp);
                okToRemoveFromWorker[execIndex] = false;
            }
            if (okToRemoveFromNode[execIndex]) {
                nodeCompAssignment.get(node).remove(comp);
                okToRemoveFromNode[execIndex] = false;
            }
            node.freeSingleExecutor(exec, td);
        }
    }

    private Map<String, RAS_Node> nodes;
    private Map<ExecutorDetails, String> execToComp;
    private Map<String, Set<ExecutorDetails>> compToExecs;
    private List<String> favoredNodes;
    private List<String> unFavoredNodes;

    //constraints and spreads
    private Map<String, Map<String, Integer>> constraintMatrix;
    private HashSet<String> spreadComps = new HashSet<>();

    //hard coded max number of states to search
    public static final int MAX_STATE_SEARCH = 100_000;
    public static final int DEFAULT_STATE_SEARCH = 10_000;

    @Override
    public SchedulingResult schedule(Cluster cluster, TopologyDetails td) {
        prepare(cluster);
        LOG.debug("Scheduling {}", td.getId());
        nodes = RAS_Nodes.getAllNodesFrom(cluster);
        Map<WorkerSlot, Set<String>> workerCompAssignment = new HashMap<>();
        Map<RAS_Node, Set<String>> nodeCompAssignment = new HashMap<>();
        //set max number of states to search
        final int maxStateSearch = Math.min(MAX_STATE_SEARCH,
            ObjectReader.getInt(td.getConf().get(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH), DEFAULT_STATE_SEARCH));

        final long maxTimeMs =
            ObjectReader.getInt(td.getConf().get(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_TIME_SECS), -1).intValue() * 1000L;

        favoredNodes = (List<String>) td.getConf().get(Config.TOPOLOGY_SCHEDULER_FAVORED_NODES);
        unFavoredNodes = (List<String>) td.getConf().get(Config.TOPOLOGY_SCHEDULER_UNFAVORED_NODES);

        //get mapping of execs to components
        execToComp = td.getExecutorToComponent();
        //get mapping of components to executors
        compToExecs = getCompToExecs(execToComp);

        //get topology constraints
        constraintMatrix = getConstraintMap(td, compToExecs.keySet());

        //get spread components
        spreadComps = getSpreadComps(td);

        //get a sorted list of unassigned executors based on number of constraints
        Set<ExecutorDetails> unassignedExecutors = new HashSet<>(cluster.getUnassignedExecutors(td));
        List<ExecutorDetails> sortedExecs = getSortedExecs(spreadComps, constraintMatrix, compToExecs).stream()
            .filter(unassignedExecutors::contains)
            .collect(Collectors.toList());

        //populate with existing assignments
        SchedulerAssignment existingAssignment = cluster.getAssignmentById(td.getId());
        if (existingAssignment != null) {
            existingAssignment.getExecutorToSlot().forEach((exec, ws) -> {
                String compId = execToComp.get(exec);
                RAS_Node node = nodes.get(ws.getNodeId());
                //populate node to component Assignments
                nodeCompAssignment.computeIfAbsent(node, (k) -> new HashSet<>()).add(compId);
                //populate worker to comp assignments
                workerCompAssignment.computeIfAbsent(ws, (k) -> new HashSet<>()).add(compId);
            });
        }

        //early detection/early fail
        if (!checkSchedulingFeasibility()) {
            //Scheduling Status set to FAIL_OTHER so no eviction policy will be attempted to make space for this topology
            return SchedulingResult.failure(SchedulingStatus.FAIL_OTHER, "Scheduling not feasible!");
        }
        return backtrackSearch(new SearcherState(workerCompAssignment, nodeCompAssignment, maxStateSearch, maxTimeMs, sortedExecs, td))
            .asSchedulingResult();
    }

    private boolean checkSchedulingFeasibility() {
        for (String comp : spreadComps) {
            int numExecs = compToExecs.get(comp).size();
            if (numExecs > nodes.size()) {
                LOG.error("Unsatisfiable constraint: Component: {} marked as spread has {} executors which is larger "
                    + "than number of nodes: {}", comp, numExecs, nodes.size());
                return false;
            }
        }
        if (execToComp.size() >= MAX_STATE_SEARCH) {
            LOG.error("Number of executors is greater than the maximum number of states allowed to be searched.  "
                + "# of executors: {} Max states to search: {}", execToComp.size(), MAX_STATE_SEARCH);
            return false;
        }
        return true;
    }
    
    @Override
    protected TreeSet<ObjectResources> sortObjectResources(
        final AllResources allResources, ExecutorDetails exec, TopologyDetails topologyDetails,
        final ExistingScheduleFunc existingScheduleFunc) {
        return GenericResourceAwareStrategy.sortObjectResourcesImpl(allResources, exec, topologyDetails, existingScheduleFunc);
    }

    // Backtracking algorithm does not take into account the ordering of executors in worker to reduce traversal space
    @VisibleForTesting
    protected SolverResult backtrackSearch(SearcherState state) {
        state.incStatesSearched();
        if (state.areSearchLimitsExceeded()) {
            LOG.warn("Limits Exceeded");
            return new SolverResult(state, false);
        }

        ExecutorDetails exec = state.currentExec();
        List<ObjectResources> sortedNodes = sortAllNodes(state.td, exec, favoredNodes, unFavoredNodes);

        for (ObjectResources nodeResources: sortedNodes) {
            RAS_Node node = nodes.get(nodeResources.id);
            for (WorkerSlot workerSlot : node.getSlotsAvailbleTo(state.td)) {
                if (isExecAssignmentToWorkerValid(workerSlot, state)) {
                    state.tryToSchedule(execToComp, node, workerSlot);

                    if (state.areAllExecsScheduled()) {
                        //Everything is scheduled correctly, so no need to search any more.
                        return new SolverResult(state, true);
                    }

                    SolverResult results = backtrackSearch(state.nextExecutor());
                    if (results.success) {
                        //We found a good result we are done.
                        return results;
                    }

                    if (state.areSearchLimitsExceeded()) {
                        //No need to search more it is not going to help.
                        return new SolverResult(state, false);
                    }

                    //backtracking (If we ever get here there really isn't a lot of hope that we will find a scheduling)
                    state.backtrack(execToComp, node, workerSlot);
                }
            }
        }
        //Tried all of the slots and none of them worked.
        return new SolverResult(state, false);
    }

    /**
     * Check if any constraints are violated if exec is scheduled on worker.
     * @return true if scheduling exec on worker does not violate any constraints, returns false if it does
     */
    public boolean isExecAssignmentToWorkerValid(WorkerSlot worker, SearcherState state) {
        final ExecutorDetails exec = state.currentExec();
        //check resources
        RAS_Node node = nodes.get(worker.getNodeId());
        if (!node.wouldFit(worker, exec, state.td)) {
            LOG.trace("{} would not fit in resources available on {}", exec, worker);
            return false;
        }

        //check if exec can be on worker based on user defined component exclusions
        String execComp = execToComp.get(exec);
        Set<String> components = state.workerCompAssignment.get(worker);
        if (components != null) {
            Map<String, Integer> subMatrix = constraintMatrix.get(execComp);
            for (String comp : components) {
                if (subMatrix.get(comp) != 0) {
                    LOG.trace("{} found {} constraint violation {} on {}", exec, execComp, comp, worker);
                    return false;
                }
            }
        }

        //check if exec satisfy spread
        if (spreadComps.contains(execComp)) {
            if (state.nodeCompAssignment.computeIfAbsent(node, (k) -> new HashSet<>()).contains(execComp)) {
                LOG.trace("{} Found spread violation {} on node {}", exec, execComp, node.getId());
                return false;
            }
        }
        return true;
    }

    static Map<String, Map<String, Integer>> getConstraintMap(TopologyDetails topo, Set<String> comps) {
        Map<String, Map<String, Integer>> matrix = new HashMap<>();
        for (String comp : comps) {
            matrix.put(comp, new HashMap<>());
            for (String comp2 : comps) {
                matrix.get(comp).put(comp2, 0);
            }
        }
        List<List<String>> constraints = (List<List<String>>) topo.getConf().get(Config.TOPOLOGY_RAS_CONSTRAINTS);
        if (constraints != null) {
            for (List<String> constraintPair : constraints) {
                String comp1 = constraintPair.get(0);
                String comp2 = constraintPair.get(1);
                if (!matrix.containsKey(comp1)) {
                    LOG.warn("Comp: {} declared in constraints is not valid!", comp1);
                    continue;
                }
                if (!matrix.containsKey(comp2)) {
                    LOG.warn("Comp: {} declared in constraints is not valid!", comp2);
                    continue;
                }
                matrix.get(comp1).put(comp2, 1);
                matrix.get(comp2).put(comp1, 1);
            }
        }
        return matrix;
    }

    /**
     * Determines if a scheduling is valid and all constraints are satisfied.
     */
    @VisibleForTesting
    public static boolean validateSolution(Cluster cluster, TopologyDetails td) {
        return checkSpreadSchedulingValid(cluster, td)
            && checkConstraintsSatisfied(cluster, td)
            && checkResourcesCorrect(cluster, td);
    }

    /**
     * Check if constraints are satisfied.
     */
    private static boolean checkConstraintsSatisfied(Cluster cluster, TopologyDetails topo) {
        LOG.info("Checking constraints...");
        Map<ExecutorDetails, WorkerSlot> result = cluster.getAssignmentById(topo.getId()).getExecutorToSlot();
        Map<ExecutorDetails, String> execToComp = topo.getExecutorToComponent();
        //get topology constraints
        Map<String, Map<String, Integer>> constraintMatrix = getConstraintMap(topo, new HashSet<>(topo.getExecutorToComponent().values()));

        Map<WorkerSlot, Set<String>> workerCompMap = new HashMap<>();
        result.forEach((exec, worker) -> {
            String comp = execToComp.get(exec);
            workerCompMap.computeIfAbsent(worker, (k) -> new HashSet<>()).add(comp);
        });
        for (Map.Entry<WorkerSlot, Set<String>> entry : workerCompMap.entrySet()) {
            Set<String> comps = entry.getValue();
            for (String comp1 : comps) {
                for (String comp2: comps) {
                    if (!comp1.equals(comp2) && constraintMatrix.get(comp1).get(comp2) != 0) {
                        LOG.error("Incorrect Scheduling: worker exclusion for Component {} and {} not satisfied on WorkerSlot: {}",
                            comp1, comp2, entry.getKey());
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private static Map<WorkerSlot, RAS_Node> workerToNodes(Cluster cluster) {
        Map<WorkerSlot, RAS_Node> workerToNodes = new HashMap<>();
        for (RAS_Node node: RAS_Nodes.getAllNodesFrom(cluster).values()) {
            for (WorkerSlot s : node.getUsedSlots()) {
                workerToNodes.put(s, node);
            }
        }
        return workerToNodes;
    }

    private static boolean checkSpreadSchedulingValid(Cluster cluster, TopologyDetails topo) {
        LOG.info("Checking for a valid scheduling...");
        Map<ExecutorDetails, WorkerSlot> result = cluster.getAssignmentById(topo.getId()).getExecutorToSlot();
        Map<ExecutorDetails, String> execToComp = topo.getExecutorToComponent();
        Map<WorkerSlot, HashSet<ExecutorDetails>> workerExecMap = new HashMap<>();
        Map<WorkerSlot, HashSet<String>> workerCompMap = new HashMap<>();
        Map<RAS_Node, HashSet<String>> nodeCompMap = new HashMap<>();
        Map<WorkerSlot, RAS_Node> workerToNodes = workerToNodes(cluster);
        boolean ret = true;

        HashSet<String> spreadComps = getSpreadComps(topo);
        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : result.entrySet()) {
            ExecutorDetails exec = entry.getKey();
            WorkerSlot worker = entry.getValue();
            RAS_Node node = workerToNodes.get(worker);

            if (workerExecMap.computeIfAbsent(worker, (k) -> new HashSet<>()).contains(exec)) {
                LOG.error("Incorrect Scheduling: Found duplicate in scheduling");
                return false;
            }
            workerExecMap.get(worker).add(exec);
            String comp = execToComp.get(exec);
            workerCompMap.computeIfAbsent(worker, (k) -> new HashSet<>()).add(comp);
            if (spreadComps.contains(comp)) {
                if (nodeCompMap.computeIfAbsent(node, (k) -> new HashSet<>()).contains(comp)) {
                    LOG.error("Incorrect Scheduling: Spread for Component: {} {} on node {} not satisfied {}",
                        comp, exec, node.getId(), nodeCompMap.get(node));
                    ret = false;
                }
            }
            nodeCompMap.computeIfAbsent(node, (k) -> new HashSet<>()).add(comp);
        }
        return ret;
    }

    /**
     * Check if resource constraints satisfied.
     */
    private static boolean checkResourcesCorrect(Cluster cluster, TopologyDetails topo) {
        LOG.info("Checking Resources...");
        Map<ExecutorDetails, WorkerSlot> result = cluster.getAssignmentById(topo.getId()).getExecutorToSlot();
        Map<RAS_Node, Collection<ExecutorDetails>> nodeToExecs = new HashMap<>();
        Map<ExecutorDetails, WorkerSlot> mergedExecToWorker = new HashMap<>();
        Map<String, RAS_Node> nodes = RAS_Nodes.getAllNodesFrom(cluster);
        //merge with existing assignments
        if (cluster.getAssignmentById(topo.getId()) != null
                && cluster.getAssignmentById(topo.getId()).getExecutorToSlot() != null) {
            mergedExecToWorker.putAll(cluster.getAssignmentById(topo.getId()).getExecutorToSlot());
        }
        mergedExecToWorker.putAll(result);

        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : mergedExecToWorker.entrySet()) {
            ExecutorDetails exec = entry.getKey();
            WorkerSlot worker = entry.getValue();
            RAS_Node node = nodes.get(worker.getNodeId());

            if (node.getAvailableMemoryResources() < 0.0 && node.getAvailableCpuResources() < 0.0) {
                LOG.error("Incorrect Scheduling: found node with negative available resources");
                return false;
            }
            nodeToExecs.computeIfAbsent(node, (k) -> new HashSet<>()).add(exec);
        }

        for (Map.Entry<RAS_Node, Collection<ExecutorDetails>> entry : nodeToExecs.entrySet()) {
            RAS_Node node = entry.getKey();
            Collection<ExecutorDetails> execs = entry.getValue();
            double cpuUsed = 0.0;
            double memoryUsed = 0.0;
            for (ExecutorDetails exec : execs) {
                cpuUsed += topo.getTotalCpuReqTask(exec);
                memoryUsed += topo.getTotalMemReqTask(exec);
            }
            if (node.getAvailableCpuResources() != (node.getTotalCpuResources() - cpuUsed)) {
                LOG.error("Incorrect Scheduling: node {} has consumed incorrect amount of cpu. Expected: {}"
                        + " Actual: {} Executors scheduled on node: {}",
                        node.getId(), (node.getTotalCpuResources() - cpuUsed), node.getAvailableCpuResources(), execs);
                return false;
            }
            if (node.getAvailableMemoryResources() != (node.getTotalMemoryResources() - memoryUsed)) {
                LOG.error("Incorrect Scheduling: node {} has consumed incorrect amount of memory. Expected: {}"
                        + " Actual: {} Executors scheduled on node: {}",
                        node.getId(), (node.getTotalMemoryResources() - memoryUsed), node.getAvailableMemoryResources(), execs);
                return false;
            }
        }
        return true;
    }

    private Map<String, Set<ExecutorDetails>> getCompToExecs(Map<ExecutorDetails, String> executorToComp) {
        Map<String, Set<ExecutorDetails>> retMap = new HashMap<>();
        executorToComp.forEach((exec, comp) -> retMap.computeIfAbsent(comp, (k) -> new HashSet<>()).add(exec));
        return retMap;
    }

    private ArrayList<ExecutorDetails> getSortedExecs(HashSet<String> spreadComps, Map<String, Map<String, Integer>> constraintMatrix,
                                                      Map<String, Set<ExecutorDetails>> compToExecs) {
        ArrayList<ExecutorDetails> retList = new ArrayList<>();
        //find number of constraints per component
        //Key->Comp Value-># of constraints
        Map<String, Integer> compConstraintCountMap = new HashMap<>();
        constraintMatrix.forEach((comp, subMatrix) -> {
            int count = subMatrix.values().stream().mapToInt(Number::intValue).sum();
            //check component is declared for spreading
            if (spreadComps.contains(comp)) {
                count++;
            }
            compConstraintCountMap.put(comp, count);
        });
        //Sort comps by number of constraints
        NavigableMap<String, Integer> sortedCompConstraintCountMap = sortByValues(compConstraintCountMap);
        //sort executors based on component constraints
        for (String comp : sortedCompConstraintCountMap.keySet()) {
            retList.addAll(compToExecs.get(comp));
        }
        return retList;
    }

    private static HashSet<String> getSpreadComps(TopologyDetails topo) {
        HashSet<String> retSet = new HashSet<>();
        List<String> spread = (List<String>) topo.getConf().get(Config.TOPOLOGY_SPREAD_COMPONENTS);
        if (spread != null) {
            Set<String> comps = topo.getComponents().keySet();
            for (String comp : spread) {
                if (comps.contains(comp)) {
                    retSet.add(comp);
                } else {
                    LOG.warn("Comp {} declared for spread not valid", comp);
                }
            }
        }
        return retSet;
    }

    /**
     * Used to sort a Map by the values.
     */
    @VisibleForTesting
    public <K extends Comparable<K>, V extends Comparable<V>> NavigableMap<K, V> sortByValues(final Map<K, V> map) {
        Comparator<K> valueComparator = (k1, k2) -> {
            int compare = map.get(k2).compareTo(map.get(k1));
            if (compare == 0) {
                return k2.compareTo(k1);
            } else {
                return compare;
            }
        };
        NavigableMap<K, V> sortedByValues = new TreeMap<>(valueComparator);
        sortedByValues.putAll(map);
        return sortedByValues;
    }
}
