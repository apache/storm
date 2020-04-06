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

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.storm.DaemonConfig;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.RasNode;
import org.apache.storm.scheduler.resource.RasNodes;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.SchedulingStatus;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.apache.storm.validation.ConfigValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConstraintSolverStrategy extends BaseResourceAwareStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ConstraintSolverStrategy.class);

    public static final String CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT = "maxNodeCoLocationCnt";
    public static final String CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS = "incompatibleComponents";

    /**
     * Component constraint as derived from configuration.
     * This is backward compatible and can parse old style Config.TOPOLOGY_RAS_CONSTRAINTS and Config.TOPOLOGY_SPREAD_COMPONENTS.
     * New style Config.TOPOLOGY_RAS_CONSTRAINTS is map where each component has a list of other incompatible components
     * and an optional number that specifies the maximum co-location count for the component on a node.
     *
     * <p>comp-1 cannot exist on same worker as comp-2 or comp-3, and at most "2" comp-1 on same node</p>
     * <p>comp-2 and comp-4 cannot be on same worker (missing comp-1 is implied from comp-1 constraint)</p>
     *
     *  <p>
     *      { "comp-1": { "maxNodeCoLocationCnt": 2, "incompatibleComponents": ["comp-2", "comp-3" ] },
     *        "comp-2": { "incompatibleComponents": [ "comp-4" ] }
     *      }
     *  </p>
     */
    public static final class ConstraintConfig {
        private Map<String, Set<String>> incompatibleComponents = new HashMap<>();
        private Map<String, Integer> maxCoLocationCnts = new HashMap<>(); // maximum node CoLocationCnt for restricted components

        ConstraintConfig(TopologyDetails topo) {
            // getExecutorToComponent().values() also contains system components
            this(topo.getConf(), Sets.union(topo.getComponents().keySet(), new HashSet(topo.getExecutorToComponent().values())));
        }

        ConstraintConfig(Map<String, Object> conf, Set<String> comps) {
            Object rasConstraints = conf.get(Config.TOPOLOGY_RAS_CONSTRAINTS);
            comps.forEach(k -> incompatibleComponents.computeIfAbsent(k, x -> new HashSet<>()));
            if (rasConstraints instanceof List) {
                // old style
                List<List<String>> constraints = (List<List<String>>) rasConstraints;
                for (List<String> constraintPair : constraints) {
                    String comp1 = constraintPair.get(0);
                    String comp2 = constraintPair.get(1);
                    if (!comps.contains(comp1)) {
                        LOG.warn("Comp: {} declared in constraints is not valid!", comp1);
                        continue;
                    }
                    if (!comps.contains(comp2)) {
                        LOG.warn("Comp: {} declared in constraints is not valid!", comp2);
                        continue;
                    }
                    incompatibleComponents.get(comp1).add(comp2);
                    incompatibleComponents.get(comp2).add(comp1);
                }
            } else {
                Map<String, Map<String, ?>> constraintMap = (Map<String, Map<String, ?>>) rasConstraints;
                constraintMap.forEach((comp1, v) -> {
                    if (comps.contains(comp1)) {
                        v.forEach((ctype, constraint) -> {
                            switch (ctype) {
                                case CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT:
                                    try {
                                        int numValue = Integer.parseInt("" + constraint);
                                        if (numValue < 1) {
                                            LOG.warn("{} {} declared for Comp {} is not valid, expected >= 1", ctype, numValue, comp1);
                                        } else {
                                            maxCoLocationCnts.put(comp1, numValue);
                                        }
                                    } catch (Exception ex) {
                                        LOG.warn("{} {} declared for Comp {} is not valid, expected >= 1", ctype, constraint, comp1);
                                    }
                                    break;

                                case CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS:
                                    if (!(constraint instanceof List || constraint instanceof String)) {
                                        LOG.warn("{} {} declared for Comp {} is not valid, expecting a list of components or 1 component",
                                                ctype, constraint, comp1);
                                        break;
                                    }
                                    List<String> list;
                                    list = (constraint instanceof String) ? Arrays.asList((String) constraint) : (List<String>) constraint;
                                    for (String comp2: list) {
                                        if (!comps.contains(comp2)) {
                                            LOG.warn("{} {} declared for Comp {} is not a valid component", ctype, comp2, comp1);
                                            continue;
                                        }
                                        incompatibleComponents.get(comp1).add(comp2);
                                        incompatibleComponents.get(comp2).add(comp1);
                                    }
                                    break;

                                default:
                                    LOG.warn("ConstraintType={} invalid for component={}, valid values are {} and {}, ignoring value={}",
                                            ctype, comp1, CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT,
                                            CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS, constraint);
                                    break;
                            }
                        });
                    } else {
                        LOG.warn("Component {} is not a valid component", comp1);
                    }
                });
            }

            // process Config.TOPOLOGY_SPREAD_COMPONENTS - old style
            // override only if not defined already using Config.TOPOLOGY_RAS_COMPONENTS above
            Object obj = conf.get(Config.TOPOLOGY_SPREAD_COMPONENTS);
            if (obj instanceof List) {
                List<String> spread = (List<String>) obj;
                if (spread != null) {
                    for (String comp : spread) {
                        if (!comps.contains(comp)) {
                            LOG.warn("Comp {} declared for spread not valid", comp);
                            continue;
                        }
                        if (maxCoLocationCnts.containsKey(comp)) {
                            LOG.warn("Comp {} maxNodeCoLocationCnt={} already defined in {}, ignoring spread config in {}", comp,
                                    maxCoLocationCnts.get(comp), Config.TOPOLOGY_RAS_CONSTRAINTS, Config.TOPOLOGY_SPREAD_COMPONENTS);
                            continue;
                        }
                        maxCoLocationCnts.put(comp, 1);
                    }
                }
            } else {
                LOG.warn("Ignoring invalid {} config={}", Config.TOPOLOGY_SPREAD_COMPONENTS, obj);
            }
        }

        public Map<String, Set<String>> getIncompatibleComponents() {
            return incompatibleComponents;
        }

        public Map<String, Integer> getMaxCoLocationCnts() {
            return maxCoLocationCnts;
        }
    }

    private Map<String, RasNode> nodes;
    private Map<ExecutorDetails, String> execToComp;
    private Map<String, Set<ExecutorDetails>> compToExecs;
    private List<String> favoredNodeIds;
    private List<String> unFavoredNodeIds;
    private ConstraintConfig constraintConfig;

    /**
     * Determines if a scheduling is valid and all constraints are satisfied.
     */
    @VisibleForTesting
    public static boolean validateSolution(Cluster cluster, TopologyDetails td, ConstraintConfig constraintConfig) {
        if (constraintConfig == null) {
            constraintConfig = new ConstraintConfig(td);
        }
        return checkSpreadSchedulingValid(cluster, td, constraintConfig)
               && checkConstraintsSatisfied(cluster, td, constraintConfig)
               && checkResourcesCorrect(cluster, td);
    }

    /**
     * Check if constraints are satisfied.
     */
    private static boolean checkConstraintsSatisfied(Cluster cluster, TopologyDetails topo, ConstraintConfig constraintConfig) {
        LOG.info("Checking constraints...");
        assert (cluster.getAssignmentById(topo.getId()) != null);
        if (constraintConfig == null) {
            constraintConfig = new ConstraintConfig(topo);
        }
        Map<ExecutorDetails, WorkerSlot> result = cluster.getAssignmentById(topo.getId()).getExecutorToSlot();
        Map<ExecutorDetails, String> execToComp = topo.getExecutorToComponent();
        //get topology constraints
        Map<String, Set<String>> constraintMatrix = constraintConfig.incompatibleComponents;

        Map<WorkerSlot, Set<String>> workerCompMap = new HashMap<>();
        result.forEach((exec, worker) -> {
            String comp = execToComp.get(exec);
            workerCompMap.computeIfAbsent(worker, (k) -> new HashSet<>()).add(comp);
        });
        for (Map.Entry<WorkerSlot, Set<String>> entry : workerCompMap.entrySet()) {
            Set<String> comps = entry.getValue();
            for (String comp1 : comps) {
                for (String comp2 : comps) {
                    if (!comp1.equals(comp2) && constraintMatrix.get(comp1).contains(comp2)) {
                        LOG.error("Incorrect Scheduling: worker exclusion for Component {} and {} not satisfied on WorkerSlot: {}",
                                  comp1, comp2, entry.getKey());
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private static Map<WorkerSlot, RasNode> workerToNodes(Cluster cluster) {
        Map<WorkerSlot, RasNode> workerToNodes = new HashMap<>();
        for (RasNode node : RasNodes.getAllNodesFrom(cluster).values()) {
            for (WorkerSlot s : node.getUsedSlots()) {
                workerToNodes.put(s, node);
            }
        }
        return workerToNodes;
    }

    private static boolean checkSpreadSchedulingValid(Cluster cluster, TopologyDetails topo, ConstraintConfig constraintConfig) {
        LOG.info("Checking for a valid scheduling...");
        assert (cluster.getAssignmentById(topo.getId()) != null);
        if (constraintConfig == null) {
            constraintConfig = new ConstraintConfig(topo);
        }
        Map<ExecutorDetails, String> execToComp = topo.getExecutorToComponent();
        Map<String, Map<String, Integer>> nodeCompMap = new HashMap<>(); // this is the critical count
        Map<WorkerSlot, RasNode> workerToNodes = workerToNodes(cluster);
        boolean ret = true;

        Map<String, Integer> spreadCompCnts = constraintConfig.maxCoLocationCnts;
        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : cluster.getAssignmentById(topo.getId()).getExecutorToSlot().entrySet()) {
            ExecutorDetails exec = entry.getKey();
            String comp = execToComp.get(exec);
            WorkerSlot worker = entry.getValue();
            RasNode node = workerToNodes.get(worker);
            String nodeId = node.getId();

            if (spreadCompCnts.containsKey(comp)) {
                int allowedColocationMaxCnt = spreadCompCnts.get(comp);
                Map<String, Integer> oneNodeCompMap = nodeCompMap.computeIfAbsent(nodeId, (k) -> new HashMap<>());
                oneNodeCompMap.put(comp, oneNodeCompMap.getOrDefault(comp, 0) + 1);
                if (allowedColocationMaxCnt < oneNodeCompMap.get(comp)) {
                    LOG.error("Incorrect Scheduling: MaxCoLocationCnt for Component: {} {} on node {} not satisfied, cnt {} > allowed {}",
                            comp, exec, nodeId, oneNodeCompMap.get(comp), allowedColocationMaxCnt);
                    ret = false;
                }
            }
        }
        if (!ret) {
            LOG.error("Incorrect MaxCoLocationCnts: Node-Component-Cnt {}", nodeCompMap);
        }
        return ret;
    }

    /**
     * Check if resource constraints satisfied.
     */
    private static boolean checkResourcesCorrect(Cluster cluster, TopologyDetails topo) {
        LOG.info("Checking Resources...");
        assert (cluster.getAssignmentById(topo.getId()) != null);
        Map<ExecutorDetails, WorkerSlot> result = cluster.getAssignmentById(topo.getId()).getExecutorToSlot();
        Map<RasNode, Collection<ExecutorDetails>> nodeToExecs = new HashMap<>();
        Map<ExecutorDetails, WorkerSlot> mergedExecToWorker = new HashMap<>();
        Map<String, RasNode> nodes = RasNodes.getAllNodesFrom(cluster);
        //merge with existing assignments
        if (cluster.getAssignmentById(topo.getId()) != null
            && cluster.getAssignmentById(topo.getId()).getExecutorToSlot() != null) {
            mergedExecToWorker.putAll(cluster.getAssignmentById(topo.getId()).getExecutorToSlot());
        }
        mergedExecToWorker.putAll(result);

        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : mergedExecToWorker.entrySet()) {
            ExecutorDetails exec = entry.getKey();
            WorkerSlot worker = entry.getValue();
            RasNode node = nodes.get(worker.getNodeId());

            if (node.getAvailableMemoryResources() < 0.0 && node.getAvailableCpuResources() < 0.0) {
                LOG.error("Incorrect Scheduling: found node with negative available resources");
                return false;
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

    @Override
    public SchedulingResult schedule(Cluster cluster, TopologyDetails td) {
        prepare(cluster);
        LOG.debug("Scheduling {}", td.getId());
        nodes = RasNodes.getAllNodesFrom(cluster);
        Map<WorkerSlot, Map<String, Integer>> workerCompAssignment = new HashMap<>();
        Map<RasNode, Map<String, Integer>> nodeCompAssignment = new HashMap<>();

        int confMaxStateSearch = ObjectReader.getInt(td.getConf().get(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH));
        int daemonMaxStateSearch = ObjectReader.getInt(cluster.getConf().get(DaemonConfig.RESOURCE_AWARE_SCHEDULER_MAX_STATE_SEARCH));
        final int maxStateSearch = Math.min(daemonMaxStateSearch, confMaxStateSearch);

        // expect to be killed by DaemonConfig.SCHEDULING_TIMEOUT_SECONDS_PER_TOPOLOGY seconds, terminate slightly before
        int daemonMaxTimeSec = ObjectReader.getInt(td.getConf().get(DaemonConfig.SCHEDULING_TIMEOUT_SECONDS_PER_TOPOLOGY), 60);
        int confMaxTimeSec = ObjectReader.getInt(td.getConf().get(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_TIME_SECS), daemonMaxTimeSec);
        final long maxTimeMs = (confMaxTimeSec >= daemonMaxTimeSec) ? daemonMaxTimeSec * 1000L - 200L :  confMaxTimeSec * 1000L;

        favoredNodeIds = makeHostToNodeIds((List<String>) td.getConf().get(Config.TOPOLOGY_SCHEDULER_FAVORED_NODES));
        unFavoredNodeIds = makeHostToNodeIds((List<String>) td.getConf().get(Config.TOPOLOGY_SCHEDULER_UNFAVORED_NODES));

        //get mapping of execs to components
        execToComp = td.getExecutorToComponent();
        //get mapping of components to executors
        compToExecs = getCompToExecs(execToComp);

        // get constraint configuration
        constraintConfig = new ConstraintConfig(td);

        //get a sorted list of unassigned executors based on number of constraints
        Set<ExecutorDetails> unassignedExecutors = new HashSet<>(cluster.getUnassignedExecutors(td));
        List<ExecutorDetails> sortedExecs;
        sortedExecs = getSortedExecs(constraintConfig.maxCoLocationCnts, constraintConfig.incompatibleComponents, compToExecs).stream()
                .filter(unassignedExecutors::contains)
                .collect(Collectors.toList());

        //populate with existing assignments
        SchedulerAssignment existingAssignment = cluster.getAssignmentById(td.getId());
        if (existingAssignment != null) {
            existingAssignment.getExecutorToSlot().forEach((exec, ws) -> {
                String compId = execToComp.get(exec);
                RasNode node = nodes.get(ws.getNodeId());
                Map<String, Integer> oneMap = nodeCompAssignment.computeIfAbsent(node, (k) -> new HashMap<>());
                oneMap.put(compId, oneMap.getOrDefault(compId, 0) + 1); // increment
                //populate worker to comp assignments
                oneMap = workerCompAssignment.computeIfAbsent(ws, (k) -> new HashMap<>());
                oneMap.put(compId, oneMap.getOrDefault(compId, 0) + 1); // increment
            });
        }

        //early detection/early fail
        if (!checkSchedulingFeasibility(maxStateSearch)) {
            //Scheduling Status set to FAIL_OTHER so no eviction policy will be attempted to make space for this topology
            return SchedulingResult.failure(SchedulingStatus.FAIL_OTHER, "Scheduling not feasible!");
        }
        return backtrackSearch(new SearcherState(workerCompAssignment, nodeCompAssignment, maxStateSearch, maxTimeMs, sortedExecs, td))
            .asSchedulingResult();
    }

    private boolean checkSchedulingFeasibility(int maxStateSearch) {
        for (Map.Entry<String, Integer> entry : constraintConfig.maxCoLocationCnts.entrySet()) {
            String comp = entry.getKey();
            int maxCoLocationCnt = entry.getValue();
            int numExecs = compToExecs.get(comp).size();
            if (numExecs > nodes.size() * maxCoLocationCnt) {
                LOG.error("Unsatisfiable constraint: Component: {} marked as spread has {} executors which is larger "
                          + "than number of nodes * maxCoLocationCnt: {} * {} ", comp, numExecs, nodes.size(), maxCoLocationCnt);
                return false;
            }
        }
        if (execToComp.size() >= maxStateSearch) {
            LOG.error("Number of executors is greater than the maximum number of states allowed to be searched.  "
                      + "# of executors: {} Max states to search: {}", execToComp.size(), maxStateSearch);
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

    /**
     * Try to schedule till successful or till limits (backtrack count or time) have been exceeded.
     *
     * @param state terminal state of the executor assignment.
     * @return SolverResult with success attribute set to true or false indicting whether ALL executors were assigned.
     */
    @VisibleForTesting
    protected SolverResult backtrackSearch(SearcherState state) {
        long         startTimeMilli     = System.currentTimeMillis();
        int          maxExecCnt         = state.getExecSize();

        // following three are state information at each "execIndex" level
        int[]        progressIdxForExec = new int[maxExecCnt];
        RasNode[]    nodeForExec        = new RasNode[maxExecCnt];
        WorkerSlot[] workerSlotForExec  = new WorkerSlot[maxExecCnt];

        for (int i = 0; i < maxExecCnt ; i++) {
            progressIdxForExec[i] = -1;
        }
        LOG.info("backtrackSearch: will assign {} executors", maxExecCnt);

        OUTERMOST_LOOP:
        for (int loopCnt = 0 ; true ; loopCnt++) {
            LOG.debug("backtrackSearch: loopCnt = {}, state.execIndex = {}", loopCnt, state.execIndex);
            if (state.areSearchLimitsExceeded()) {
                LOG.warn("backtrackSearch: Search limits exceeded, backtracked {} times, looped {} times", state.numBacktrack, loopCnt);
                return new SolverResult(state, false);
            }

            if (Thread.currentThread().isInterrupted()) {
                return new SolverResult(state, false);
            }

            int execIndex = state.execIndex;

            ExecutorDetails exec = state.currentExec();
            String comp = execToComp.get(exec);
            Iterable<String> sortedNodesIter = sortAllNodes(state.td, exec, favoredNodeIds, unFavoredNodeIds);

            int progressIdx = -1;
            for (String nodeId : sortedNodesIter) {
                RasNode node = nodes.get(nodeId);
                for (WorkerSlot workerSlot : node.getSlotsAvailableToScheduleOn()) {
                    progressIdx++;
                    if (progressIdx <= progressIdxForExec[execIndex]) {
                        continue;
                    }
                    progressIdxForExec[execIndex]++;
                    LOG.debug("backtrackSearch: loopCnt = {}, state.execIndex = {}, comp = {}, node/slot-ordinal = {}, nodeId = {}",
                            loopCnt, execIndex, comp, progressIdx, nodeId);

                    if (!isExecAssignmentToWorkerValid(workerSlot, state)) {
                        continue;
                    }

                    state.incStatesSearched();
                    state.tryToSchedule(execToComp, node, workerSlot);
                    if (state.areAllExecsScheduled()) {
                        //Everything is scheduled correctly, so no need to search any more.
                        LOG.info("backtrackSearch: AllExecsScheduled at loopCnt={} in {} ms, elapsedtime in state={}, backtrackCnt={}",
                                loopCnt, System.currentTimeMillis() - startTimeMilli, Time.currentTimeMillis() - state.startTimeMillis,
                                state.numBacktrack);
                        return new SolverResult(state, true);
                    }
                    state = state.nextExecutor();
                    nodeForExec[execIndex] = node;
                    workerSlotForExec[execIndex] = workerSlot;
                    LOG.debug("backtrackSearch: Assigned execId={}, comp={} to node={}, node/slot-ordinal={} at loopCnt={}",
                            execIndex, comp, nodeId, progressIdx, loopCnt);
                    continue OUTERMOST_LOOP;
                }
            }
            // if here, then the executor was not assigned, backtrack;
            LOG.debug("backtrackSearch: Failed to schedule execId={}, comp={} at loopCnt={}", execIndex, comp, loopCnt);
            if (execIndex == 0) {
                break;
            } else {
                state.backtrack(execToComp, nodeForExec[execIndex - 1], workerSlotForExec[execIndex - 1]);
                progressIdxForExec[execIndex] = -1;
            }
        }
        boolean success = state.areAllExecsScheduled();
        LOG.info("backtrackSearch: Scheduled={} in {} milliseconds, elapsedtime in state={}, backtrackCnt={}",
            success, System.currentTimeMillis() - startTimeMilli, Time.currentTimeMillis() - state.startTimeMillis, state.numBacktrack);
        return new SolverResult(state, success);
    }

    /**
     * Check if any constraints are violated if exec is scheduled on worker.
     * @return true if scheduling exec on worker does not violate any constraints, returns false if it does
     */
    public boolean isExecAssignmentToWorkerValid(WorkerSlot worker, SearcherState state) {
        final ExecutorDetails exec = state.currentExec();
        //check resources
        RasNode node = nodes.get(worker.getNodeId());
        if (!node.wouldFit(worker, exec, state.td)) {
            LOG.trace("{} would not fit in resources available on {}", exec, worker);
            return false;
        }

        //check if exec can be on worker based on user defined component exclusions
        String execComp = execToComp.get(exec);
        Map<String, Integer> compAssignmentCnts = state.workerCompAssignmentCnts.get(worker);
        if (compAssignmentCnts != null && constraintConfig.incompatibleComponents.containsKey(execComp)) {
            Set<String> subMatrix = constraintConfig.incompatibleComponents.get(execComp);
            for (String comp : compAssignmentCnts.keySet()) {
                if (subMatrix.contains(comp)) {
                    LOG.trace("{} found {} constraint violation {} on {}", exec, execComp, comp, worker);
                    return false;
                }
            }
        }

        //check if exec satisfy spread
        if (constraintConfig.maxCoLocationCnts.containsKey(execComp)) {
            int coLocationMaxCnt = constraintConfig.maxCoLocationCnts.get(execComp);
            if (state.nodeCompAssignmentCnts.containsKey(node)
                    && state.nodeCompAssignmentCnts.get(node).getOrDefault(execComp, 0) >= coLocationMaxCnt) {
                LOG.trace("{} Found MaxCoLocationCnt violation {} on node {}, count {} >= colocation count {}",
                        exec, execComp, node.getId(), state.nodeCompAssignmentCnts.get(node).get(execComp), coLocationMaxCnt);
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

    private ArrayList<ExecutorDetails> getSortedExecs(Map<String, Integer> spreadCompCnts,
                                                      Map<String, Set<String>> constraintMatrix,
                                                      Map<String, Set<ExecutorDetails>> compToExecs) {
        ArrayList<ExecutorDetails> retList = new ArrayList<>();
        //find number of constraints per component
        //Key->Comp Value-># of constraints
        Map<String, Double> compConstraintCountMap = new HashMap<>();
        constraintMatrix.forEach((comp, subMatrix) -> {
            double count = subMatrix.size();
            // check if component is declared for spreading
            if (spreadCompCnts.containsKey(comp)) {
                // lower (1 and above only) value is most constrained should have higher count
                count += (compToExecs.size() / spreadCompCnts.get(comp));
            }
            compConstraintCountMap.put(comp, count); // higher count sorts to the front
        });
        //Sort comps by number of constraints
        NavigableMap<String, Double> sortedCompConstraintCountMap = sortByValues(compConstraintCountMap);
        //sort executors based on component constraints
        for (String comp : sortedCompConstraintCountMap.keySet()) {
            retList.addAll(compToExecs.get(comp));
        }
        return retList;
    }

    /**
     * Used to sort a Map by the values - higher values up front.
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

    protected static final class SolverResult {
        private final SearcherState state;
        private final int statesSearched;
        private final boolean success;
        private final long timeTakenMillis;
        private final int backtracked;

        public SolverResult(SearcherState state, boolean success) {
            this.state = state;
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
            state.logNodeCompAssignments();
            return SchedulingResult.failure(SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES,
                                            "Cannot find scheduling that satisfies all constraints (" + statesSearched
                                            + " states traversed in " + timeTakenMillis + "ms, backtracked " + backtracked + " times)");
        }
    }

    protected static final class SearcherState {
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
        private final List<ExecutorDetails> execs;
        //The maximum number of state to search before stopping.
        private final int maxStatesSearched;
        //The topology we are scheduling
        private final TopologyDetails td;
        // Metrics
        // How many states searched so far.
        private int statesSearched = 0;
        // Number of times we had to backtrack.
        private int numBacktrack = 0;
        // Current state
        // The current executor we are trying to schedule
        private int execIndex = 0;

        private SearcherState(Map<WorkerSlot, Map<String, Integer>> workerCompAssignmentCnts,
                              Map<RasNode, Map<String, Integer>> nodeCompAssignmentCnts, int maxStatesSearched, long maxTimeMs,
                              List<ExecutorDetails> execs, TopologyDetails td) {
            assert !execs.isEmpty();
            assert execs != null;

            this.workerCompAssignmentCnts = workerCompAssignmentCnts;
            this.nodeCompAssignmentCnts = nodeCompAssignmentCnts;
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

        public int getExecSize() {
            return execs.size();
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

        /**
          * Assign executor to worker and node.
          * TODO: tryToSchedule is a misnomer, since it always schedules.
          * Assignment validity check is done before the call to tryToSchedule().
          *
          * @param execToComp Mapping from executor to component name.
          * @param node RasNode on which to schedule.
          * @param workerSlot WorkerSlot on which to schedule.
          */
        public void tryToSchedule(Map<ExecutorDetails, String> execToComp, RasNode node, WorkerSlot workerSlot) {
            ExecutorDetails exec = currentExec();
            String comp = execToComp.get(exec);
            LOG.trace("Trying assignment of {} {} to {}", exec, comp, workerSlot);
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
                throw new IllegalStateException("Internal Error: exec index became negative");
            }
            numBacktrack++;
            ExecutorDetails exec = currentExec();
            String comp = execToComp.get(exec);
            LOG.trace("Backtracking {} {} from {}", exec, comp, workerSlot);
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
                LOG.info("NodeCompAssignment is empty");
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
                String oneMapJoined = String.join(
                        ",",
                        oneMap.entrySet()
                                .stream().map(e -> String.format("%s: %s", e.getKey(), e.getValue()))
                                .collect(Collectors.toList())
                );
                sb.append(String.format("\n\t(%d) Node %s: %s", cntFilledNodes, node.getId(), oneMapJoined));
            }
            LOG.info("NodeCompAssignments available for {} of {} nodes {}", cntFilledNodes, cntAllNodes, sb);
            LOG.info("Executors assignments attempted (cnt={}) are: \n\t{}",
                    execs.size(), execs.stream().map(x -> x.toString()).collect(Collectors.joining(","))
            );
        }
    }
}


