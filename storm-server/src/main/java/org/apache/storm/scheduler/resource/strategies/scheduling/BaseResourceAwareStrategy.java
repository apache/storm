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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.Acker;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.RasNode;
import org.apache.storm.scheduler.resource.RasNodes;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.SchedulingStatus;
import org.apache.storm.scheduler.resource.strategies.scheduling.sorter.ExecSorterByConnectionCount;
import org.apache.storm.scheduler.resource.strategies.scheduling.sorter.ExecSorterByProximity;
import org.apache.storm.scheduler.resource.strategies.scheduling.sorter.IExecSorter;
import org.apache.storm.scheduler.resource.strategies.scheduling.sorter.INodeSorter;
import org.apache.storm.scheduler.resource.strategies.scheduling.sorter.NodeSorter;
import org.apache.storm.scheduler.resource.strategies.scheduling.sorter.NodeSorterHostProximity;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseResourceAwareStrategy implements IStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(BaseResourceAwareStrategy.class);

    /**
     * Different node sorting types available. Two of these are for backward compatibility.
     * The last one (COMMON) is the new sorting type used across the board.
     * Refer to {@link NodeSorter#NodeSorter(Cluster, TopologyDetails, NodeSortType)} for more details.
     */
    public enum NodeSortType {
        /**
         * Generic Resource Aware Strategy sorting type.
         * @deprecated used by GenericResourceAwareStrategyOld only. Use {link #COMMON} instead.
         */
        @Deprecated
        GENERIC_RAS,

        /**
         * Default Resource Aware Strategy sorting type.
         * @deprecated used by DefaultResourceAwareStrategyOld only. Use {link #COMMON} instead.
         */
        @Deprecated
        DEFAULT_RAS,

        /**
         * New and only node sorting type going forward.
         * {@link NodeSorterHostProximity#NodeSorterHostProximity(Cluster, TopologyDetails)} for more details
         */
        COMMON,
    }

    // instance variables from class instantiation
    protected final boolean sortNodesForEachExecutor;
    protected final NodeSortType nodeSortType;

    // instance variable set by two IStrategy methods
    protected Map<String, Object> config;
    protected Cluster cluster;
    protected TopologyDetails topologyDetails;

    // Instance variables derived from Cluster.
    protected RasNodes nodes;
    private Map<String, List<String>> networkTopography;
    private Map<String, List<RasNode>> hostnameToNodes;

    // Instance variables derived from TopologyDetails
    protected String topoName;
    protected Map<String, Set<ExecutorDetails>> compToExecs;
    protected Map<ExecutorDetails, String> execToComp;
    protected boolean orderExecutorsByProximity;
    private long maxSchedulingTimeMs;

    // Instance variables from Cluster and TopologyDetails.
    Set<ExecutorDetails> unassignedExecutors;
    private int maxStateSearch;
    protected SchedulingSearcherState searcherState;
    protected IExecSorter execSorter;
    protected INodeSorter nodeSorter;

    public BaseResourceAwareStrategy() {
        this(true, NodeSortType.COMMON);
    }

    /**
     * Initialize for the default implementation of schedule().
     *
     * @param sortNodesForEachExecutor Sort nodes before scheduling each executor.
     * @param nodeSortType type of sorting to be applied to object resource collection {@link NodeSortType}.
     */
    public BaseResourceAwareStrategy(boolean sortNodesForEachExecutor, NodeSortType nodeSortType) {
        this.sortNodesForEachExecutor = sortNodesForEachExecutor;
        this.nodeSortType = nodeSortType;
    }

    @Override
    public void prepare(Map<String, Object> config) {
        this.config = config;
    }

    /**
     * Note that this method is not thread-safe.
     * Several instance variables are generated from supplied
     * parameters. In addition, the following instance variables are set to complete scheduling:
     *  <li>{@link #searcherState}</li>
     *  <li>{@link #execSorter} to sort executors</li>
     *  <li>{@link #nodeSorter} to sort nodes</li>
     * <p>
     * Scheduling consists of three main steps:
     *  <li>{@link #prepareForScheduling(Cluster, TopologyDetails)}</li>
     *  <li>{@link #checkSchedulingFeasibility()}, and</li>
     *  <li>{@link #scheduleExecutorsOnNodes(List, Iterable)}</li>
     * </p><p>
     * The executors and nodes are sorted in the order most conducive to scheduling for the strategy.
     * Those interfaces may be overridden by subclasses using mutators:
     *  <li>{@link #setExecSorter(IExecSorter)} and</li>
     *  <li>{@link #setNodeSorter(INodeSorter)}</li>
     *</p>
     *
     * @param cluster on which executors will be scheduled.
     * @param td the topology to schedule for.
     * @return result of scheduling (success, failure, or null when interrupted).
     */
    @Override
    public SchedulingResult schedule(Cluster cluster, TopologyDetails td) {
        prepareForScheduling(cluster, td);
        // early detection of success or failure
        SchedulingResult earlyResult = checkSchedulingFeasibility();
        if (earlyResult != null) {
            return earlyResult;
        }

        LOG.debug("Topology {} {} Number of ExecutorsNeedScheduling: {}", topoName, topologyDetails.getId(), unassignedExecutors.size());

        //order executors to be scheduled
        List<ExecutorDetails> orderedExecutors = execSorter.sortExecutors(unassignedExecutors);
        Iterable<String> sortedNodes = null;
        if (!this.sortNodesForEachExecutor) {
            nodeSorter.prepare(null);
            sortedNodes = nodeSorter.sortAllNodes();
        }
        return scheduleExecutorsOnNodes(orderedExecutors, sortedNodes);
    }

    /**
     * Initialize instance variables as the first step in {@link #schedule(Cluster, TopologyDetails)}.
     * This method may be extended by subclasses to initialize additional variables as in
     * {@link ConstraintSolverStrategy#prepareForScheduling(Cluster, TopologyDetails)}.
     *
     * @param cluster on which executors will be scheduled.
     * @param topologyDetails to be scheduled.
     */
    protected void prepareForScheduling(Cluster cluster, TopologyDetails topologyDetails) {
        this.cluster = cluster;
        this.topologyDetails = topologyDetails;

        // from Cluster
        this.nodes = new RasNodes(cluster);
        networkTopography = cluster.getNetworkTopography();
        hostnameToNodes = this.nodes.getHostnameToNodes();

        // from TopologyDetails
        topoName = topologyDetails.getName();
        execToComp = topologyDetails.getExecutorToComponent();
        compToExecs = topologyDetails.getComponentToExecutors();
        Map<String, Object> topoConf = topologyDetails.getConf();
        orderExecutorsByProximity = isOrderByProximity(topoConf);
        maxSchedulingTimeMs = computeMaxSchedulingTimeMs(topoConf);

        // From Cluster and TopologyDetails - and cleaned-up
        // all unassigned executors including system components execs
        unassignedExecutors = Collections.unmodifiableSet(new HashSet<>(cluster.getUnassignedExecutors(topologyDetails)));
        int confMaxStateSearch = getMaxStateSearchFromTopoConf(topologyDetails.getConf());
        int daemonMaxStateSearch = ObjectReader.getInt(cluster.getConf().get(DaemonConfig.RESOURCE_AWARE_SCHEDULER_MAX_STATE_SEARCH));
        maxStateSearch = Math.min(daemonMaxStateSearch, confMaxStateSearch);
        LOG.debug("The max state search configured by topology {} is {}", topologyDetails.getId(), confMaxStateSearch);
        LOG.debug("The max state search that will be used by topology {} is {}", topologyDetails.getId(), maxStateSearch);

        searcherState = createSearcherState();
        setNodeSorter(new NodeSorterHostProximity(cluster, topologyDetails, nodeSortType));
        setExecSorter(orderExecutorsByProximity
                ? new ExecSorterByProximity(topologyDetails)
                : new ExecSorterByConnectionCount(topologyDetails));

        logClusterInfo();
    }

    /**
     * Set the pluggable sorter for ExecutorDetails.
     *
     * @param execSorter to use for sorting executorDetails when scheduling.
     */
    protected void setExecSorter(IExecSorter execSorter) {
        this.execSorter = execSorter;
    }

    /**
     * Set the pluggable sorter for Nodes.
     *
     * @param nodeSorter to use for sorting nodes when scheduling.
     */
    protected void setNodeSorter(INodeSorter nodeSorter) {
        this.nodeSorter = nodeSorter;
    }

    private static long computeMaxSchedulingTimeMs(Map<String, Object> topoConf) {
        // expect to be killed by DaemonConfig.SCHEDULING_TIMEOUT_SECONDS_PER_TOPOLOGY seconds, terminate slightly before
        int daemonMaxTimeSec = ObjectReader.getInt(topoConf.get(DaemonConfig.SCHEDULING_TIMEOUT_SECONDS_PER_TOPOLOGY), 60);
        int confMaxTimeSec = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_TIME_SECS), daemonMaxTimeSec);
        return (confMaxTimeSec >= daemonMaxTimeSec) ? daemonMaxTimeSec * 1000L - 200L :  confMaxTimeSec * 1000L;
    }

    public static int getMaxStateSearchFromTopoConf(Map<String, Object> topoConf) {
        int confMaxStateSearch;
        if (topoConf.containsKey(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH)) {
            //this config is always set for topologies of 2.0 or newer versions since it is in defaults.yaml file
            //topologies of older versions can also use it if configures it explicitly
            confMaxStateSearch = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH));
        } else {
            // For backwards compatibility
            confMaxStateSearch = 10_000;
        }
        return confMaxStateSearch;
    }

    public static boolean isOrderByProximity(Map<String, Object> topoConf) {
        return ObjectReader.getBoolean(topoConf.get(Config.TOPOLOGY_RAS_ORDER_EXECUTORS_BY_PROXIMITY_NEEDS), false);
    }

    /**
     * Create an instance of {@link SchedulingSearcherState}. This method is called by
     * {@link #prepareForScheduling(Cluster, TopologyDetails)} and depends on variables initialized therein prior.
     *
     * @return a new instance of {@link SchedulingSearcherState}.
     */
    private SchedulingSearcherState createSearcherState() {
        Map<WorkerSlot, Map<String, Integer>> workerCompCnts = new HashMap<>();
        Map<RasNode, Map<String, Integer>> nodeCompCnts = new HashMap<>();

        //populate with existing assignments
        SchedulerAssignment existingAssignment = cluster.getAssignmentById(topologyDetails.getId());
        if (existingAssignment != null) {
            existingAssignment.getExecutorToSlot().forEach((exec, ws) -> {
                String compId = execToComp.get(exec);
                RasNode node = nodes.getNodeById(ws.getNodeId());
                Map<String, Integer> compCnts = nodeCompCnts.computeIfAbsent(node, (k) -> new HashMap<>());
                compCnts.put(compId, compCnts.getOrDefault(compId, 0) + 1); // increment
                //populate worker to comp assignments
                compCnts = workerCompCnts.computeIfAbsent(ws, (k) -> new HashMap<>());
                compCnts.put(compId, compCnts.getOrDefault(compId, 0) + 1); // increment
            });
        }
        LinkedList<ExecutorDetails> unassignedAckers = new LinkedList<>();
        if (compToExecs.containsKey(Acker.ACKER_COMPONENT_ID)) {
            for (ExecutorDetails acker : compToExecs.get(Acker.ACKER_COMPONENT_ID)) {
                if (unassignedExecutors.contains(acker)) {
                    unassignedAckers.add(acker);
                }
            }
        }

        return new SchedulingSearcherState(workerCompCnts, nodeCompCnts,
                maxStateSearch, maxSchedulingTimeMs, new ArrayList<>(unassignedExecutors),
                unassignedAckers, topologyDetails, execToComp);
    }

    /**
     * Check scheduling feasibility for a quick failure as the second step in {@link #schedule(Cluster, TopologyDetails)}.
     * If scheduling is not possible, then return a SchedulingStatus object with a failure status.
     * If fully scheduled then return a successful SchedulingStatus.
     * This method can be extended by subclasses {@link ConstraintSolverStrategy#checkSchedulingFeasibility()}
     * to check for additional failure conditions.
     *
     * @return A non-null {@link SchedulingResult} to terminate scheduling, otherwise return null to continue scheduling.
     */
    protected SchedulingResult checkSchedulingFeasibility() {
        if (unassignedExecutors.isEmpty()) {
            return SchedulingResult.success("Fully Scheduled by " + this.getClass().getSimpleName());
        }

        String err;
        if (nodes.getNodes().size() <= 0) {
            err = "No available nodes to schedule tasks on!";
            LOG.warn("Topology {}:{}", topoName, err);
            return SchedulingResult.failure(SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES, err);
        }

        if (!topologyDetails.hasSpouts()) {
            err = "Cannot find a Spout!";
            LOG.error("Topology {}:{}", topoName, err);
            return SchedulingResult.failure(SchedulingStatus.FAIL_INVALID_TOPOLOGY, err);
        }

        int execCnt = unassignedExecutors.size();
        if (execCnt >= maxStateSearch) {
            err = String.format("Unassignerd Executor count (%d) is greater than searchable state count %d", execCnt, maxStateSearch);
            LOG.error("Topology {}:{}", topoName, err);
            return SchedulingResult.failure(SchedulingStatus.FAIL_OTHER, err);
        }

        return null;
    }

    /**
     * Check if the assignment of the executor to the worker is valid. In simple cases,
     * this is simply a check of {@link RasNode#wouldFit(WorkerSlot, ExecutorDetails, TopologyDetails)}.
     * This method may be extended by subclasses to add additional checks,
     * see {@link ConstraintSolverStrategy#isExecAssignmentToWorkerValid(ExecutorDetails, WorkerSlot)}.
     *
     * @param exec being scheduled.
     * @param worker on which to schedule.
     * @return true if executor can be assigned to the worker, false otherwise.
     */
    protected boolean isExecAssignmentToWorkerValid(ExecutorDetails exec, WorkerSlot worker) {
        //check resources
        RasNode node = nodes.getNodeById(worker.getNodeId());
        if (!node.wouldFit(worker, exec, topologyDetails)) {
            LOG.trace("Topology {}, executor {} would not fit in resources available on worker {}", topoName, exec, worker);
            return false;
        }
        return true;
    }

    /**
     * Log a bunch of stuff for debugging.
     */
    private void logClusterInfo() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Cluster:");
            for (Map.Entry<String, List<String>> clusterEntry : networkTopography.entrySet()) {
                String rackId = clusterEntry.getKey();
                LOG.debug("Rack: {}", rackId);
                for (String nodeHostname : clusterEntry.getValue()) {
                    for (RasNode node : hostnameToNodes(nodeHostname)) {
                        LOG.debug("-> Node: {} {}", node.getHostname(), node.getId());
                        LOG.debug(
                            "--> Avail Resources: {Mem {}, CPU {} Slots: {}}",
                            node.getAvailableMemoryResources(),
                            node.getAvailableCpuResources(),
                            node.totalSlotsFree());
                        LOG.debug(
                            "--> Total Resources: {Mem {}, CPU {} Slots: {}}",
                            node.getTotalMemoryResources(),
                            node.getTotalCpuResources(),
                            node.totalSlots());
                    }
                }
            }
        }
    }

    /**
     * hostname to Ids.
     *
     * @param hostname the hostname.
     * @return the ids n that node.
     */
    public List<RasNode> hostnameToNodes(String hostname) {
        return hostnameToNodes.getOrDefault(hostname, Collections.emptyList());
    }

    /**
     * Find RASNode for specified node id.
     *
     * @param id the node/supervisor id to lookup
     * @return a RASNode object
     */
    public RasNode idToNode(String id) {
        RasNode ret = nodes.getNodeById(id);
        if (ret == null) {
            LOG.error("Cannot find Node with Id: {}", id);
        }
        return ret;
    }

    /**
     * Try to schedule till successful or till limits (backtrack count or time) have been exceeded.
     *
     * @param orderedExecutors Executors sorted in the preferred order cannot be null.
     * @param sortedNodesIter Node iterable which may be null.
     * @return SchedulingResult with success attribute set to true or false indicting whether ALL executors were assigned.
     */
    protected SchedulingResult scheduleExecutorsOnNodes(List<ExecutorDetails> orderedExecutors, Iterable<String> sortedNodesIter) {
        // isolate ackers and put it to the end of orderedExecutors
        // the order of unassigned ackers in orderedExecutors and searcherState.getUnassignedAckers() are same
        orderedExecutors.removeAll(searcherState.getUnassignedAckers());
        orderedExecutors.addAll(searcherState.getUnassignedAckers());
        LOG.debug("For topology: {}, we have sorted execs: {} and unassigned ackers: {}",
                    topoName, orderedExecutors, searcherState.getUnassignedAckers());

        long         startTimeMilli     = Time.currentTimeMillis();
        searcherState.setSortedExecs(orderedExecutors);
        int          maxExecCnt         = searcherState.getExecSize();

        // following three are state information at each "execIndex" level
        int progressIdx = -1;
        int[]        progressIdxForExec = new int[maxExecCnt];
        RasNode[]    nodeForExec        = new RasNode[maxExecCnt];
        WorkerSlot[] workerSlotForExec  = new WorkerSlot[maxExecCnt];

        for (int i = 0; i < maxExecCnt ; i++) {
            progressIdxForExec[i] = -1;
        }
        LOG.debug("scheduleExecutorsOnNodes: will assign {} executors for topo {}, sortNodesForEachExecutor={}",
                maxExecCnt, topoName, sortNodesForEachExecutor);

        OUTERMOST_LOOP:
        for (int loopCnt = 0 ; true ; loopCnt++) {
            LOG.debug("scheduleExecutorsOnNodes: loopCnt={}, execIndex={}, topo={}", loopCnt, searcherState.getExecIndex(), topoName);
            if (searcherState.areSearchLimitsExceeded()) {
                LOG.warn("Limits exceeded, backtrackCnt={}, loopCnt={}, topo={}", searcherState.getNumBacktrack(), loopCnt, topoName);
                return searcherState.createSchedulingResult(false, this.getClass().getSimpleName());
            }

            if (Thread.currentThread().isInterrupted()) {
                return searcherState.createSchedulingResult(false, this.getClass().getSimpleName());
            }

            int execIndex = searcherState.getExecIndex();
            ExecutorDetails exec = searcherState.currentExec();

            // If current exec is found in searcherState assigned Ackers,
            // it means it has been assigned as a bound acker already.
            // So we skip to the next.
            if (searcherState.getBoundAckers().contains(exec)) {
                if (searcherState.areAllExecsScheduled()) {
                    //Everything is scheduled correctly, so no need to search any more.
                    LOG.info("scheduleExecutorsOnNodes: Done at loopCnt={} in {}ms, state.elapsedtime={}, backtrackCnt={}, topo={}",
                        loopCnt, Time.currentTimeMillis() - startTimeMilli,
                        Time.currentTimeMillis() - searcherState.startTimeMillis,
                        searcherState.getNumBacktrack(),
                        topoName);
                    return searcherState.createSchedulingResult(true, this.getClass().getSimpleName());
                }
                searcherState = searcherState.nextExecutor();
                continue OUTERMOST_LOOP;
            }

            String comp = execToComp.get(exec);
            if (sortedNodesIter == null || (this.sortNodesForEachExecutor && searcherState.isExecCompDifferentFromPrior())) {
                progressIdx = -1;
                nodeSorter.prepare(exec);
                sortedNodesIter = nodeSorter.sortAllNodes();
            }

            for (String nodeId : sortedNodesIter) {
                RasNode node = nodes.getNodeById(nodeId);
                if (!node.couldEverFit(exec, topologyDetails)) {
                    continue;
                }
                for (WorkerSlot workerSlot : node.getSlotsAvailableToScheduleOn()) {
                    progressIdx++;
                    if (progressIdx <= progressIdxForExec[execIndex]) {
                        continue;
                    }
                    progressIdxForExec[execIndex]++;

                    if (!isExecAssignmentToWorkerValid(exec, workerSlot)) {
                        // exec can't fit in this workerSlot, try next workerSlot
                        LOG.debug("Failed to assign exec={}, comp={}, topo={} to worker={} on node=({}, availCpu={}, availMem={}).",
                            exec, comp, topoName, workerSlot,
                            node.getId(), node.getAvailableCpuResources(), node.getAvailableMemoryResources());
                        continue;
                    }

                    searcherState.incStatesSearched();
                    searcherState.assignCurrentExecutor(execToComp, node, workerSlot);
                    int numBoundAckerAssigned = assignBoundAckersForNewWorkerSlot(exec, node, workerSlot);
                    if (numBoundAckerAssigned > 0) {
                        // This exec with some of its bounded ackers have all been successfully assigned
                        searcherState.getExecsWithBoundAckers().add(exec);
                    }

                    if (searcherState.areAllExecsScheduled()) {
                        //Everything is scheduled correctly, so no need to search any more.
                        LOG.info("scheduleExecutorsOnNodes: Done at loopCnt={} in {}ms, state.elapsedtime={}, backtrackCnt={}, topo={}",
                                loopCnt, System.currentTimeMillis() - startTimeMilli,
                                Time.currentTimeMillis() - searcherState.startTimeMillis,
                                searcherState.getNumBacktrack(),
                                topoName);
                        return searcherState.createSchedulingResult(true, this.getClass().getSimpleName());
                    }
                    searcherState = searcherState.nextExecutor();
                    nodeForExec[execIndex] = node;
                    workerSlotForExec[execIndex] = workerSlot;
                    LOG.debug("scheduleExecutorsOnNodes: Assigned execId={}, comp={} to node={}/cpu={}/mem={}, "
                            + "slot-ordinal={} at loopCnt={}, topo={}",
                        execIndex, comp, nodeId, node.getAvailableCpuResources(), node.getAvailableMemoryResources(),
                        progressIdx, loopCnt, topoName);
                    continue OUTERMOST_LOOP;
                }
            }
            sortedNodesIter = null;
            // if here, then the executor was not assigned, backtrack;
            LOG.debug("scheduleExecutorsOnNodes: Failed to schedule execId={}, comp={} at loopCnt={}, topo={}",
                    execIndex, comp, loopCnt, topoName);
            if (execIndex == 0) {
                break;
            } else {
                searcherState.backtrack(execToComp, nodeForExec[execIndex - 1], workerSlotForExec[execIndex - 1]);
                progressIdxForExec[execIndex] = -1;
            }
        }
        boolean success = searcherState.areAllExecsScheduled();
        LOG.info("scheduleExecutorsOnNodes: Scheduled={} in {} milliseconds, state.elapsedtime={}, backtrackCnt={}, topo={}",
                success, System.currentTimeMillis() - startTimeMilli, Time.currentTimeMillis() - searcherState.startTimeMillis,
                searcherState.getNumBacktrack(),
                topoName);
        return searcherState.createSchedulingResult(success, this.getClass().getSimpleName());
    }

    /**
     * <p>
     * Determine how many bound ackers to put into the given workerSlot.
     * Then try to assign the ackers one by one into this workerSlot upto the calculated
     * maximum required. Return the number of ackers assigned.
     *
     * Return 0 if one of the conditions hold true:
     *  1. No bound ackers are used.
     *  2. This is not first exec assigned to this worker.
     *  3. No ackers could be assigned because of space or exception.
     *
     * </p>
     * @param exec              being scheduled.
     * @param node              RasNode on which to schedule.
     * @param workerSlot        WorkerSlot on which to schedule.
     * @return                  Number of ackers assigned.
     */
    private int assignBoundAckersForNewWorkerSlot(ExecutorDetails exec, RasNode node, WorkerSlot workerSlot) {
        int numOfAckersToBind = searcherState.getNumOfAckersToBind(exec, workerSlot);
        if (numOfAckersToBind > 0) {
            for (int i = 0; i < numOfAckersToBind; i++) {
                if (!isExecAssignmentToWorkerValid(searcherState.peekUnassignedAckers(), workerSlot)) {
                    LOG.debug("Assigned {} of {} ackers on workerSlot={} with the executor={} for topology={}",
                        i, numOfAckersToBind, workerSlot, exec, topoName);
                    return i;
                }
                try {
                    searcherState.assignSingleBoundAcker(node, workerSlot);
                } catch (Exception e) {
                    LOG.error("Exception happens when assigning {} of {} ackers on workerSlot={} for topology={}",
                                i + 1, numOfAckersToBind, workerSlot, topoName, e);
                    return i;
                }
            }
        }
        LOG.debug("Assigned {} ackers on workerSlot={} with the executor={} for topology={}",
            numOfAckersToBind, workerSlot, exec, topoName);
        return numOfAckersToBind;
    }
}
