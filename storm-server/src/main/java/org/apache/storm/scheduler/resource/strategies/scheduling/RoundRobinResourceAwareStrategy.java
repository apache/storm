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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.RasNode;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoundRobinResourceAwareStrategy extends BaseResourceAwareStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(RoundRobinResourceAwareStrategy.class);

    public RoundRobinResourceAwareStrategy() {
        super(false, NodeSortType.COMMON);
    }

    /**
     * Maximum number of isolated nodes being requested based on the topology configuration
     * {@link Config#TOPOLOGY_ISOLATED_MACHINES}.
     */
    private int getMaxNumberOfNodesRequested() {
        Map<String, Object> conf = topologyDetails.getConf();
        if (conf.get(Config.TOPOLOGY_ISOLATED_MACHINES) == null) {
            return Integer.MAX_VALUE;
        } else {
            return ((Number) topologyDetails.getConf().get(Config.TOPOLOGY_ISOLATED_MACHINES)).intValue();
        }
    }

    /**
     * If the number of machines is limited, then truncate the node list to this maximum number of nodes
     * that have no other topologies running on it. If the current topology is running on it, then it
     * is subject to selection in the list. If other topologies are running on it, then it is not selected.
     *
     * @param sortedNodesIterable Iterable of nodes
     * @return an ArrayList of nodes
     */
    private ArrayList<String> getTruncatedNodeList(Iterable<String> sortedNodesIterable) {
        final int maxNodes = getMaxNumberOfNodesRequested();
        final ArrayList<String> ret = new ArrayList<>();
        sortedNodesIterable.forEach(node -> {
            if (ret.size() < maxNodes) {
                RasNode rasNode = nodes.getNodeById(node);
                Collection<String> runningTopos = rasNode.getRunningTopologies();
                if (runningTopos.isEmpty() || runningTopos.size() == 1 && runningTopos.contains(topologyDetails.getId())) {
                    ret.add(node);
                }
            }
        });
        return ret;
    }

    /**
     * For each component try to schedule executors in sequence on the nodes.
     *
     * @param orderedExecutors Executors sorted in the preferred order cannot be null
     * @param sortedNodesIterable Node iterable which cannot be null, relies on behavior when {@link #sortNodesForEachExecutor} is false
     * @return SchedulingResult with success attribute set to true or false indicting whether ALL executors were assigned. @{#}
     */
    @Override
    protected SchedulingResult scheduleExecutorsOnNodes(List<ExecutorDetails> orderedExecutors, Iterable<String> sortedNodesIterable) {
        long startTimeMilli = Time.currentTimeMillis();
        int  maxExecCnt     = searcherState.getExecSize();
        int  nodeSortCnt    = 1;
        Iterator<String> sortedNodesIter = null;
        ArrayList<String> sortedNodes = getTruncatedNodeList(sortedNodesIterable);

        LOG.debug("scheduleExecutorsOnNodes: will assign {} executors for topo {}", maxExecCnt, topoName);

        searcherState.setSortedExecs(orderedExecutors);

        OUTERMOST_LOOP:
        for (int loopCnt = 0 ; true ; loopCnt++) {
            LOG.debug("scheduleExecutorsOnNodes: loopCnt={}, execIndex={}, topo={}, nodeSortCnt={}",
                    loopCnt, searcherState.getExecIndex(), topoName, nodeSortCnt);
            if (searcherState.areSearchLimitsExceeded()) {
                LOG.warn("Limits exceeded, loopCnt={}, topo={}, nodeSortCnt={}", loopCnt, topoName, nodeSortCnt);
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
                    LOG.info("scheduleExecutorsOnNodes: Done at loopCnt={} in {}ms, state.elapsedtime={}, topo={}, nodeSortCnt={}",
                        loopCnt, Time.currentTimeMillis() - startTimeMilli,
                        Time.currentTimeMillis() - searcherState.getStartTimeMillis(),
                        topoName, nodeSortCnt);
                    return searcherState.createSchedulingResult(true, this.getClass().getSimpleName());
                }
                searcherState = searcherState.nextExecutor();
                continue OUTERMOST_LOOP;
            }

            String comp = execToComp.get(exec);
            // start at the beginning of node list when component changes or when at end of nodes
            if (sortedNodesIter == null || searcherState.isExecCompDifferentFromPrior() || !sortedNodesIter.hasNext()) {
                sortedNodesIter = sortedNodes.iterator();
                nodeSortCnt++;
            }

            while (sortedNodesIter.hasNext()) {
                String nodeId = sortedNodesIter.next();
                RasNode node = nodes.getNodeById(nodeId);
                if (!node.couldEverFit(exec, topologyDetails)) {
                    continue;
                }
                for (WorkerSlot workerSlot : node.getSlotsAvailableToScheduleOn()) {
                    if (!isExecAssignmentToWorkerValid(exec, workerSlot)) {
                        // exec can't fit in this workerSlot, try next workerSlot
                        LOG.trace("Failed to assign exec={}, comp={}, topo={} to worker={} on node=({}, availCpu={}, availMem={}).",
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
                        LOG.info("scheduleExecutorsOnNodes: Done at loopCnt={} in {}ms, state.elapsedtime={}, topo={}, nodeSortCnt={}",
                                loopCnt, Time.currentTimeMillis() - startTimeMilli,
                                Time.currentTimeMillis() - searcherState.getStartTimeMillis(),
                                topoName, nodeSortCnt);
                        return searcherState.createSchedulingResult(true, this.getClass().getSimpleName());
                    }
                    searcherState = searcherState.nextExecutor();
                    LOG.debug("scheduleExecutorsOnNodes: Assigned execId={}, comp={} to node={}/cpu={}/mem={}, "
                            + "worker-port={} at loopCnt={}, topo={}, nodeSortCnt={}",
                        execIndex, comp, nodeId, node.getAvailableCpuResources(), node.getAvailableMemoryResources(),
                        workerSlot.getPort(), loopCnt, topoName, nodeSortCnt);
                    continue OUTERMOST_LOOP;
                }
            }
            // if here, then the executor was not assigned, scheduling failed
            LOG.debug("scheduleExecutorsOnNodes: Failed to schedule execId={}, comp={} at loopCnt={}, topo={}, nodeSortCnt={}",
                    execIndex, comp, loopCnt, topoName, nodeSortCnt);
            break;
        }
        boolean success = searcherState.areAllExecsScheduled();
        LOG.info("scheduleExecutorsOnNodes: Scheduled={} in {} milliseconds, state.elapsedtime={}, topo={}, nodeSortCnt={}",
                success, Time.currentTimeMillis() - startTimeMilli, Time.currentTimeMillis() - searcherState.getStartTimeMillis(),
                topoName, nodeSortCnt);
        return searcherState.createSchedulingResult(success, this.getClass().getSimpleName());
    }
}
