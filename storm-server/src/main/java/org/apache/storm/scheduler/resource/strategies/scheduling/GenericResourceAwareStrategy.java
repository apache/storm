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
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import org.apache.storm.Config;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.Component;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.SchedulingStatus;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericResourceAwareStrategy extends BaseResourceAwareStrategy implements IStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(GenericResourceAwareStrategy.class);

    /**
     * Implementation of the sortObjectResources method so other strategies can reuse it.
     */
    public static TreeSet<ObjectResources> sortObjectResourcesImpl(
        final AllResources allResources, ExecutorDetails exec, TopologyDetails topologyDetails,
        final ExistingScheduleFunc existingScheduleFunc) {
        AllResources affinityBasedAllResources = new AllResources(allResources);
        NormalizedResourceRequest requestedResources = topologyDetails.getTotalResources(exec);
        for (ObjectResources objectResources : affinityBasedAllResources.objectResources) {
            objectResources.availableResources.updateForRareResourceAffinity(requestedResources);
        }

        TreeSet<ObjectResources> sortedObjectResources =
            new TreeSet<>((o1, o2) -> {
                int execsScheduled1 = existingScheduleFunc.getNumExistingSchedule(o1.id);
                int execsScheduled2 = existingScheduleFunc.getNumExistingSchedule(o2.id);
                if (execsScheduled1 > execsScheduled2) {
                    return -1;
                } else if (execsScheduled1 < execsScheduled2) {
                    return 1;
                } else {
                    double o1Avg = allResources.availableResourcesOverall.calculateAveragePercentageUsedBy(o1.availableResources);
                    double o2Avg = allResources.availableResourcesOverall.calculateAveragePercentageUsedBy(o2.availableResources);

                    if (o1Avg > o2Avg) {
                        return -1;
                    } else if (o1Avg < o2Avg) {
                        return 1;
                    } else {
                        return o1.id.compareTo(o2.id);
                    }

                }
            });
        sortedObjectResources.addAll(affinityBasedAllResources.objectResources);
        LOG.debug("Sorted Object Resources: {}", sortedObjectResources);
        return sortedObjectResources;
    }

    @Override
    public SchedulingResult schedule(Cluster cluster, TopologyDetails td) {
        prepare(cluster);
        if (nodes.getNodes().size() <= 0) {
            LOG.warn("No available nodes to schedule tasks on!");
            return SchedulingResult.failure(
                SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES, "No available nodes to schedule tasks on!");
        }
        Collection<ExecutorDetails> unassignedExecutors =
            new HashSet<>(this.cluster.getUnassignedExecutors(td));
        LOG.debug("Topology: {} has {} executors which need scheduling.",
                    td.getId(), unassignedExecutors.size());

        Collection<ExecutorDetails> scheduledTasks = new ArrayList<>();
        List<Component> spouts = this.getSpouts(td);

        if (spouts.size() == 0) {
            LOG.error("Cannot find a Spout!");
            return SchedulingResult.failure(
                SchedulingStatus.FAIL_INVALID_TOPOLOGY, "Cannot find a Spout!");
        }

        //order executors to be scheduled
        List<ExecutorDetails> orderedExecutors = orderExecutors(td, unassignedExecutors);
        Collection<ExecutorDetails> executorsNotScheduled = new HashSet<>(unassignedExecutors);
        List<String> favoredNodeIds = makeHostToNodeIds((List<String>) td.getConf().get(Config.TOPOLOGY_SCHEDULER_FAVORED_NODES));
        List<String> unFavoredNodeIds = makeHostToNodeIds((List<String>) td.getConf().get(Config.TOPOLOGY_SCHEDULER_UNFAVORED_NODES));

        for (ExecutorDetails exec : orderedExecutors) {
            if (Thread.currentThread().isInterrupted()) {
                return null;
            }
            LOG.debug(
                "Attempting to schedule: {} of component {}[ REQ {} ]",
                exec,
                td.getExecutorToComponent().get(exec),
                td.getTaskResourceReqList(exec));
            final Iterable<String> sortedNodes = sortAllNodes(td, exec, favoredNodeIds, unFavoredNodeIds);

            if (!scheduleExecutor(exec, td, scheduledTasks, sortedNodes)) {
                return mkNotEnoughResources(td);
            }
        }

        executorsNotScheduled.removeAll(scheduledTasks);
        if (!executorsNotScheduled.isEmpty()) {
            LOG.debug("Scheduling left over tasks {} (most likely sys tasks) from topology {}",
                        executorsNotScheduled, td.getId());
            // schedule left over system tasks
            for (ExecutorDetails exec : executorsNotScheduled) {
                if (Thread.currentThread().isInterrupted()) {
                    return null;
                }
                final Iterable<String> sortedNodes = sortAllNodes(td, exec, favoredNodeIds, unFavoredNodeIds);
                if (!scheduleExecutor(exec, td, scheduledTasks, sortedNodes)) {
                    return mkNotEnoughResources(td);
                }
            }
            executorsNotScheduled.removeAll(scheduledTasks);
        }

        SchedulingResult result;
        executorsNotScheduled.removeAll(scheduledTasks);
        if (executorsNotScheduled.size() > 0) {
            LOG.error("Not all executors successfully scheduled: {}", executorsNotScheduled);
            result =
                SchedulingResult.failure(
                    SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES,
                    (td.getExecutors().size() - unassignedExecutors.size())
                    + "/"
                    + td.getExecutors().size()
                    + " executors scheduled");
        } else {
            LOG.debug("All resources successfully scheduled!");
            result = SchedulingResult.success("Fully Scheduled by " + this.getClass().getSimpleName());
        }
        return result;
    }

    /**
     * Sort objects by the following two criteria. 1) the number executors of the topology that needs to be scheduled is already on the
     * object (node or rack) in descending order. The reasoning to sort based on criterion 1 is so we schedule the rest of a topology on the
     * same object (node or rack) as the existing executors of the topology. 2) the subordinate/subservient resource availability percentage
     * of a rack in descending order We calculate the resource availability percentage by dividing the resource availability of the object
     * (node or rack) by the resource availability of the entire rack or cluster depending on if object references a node or a rack. How
     * this differs from the DefaultResourceAwareStrategy is that the percentage boosts the node or rack if it is requested by the executor
     * that the sorting is being done for and pulls it down if it is not. By doing this calculation, objects (node or rack) that have
     * exhausted or little of one of the resources mentioned above will be ranked after racks that have more balanced resource availability
     * and nodes or racks that have resources that are not requested will be ranked below . So we will be less likely to pick a rack that
     * have a lot of one resource but a low amount of another and have a lot of resources that are not requested by the executor.
     *
     * @param allResources         contains all individual ObjectResources as well as cumulative stats
     * @param exec                 executor for which the sorting is done
     * @param topologyDetails      topologyDetails for the above executor
     * @param existingScheduleFunc a function to get existing executors already scheduled on this object
     * @return a sorted list of ObjectResources
     */
    @Override
    protected TreeSet<ObjectResources> sortObjectResources(
        final AllResources allResources, ExecutorDetails exec, TopologyDetails topologyDetails,
        final ExistingScheduleFunc existingScheduleFunc) {
        return sortObjectResourcesImpl(allResources, exec, topologyDetails, existingScheduleFunc);
    }
}
