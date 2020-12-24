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

package org.apache.storm.scheduler.resource.strategies.scheduling.sorter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.scheduler.Component;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecSorterByProximity implements IExecSorter {
    private static final Logger LOG = LoggerFactory.getLogger(ExecSorterByProximity.class);

    protected TopologyDetails topologyDetails;

    public ExecSorterByProximity(TopologyDetails topologyDetails) {
        this.topologyDetails = topologyDetails;
    }

    /**
     * Order executors by network proximity needs. First add all executors for components that
     * are in topological sorted order. Then add back executors not accounted for - which are
     * system executors.
     *
     * @param unassignedExecutors an unmodifiable set of executors that need to be scheduled.
     * @return a list of executors in sorted order for scheduling.
     */
    public List<ExecutorDetails> sortExecutors(Set<ExecutorDetails> unassignedExecutors) {
        Map<String, Component> componentMap = topologyDetails.getUserTopolgyComponents(); // excludes system components
        LinkedHashSet<ExecutorDetails> orderedExecutorSet = new LinkedHashSet<>(); // in insert order

        Map<String, Queue<ExecutorDetails>> compToExecsToSchedule = new HashMap<>();
        for (Component component : componentMap.values()) {
            compToExecsToSchedule.put(component.getId(), new LinkedList<>());
            for (ExecutorDetails exec : component.getExecs()) {
                if (unassignedExecutors.contains(exec)) {
                    compToExecsToSchedule.get(component.getId()).add(exec);
                }
            }
        }

        List<Component> sortedComponents = topologicalSortComponents(componentMap);

        for (Component currComp: sortedComponents) {
            int numExecs = compToExecsToSchedule.get(currComp.getId()).size();
            for (int i = 0; i < numExecs; i++) {
                orderedExecutorSet.addAll(takeExecutors(currComp, componentMap, compToExecsToSchedule));
            }
        }

        // add executors not in sorted list - which may be system executors
        orderedExecutorSet.addAll(unassignedExecutors);
        return new LinkedList<>(orderedExecutorSet);
    }

    /**
     * Sort components topologically.
     * @param componentMap The map of component Id to Component Object.
     * @return The sorted components
     */
    private List<Component> topologicalSortComponents(final Map<String, Component> componentMap) {
        LinkedHashSet<Component> sortedComponentsSet = new LinkedHashSet<>();
        boolean[] visited = new boolean[componentMap.size()];
        int[] inDegree = new int[componentMap.size()];
        List<String> componentIds = new ArrayList<>(componentMap.keySet());
        Map<String, Integer> compIdToIndex = new HashMap<>();
        for (int i = 0; i < componentIds.size(); i++) {
            compIdToIndex.put(componentIds.get(i), i);
        }
        //initialize the in-degree array
        for (int i = 0; i < inDegree.length; i++) {
            String compId = componentIds.get(i);
            Component comp = componentMap.get(compId);
            for (String childId : comp.getChildren()) {
                inDegree[compIdToIndex.get(childId)] += 1;
            }
        }
        //sorting components topologically
        for (int t = 0; t < inDegree.length; t++) {
            for (int i = 0; i < inDegree.length; i++) {
                if (inDegree[i] == 0 && !visited[i]) {
                    String compId = componentIds.get(i);
                    Component comp = componentMap.get(compId);
                    sortedComponentsSet.add(comp);
                    visited[i] = true;
                    for (String childId : comp.getChildren()) {
                        inDegree[compIdToIndex.get(childId)]--;
                    }
                    break;
                }
            }
        }
        // add back components that could not be visited and issue warning about loop in component data flow
        if (sortedComponentsSet.size() != componentMap.size()) {
            String unvisitedComponentIds = componentMap.entrySet().stream()
                    .filter(x -> !sortedComponentsSet.contains(x.getValue()))
                    .map(x -> x.getKey())
                    .collect(Collectors.joining(","));
            LOG.warn("topologicalSortComponents for topology {} detected possible loop(s) involving components {}, "
                            + "appending them to the end of the sorted component list",
                    topologyDetails.getId(), unvisitedComponentIds);
            sortedComponentsSet.addAll(componentMap.values());
        }
        return new ArrayList<>(sortedComponentsSet);
    }

    /**
     * Take unscheduled executors from current and all its downstream components in a particular order.
     * First, take one executor from the current component;
     * then for every child (direct downstream component) of this component,
     *     if it's shuffle grouping from the current component to this child,
     *         the number of executors to take from this child is the max of
     *         1 and (the number of unscheduled executors this child has / the number of unscheduled executors the current component has);
     *     otherwise, the number of executors to take is 1;
     *     for every executor to take from this child, call takeExecutors(...).
     * @param currComp The current component.
     * @param componentMap The map from component Id to component object.
     * @param compToExecsToSchedule The map from component Id to unscheduled executors.
     * @return The executors to schedule in order.
     */
    private List<ExecutorDetails> takeExecutors(Component currComp,
                                                final Map<String, Component> componentMap,
                                                final Map<String, Queue<ExecutorDetails>> compToExecsToSchedule) {
        List<ExecutorDetails> execsScheduled = new ArrayList<>();
        Queue<ExecutorDetails> currQueue = compToExecsToSchedule.get(currComp.getId());
        int currUnscheduledNumExecs = currQueue.size();
        //Just for defensive programming as this won't actually happen.
        if (currUnscheduledNumExecs == 0) {
            return execsScheduled;
        }
        execsScheduled.add(currQueue.poll());
        Set<String> sortedChildren = getSortedChildren(currComp, componentMap);
        for (String childId: sortedChildren) {
            Component childComponent = componentMap.get(childId);
            Queue<ExecutorDetails> childQueue = compToExecsToSchedule.get(childId);
            int childUnscheduledNumExecs = childQueue.size();
            if (childUnscheduledNumExecs == 0) {
                continue;
            }
            int numExecsToTake = 1;
            if (hasShuffleGroupingFromParentToChild(currComp, childComponent)) {
                // if it's shuffle grouping, truncate
                numExecsToTake = Math.max(1, childUnscheduledNumExecs / currUnscheduledNumExecs);
            } // otherwise, one-by-one
            for (int i = 0; i < numExecsToTake; i++) {
                execsScheduled.addAll(takeExecutors(childComponent, componentMap, compToExecsToSchedule));
            }
        }
        return execsScheduled;
    }

    private Set<String> getSortedChildren(Component component, final Map<String, Component> componentMap) {
        Set<String> children = component.getChildren();
        Set<String> sortedChildren =
            new TreeSet<>((o1, o2) -> {
                Component child1 = componentMap.get(o1);
                Component child2 = componentMap.get(o2);
                boolean child1IsShuffle = hasShuffleGroupingFromParentToChild(component, child1);
                boolean child2IsShuffle = hasShuffleGroupingFromParentToChild(component, child2);
                if (child1IsShuffle && child2IsShuffle) {
                    return o1.compareTo(o2);
                } else if (child1IsShuffle) {
                    return 1;
                } else {
                    return -1;
                }
            });
        sortedChildren.addAll(children);
        return sortedChildren;
    }

    private boolean hasShuffleGroupingFromParentToChild(Component parent, Component child) {
        for (Map.Entry<GlobalStreamId, Grouping> inputEntry: child.getInputs().entrySet()) {
            GlobalStreamId globalStreamId = inputEntry.getKey();
            Grouping grouping = inputEntry.getValue();
            if (globalStreamId.get_componentId().equals(parent.getId())
                && (inputEntry.getValue().is_set_local_or_shuffle() || grouping.is_set_shuffle())) {
                return true;
            }
        }
        return false;
    }
}
