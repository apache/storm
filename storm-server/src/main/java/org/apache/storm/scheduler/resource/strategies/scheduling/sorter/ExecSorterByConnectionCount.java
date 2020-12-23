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

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

import org.apache.storm.scheduler.Component;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.shade.com.google.common.collect.Sets;

public class ExecSorterByConnectionCount implements IExecSorter {

    protected TopologyDetails topologyDetails;

    public ExecSorterByConnectionCount(TopologyDetails topologyDetails) {
        this.topologyDetails = topologyDetails;
    }

    /**
     * Order executors based on how many in and out connections it will potentially need to make, in descending order. First order
     * components by the number of in and out connections it will have.  Then iterate through the sorted list of components. For each
     * component sort the neighbors of that component by how many connections it will have to make with that component.
     * Add an executor from this component and then from each neighboring component in sorted order. Do this until there is
     * nothing left to schedule. Then add back executors not accounted for - which are system executors.
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

        Set<Component> sortedComponents = sortComponents(componentMap);
        sortedComponents.addAll(componentMap.values());

        for (Component currComp : sortedComponents) {
            Map<String, Component> neighbors = new HashMap<>();
            for (String compId : Sets.union(currComp.getChildren(), currComp.getParents())) {
                neighbors.put(compId, componentMap.get(compId));
            }
            Set<Component> sortedNeighbors = sortNeighbors(currComp, neighbors);
            Queue<ExecutorDetails> currCompExecsToSched = compToExecsToSchedule.get(currComp.getId());

            boolean flag;
            do {
                flag = false;
                if (!currCompExecsToSched.isEmpty()) {
                    orderedExecutorSet.add(currCompExecsToSched.poll());
                    flag = true;
                }

                for (Component neighborComp : sortedNeighbors) {
                    Queue<ExecutorDetails> neighborCompExesToSched = compToExecsToSchedule.get(neighborComp.getId());
                    if (!neighborCompExesToSched.isEmpty()) {
                        orderedExecutorSet.add(neighborCompExesToSched.poll());
                        flag = true;
                    }
                }
            } while (flag);
        }

        // add executors not in sorted list - which may be system executors
        orderedExecutorSet.addAll(unassignedExecutors);
        return new LinkedList<>(orderedExecutorSet);
    }

    /**
     * sort components by the number of in and out connections that need to be made, in descending order.
     *
     * @param componentMap The components that need to be sorted
     * @return a sorted set of components
     */
    private Set<Component> sortComponents(final Map<String, Component> componentMap) {
        Set<Component> sortedComponents =
            new TreeSet<>((o1, o2) -> {
                int connections1 = 0;
                int connections2 = 0;

                for (String childId : Sets.union(o1.getChildren(), o1.getParents())) {
                    connections1 +=
                        (componentMap.get(childId).getExecs().size() * o1.getExecs().size());
                }

                for (String childId : Sets.union(o2.getChildren(), o2.getParents())) {
                    connections2 +=
                        (componentMap.get(childId).getExecs().size() * o2.getExecs().size());
                }

                if (connections1 > connections2) {
                    return -1;
                } else if (connections1 < connections2) {
                    return 1;
                } else {
                    return o1.getId().compareTo(o2.getId());
                }
            });
        sortedComponents.addAll(componentMap.values());
        return sortedComponents;
    }

    /**
     * Sort a component's neighbors by the number of connections it needs to make with this component.
     *
     * @param thisComp     the component that we need to sort its neighbors
     * @param componentMap all the components to sort
     * @return a sorted set of components
     */
    private Set<Component> sortNeighbors(
            final Component thisComp, final Map<String, Component> componentMap) {
        Set<Component> sortedComponents =
                new TreeSet<>((o1, o2) -> {
                    int connections1 = o1.getExecs().size() * thisComp.getExecs().size();
                    int connections2 = o2.getExecs().size() * thisComp.getExecs().size();
                    if (connections1 < connections2) {
                        return -1;
                    } else if (connections1 > connections2) {
                        return 1;
                    } else {
                        return o1.getId().compareTo(o2.getId());
                    }
                });
        sortedComponents.addAll(componentMap.values());
        return sortedComponents;
    }

}
