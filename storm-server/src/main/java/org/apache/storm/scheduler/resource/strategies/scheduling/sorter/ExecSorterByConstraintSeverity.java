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

package org.apache.storm.scheduler.resource.strategies.scheduling.sorter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.strategies.scheduling.ConstraintSolverConfig;

public class ExecSorterByConstraintSeverity implements IExecSorter {
    private final ConstraintSolverConfig constraintSolverConfig;
    private final Map<String, Set<ExecutorDetails>> compToExecs;

    public ExecSorterByConstraintSeverity(Cluster cluster, TopologyDetails topologyDetails) {
        this.constraintSolverConfig = new ConstraintSolverConfig(topologyDetails);
        this.compToExecs = new HashMap<>();
        topologyDetails.getExecutorToComponent()
                .forEach((exec, comp) -> compToExecs.computeIfAbsent(comp, (k) -> new HashSet<>()).add(exec));
    }

    @Override
    public List<ExecutorDetails> sortExecutors(Set<ExecutorDetails> unassignedExecutors) {
        //get unassigned executors sorted based on number of constraints
        List<ExecutorDetails> sortedExecs = getSortedExecs()
                .stream()
                .filter(unassignedExecutors::contains)
                .collect(Collectors.toList());
        return sortedExecs;
    }

    /**
     * Sort executors such that components with more constraints are first. A component is more constrained if it
     * has a higher number of incompatible components and/or it allows lesser instances on a node.
     *
     * @return a list of executors sorted constrained components first.
     */
    private ArrayList<ExecutorDetails> getSortedExecs() {
        ArrayList<ExecutorDetails> retList = new ArrayList<>();

        //find number of constraints per component
        //Key->Comp Value-># of constraints
        Map<String, Double> compConstraintCountMap = new HashMap<>();
        constraintSolverConfig.getIncompatibleComponentSets().forEach((comp, incompatibleComponents) -> {
            double constraintCnt = incompatibleComponents.size();
            // check if component is declared for spreading
            if (constraintSolverConfig.getMaxNodeCoLocationCnts().containsKey(comp)) {
                // lower (1 and above only) value is most constrained should have higher count
                constraintCnt += (compToExecs.size() / constraintSolverConfig.getMaxNodeCoLocationCnts().get(comp));
            }
            compConstraintCountMap.put(comp, constraintCnt); // higher count sorts to the front
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
    protected <K extends Comparable<K>, V extends Comparable<V>> NavigableMap<K, V> sortByValues(final Map<K, V> map) {
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


