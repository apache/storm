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

package org.apache.storm.utils;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.WorkerResources;

public class EquivalenceUtils {

    /**
     * Decide the equivalence of two local assignments, ignoring the order of executors This is different from #equal method.
     *
     * @param first  Local assignment A
     * @param second Local assignment B
     * @return True if A and B are equivalent, ignoring the order of the executors
     */
    public static boolean areLocalAssignmentsEquivalent(LocalAssignment first, LocalAssignment second) {
        if (first == null && second == null) {
            return true;
        }
        if (first != null && second != null) {
            if (first.get_topology_id().equals(second.get_topology_id())) {
                Set<ExecutorInfo> aexec = new HashSet<>(first.get_executors());
                Set<ExecutorInfo> bexec = new HashSet<>(second.get_executors());
                if (aexec.equals(bexec)) {
                    boolean firstHasResources = first.is_set_resources();
                    boolean secondHasResources = second.is_set_resources();
                    if (!firstHasResources && !secondHasResources) {
                        return true;
                    }
                    if (firstHasResources && secondHasResources) {
                        WorkerResources firstResources = first.get_resources();
                        WorkerResources secondResources = second.get_resources();
                        return customWorkerResourcesEquality(firstResources, secondResources);
                    }
                }
            }
        }
        return false;
    }

    /**
     * This method compares WorkerResources while considering any resources are NULL to be 0.0
     *
     * @param first  WorkerResources A
     * @param second WorkerResources B
     * @return True if A and B are equivalent, treating the absent resources as 0.0
     */
    @VisibleForTesting
    static boolean customWorkerResourcesEquality(WorkerResources first, WorkerResources second) {
        if (first == null) {
            return false;
        }
        if (second == null) {
            return false;
        }
        if (first == second) {
            return true;
        }
        if (first.equals(second)) {
            return true;
        }

        if (first.get_cpu() != second.get_cpu()) {
            return false;
        }
        if (first.get_mem_on_heap() != second.get_mem_on_heap()) {
            return false;
        }
        if (first.get_mem_off_heap() != second.get_mem_off_heap()) {
            return false;
        }
        if (first.get_shared_mem_off_heap() != second.get_shared_mem_off_heap()) {
            return false;
        }
        if (first.get_shared_mem_on_heap() != second.get_shared_mem_on_heap()) {
            return false;
        }
        if (!customResourceMapEquality(first.get_resources(), second.get_resources())) {
            return false;
        }
        if (!customResourceMapEquality(first.get_shared_resources(), second.get_shared_resources())) {
            return false;
        }
        return true;
    }

    /**
     * This method compares Resource Maps while considering any resources are NULL to be 0.0
     *
     * @param firstMap  Resource Map A
     * @param secondMap Resource Map B
     * @return True if A and B are equivalent, treating the absent resources as 0.0
     */
    private static boolean customResourceMapEquality(Map<String, Double> firstMap, Map<String, Double> secondMap) {
        if (firstMap == null && secondMap == null) {
            return true;
        }
        if (firstMap == null) {
            firstMap = new HashMap<>();
        }
        if (secondMap == null) {
            secondMap = new HashMap<>();
        }

        Set<String> keys = new HashSet<>(firstMap.keySet());
        keys.addAll(secondMap.keySet());
        for (String key : keys) {
            if (firstMap.getOrDefault(key, 0.0).doubleValue() != secondMap.getOrDefault(key, 0.0).doubleValue()) {
                return false;
            }
        }
        return true;
    }
}
