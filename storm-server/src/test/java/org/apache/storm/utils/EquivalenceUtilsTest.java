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

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.WorkerResources;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EquivalenceUtilsTest {

    static WorkerResources mkWorkerResources(Double cpu, Double mem_on_heap, Double mem_off_heap, Map<String, Double> resources) {
        WorkerResources workerResources = mkWorkerResources(cpu, mem_on_heap, mem_off_heap);
        if (resources != null) {
            workerResources.set_resources(resources);
        }
        return workerResources;
    }

    static WorkerResources mkWorkerResources(Double cpu, Double mem_on_heap, Double mem_off_heap) {
        WorkerResources resources = new WorkerResources();
        if (cpu != null) {
            resources.set_cpu(cpu);
        }

        if (mem_on_heap != null) {
            resources.set_mem_on_heap(mem_on_heap);
        }

        if (mem_off_heap != null) {
            resources.set_mem_off_heap(mem_off_heap);
        }
        return resources;
    }

    static LocalAssignment mkLocalAssignment(String id, List<ExecutorInfo> exec, WorkerResources resources) {
        LocalAssignment ret = new LocalAssignment();
        ret.set_topology_id(id);
        ret.set_executors(exec);
        if (resources != null) {
            ret.set_resources(resources);
        }
        return ret;
    }

    static List<ExecutorInfo> mkExecutorInfoList(int... executors) {
        ArrayList<ExecutorInfo> ret = new ArrayList<>(executors.length);
        for (int exec : executors) {
            ExecutorInfo execInfo = new ExecutorInfo();
            execInfo.set_task_start(exec);
            execInfo.set_task_end(exec);
            ret.add(execInfo);
        }
        return ret;
    }

    @Test
    public void testWorkerResourceEquality() {
        WorkerResources resourcesRNull = mkWorkerResources(100.0, 100.0, 100.0, null);
        WorkerResources resourcesREmpty = mkWorkerResources(100.0, 100.0, 100.0, Maps.newHashMap());
        assertTrue(EquivalenceUtils.customWorkerResourcesEquality(resourcesRNull,resourcesREmpty));

        Map resources = new HashMap<String, Double>();
        resources.put("network.resource.units", 0.0);
        WorkerResources resourcesRNetwork = mkWorkerResources(100.0, 100.0, 100.0,resources);
        assertTrue(EquivalenceUtils.customWorkerResourcesEquality(resourcesREmpty, resourcesRNetwork));


        Map resourcesNetwork = new HashMap<String, Double>();
        resourcesNetwork.put("network.resource.units", 50.0);
        WorkerResources resourcesRNetworkNonZero = mkWorkerResources(100.0, 100.0, 100.0,resourcesNetwork);
        assertFalse(EquivalenceUtils.customWorkerResourcesEquality(resourcesREmpty, resourcesRNetworkNonZero));

        Map resourcesNetworkOne = new HashMap<String, Double>();
        resourcesNetworkOne.put("network.resource.units", 50.0);
        WorkerResources resourcesRNetworkOne = mkWorkerResources(100.0, 100.0, 100.0,resourcesNetworkOne);
        assertTrue(EquivalenceUtils.customWorkerResourcesEquality(resourcesRNetworkOne, resourcesRNetworkNonZero));

        Map resourcesNetworkTwo = new HashMap<String, Double>();
        resourcesNetworkTwo.put("network.resource.units", 100.0);
        WorkerResources resourcesRNetworkTwo = mkWorkerResources(100.0, 100.0, 100.0,resourcesNetworkTwo);
        assertFalse(EquivalenceUtils.customWorkerResourcesEquality(resourcesRNetworkOne, resourcesRNetworkTwo));

        WorkerResources resourcesCpuNull = mkWorkerResources(null, 100.0,100.0);
        WorkerResources resourcesCPUZero = mkWorkerResources(0.0, 100.0,100.0);
        assertTrue(EquivalenceUtils.customWorkerResourcesEquality(resourcesCpuNull, resourcesCPUZero));

        WorkerResources resourcesOnHeapMemNull = mkWorkerResources(100.0, null,100.0);
        WorkerResources resourcesOnHeapMemZero = mkWorkerResources(100.0, 0.0,100.0);
        assertTrue(EquivalenceUtils.customWorkerResourcesEquality(resourcesOnHeapMemNull, resourcesOnHeapMemZero));

        WorkerResources resourcesOffHeapMemNull = mkWorkerResources(100.0, 100.0,null);
        WorkerResources resourcesOffHeapMemZero = mkWorkerResources(100.0, 100.0,0.0);
        assertTrue(EquivalenceUtils.customWorkerResourcesEquality(resourcesOffHeapMemNull, resourcesOffHeapMemZero));

        assertFalse(EquivalenceUtils.customWorkerResourcesEquality(resourcesOffHeapMemNull, null));
    }

    @Test
    public void testEquivalent() {
        LocalAssignment a = mkLocalAssignment("A", mkExecutorInfoList(1, 2, 3, 4, 5), mkWorkerResources(100.0, 100.0, 100.0));
        LocalAssignment aResized = mkLocalAssignment("A", mkExecutorInfoList(1, 2, 3, 4, 5), mkWorkerResources(100.0, 200.0, 100.0));
        LocalAssignment b = mkLocalAssignment("B", mkExecutorInfoList(1, 2, 3, 4, 5, 6), mkWorkerResources(100.0, 100.0, 100.0));
        LocalAssignment bReordered = mkLocalAssignment("B", mkExecutorInfoList(6, 5, 4, 3, 2, 1), mkWorkerResources(100.0, 100.0, 100.0));

        LocalAssignment c = mkLocalAssignment("C", mkExecutorInfoList(188, 261),mkWorkerResources(400.0,10000.0,0.0));

        WorkerResources workerResources = mkWorkerResources(400.0, 10000.0, 0.0);
        Map<String, Double> additionalResources = workerResources.get_resources();
        if( additionalResources == null) additionalResources = new HashMap<>();
        additionalResources.put("network.resource.units", 0.0);

        workerResources.set_resources(additionalResources);
        LocalAssignment cReordered = mkLocalAssignment("C", mkExecutorInfoList(188, 261), workerResources);

        assertTrue(EquivalenceUtils.areLocalAssignmentsEquivalent(c,cReordered));
        assertTrue(EquivalenceUtils.areLocalAssignmentsEquivalent(null, null));
        assertTrue(EquivalenceUtils.areLocalAssignmentsEquivalent(a, a));
        assertTrue(EquivalenceUtils.areLocalAssignmentsEquivalent(b, bReordered));
        assertTrue(EquivalenceUtils.areLocalAssignmentsEquivalent(bReordered, b));

        assertFalse(EquivalenceUtils.areLocalAssignmentsEquivalent(a, aResized));
        assertFalse(EquivalenceUtils.areLocalAssignmentsEquivalent(aResized, a));
        assertFalse(EquivalenceUtils.areLocalAssignmentsEquivalent(a, null));
        assertFalse(EquivalenceUtils.areLocalAssignmentsEquivalent(null, b));
        assertFalse(EquivalenceUtils.areLocalAssignmentsEquivalent(a, b));
    }
}
