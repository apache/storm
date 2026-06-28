/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.storm.Constants;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.blacklist.TestUtilsForBlacklistScheduler;
import org.apache.storm.scheduler.resource.normalization.ResourceMetrics;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link IsolationScheduler}.
 */
public class IsolationSchedulerTest {

    private static SupervisorDetails mkSupervisor(String id, String host, int numPorts) {
        List<Number> ports = new ArrayList<>();
        for (int i = 0; i < numPorts; i++) {
            ports.add(i);
        }
        Map<String, Double> resources = new HashMap<>();
        resources.put(Constants.COMMON_CPU_RESOURCE_NAME, 400.0);
        resources.put(Constants.COMMON_TOTAL_MEMORY_RESOURCE_NAME, 4096.0);
        return new SupervisorDetails(id, host, null, ports, resources);
    }

    private static Cluster mkCluster(Map<String, SupervisorDetails> supervisors, Topologies topologies) {
        INimbus iNimbus = new TestUtilsForBlacklistScheduler.INimbusTest();
        ResourceMetrics resourceMetrics = new ResourceMetrics(new StormMetricsRegistry());
        return new Cluster(iNimbus, resourceMetrics, supervisors, new HashMap<String, SchedulerAssignmentImpl>(),
                           topologies, new HashMap<String, Object>());
    }

    private static List<String> hostOrder(LinkedList<IsolationScheduler.HostAssignableSlots> slots) {
        List<String> hosts = new ArrayList<>();
        for (IsolationScheduler.HostAssignableSlots slot : slots) {
            hosts.add(slot.getHostName());
        }
        return hosts;
    }

    @Test
    public void hostAssignableSlots_prefersHostWithMoreFreeSlots() {
        Map<String, SupervisorDetails> supervisors = new HashMap<>();
        supervisors.put("sup-busy", mkSupervisor("sup-busy", "host-busy", 2));
        supervisors.put("sup-free", mkSupervisor("sup-free", "host-free", 2));

        Map<String, Object> conf = new HashMap<>();
        TopologyDetails filler = TestUtilsForBlacklistScheduler.getTopology("filler", conf, 1, 0, 1, 0, 0, false);
        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(filler.getId(), filler);
        Topologies topologies = new Topologies(topoMap);

        Cluster cluster = mkCluster(supervisors, topologies);
        cluster.assign(new WorkerSlot("sup-busy", 0), filler.getId(),
                       Collections.singletonList(filler.getExecutors().iterator().next()));

        LinkedList<IsolationScheduler.HostAssignableSlots> ranked =
            new IsolationScheduler().hostAssignableSlots(cluster);

        assertEquals(2, ranked.size());
        assertEquals("host-free", ranked.get(0).getHostName());
        assertEquals(2, ranked.get(0).getFreeSlots());
        assertEquals("host-busy", ranked.get(1).getHostName());
        assertEquals(1, ranked.get(1).getFreeSlots());
        assertEquals(hostOrder(ranked), List.of("host-free", "host-busy"));
    }

    @Test
    public void hostAssignableSlots_breaksTiesByHostName() {
        Map<String, SupervisorDetails> supervisors = new HashMap<>();
        supervisors.put("sup-a", mkSupervisor("sup-a", "host-aaa", 2));
        supervisors.put("sup-b", mkSupervisor("sup-b", "host-bbb", 2));

        Cluster cluster = mkCluster(supervisors, new Topologies());

        LinkedList<IsolationScheduler.HostAssignableSlots> ranked =
            new IsolationScheduler().hostAssignableSlots(cluster);

        assertEquals(2, ranked.size());
        assertEquals(2, ranked.get(0).getWorkerSlots().size());
        assertEquals(2, ranked.get(1).getWorkerSlots().size());
        assertEquals(2, ranked.get(0).getFreeSlots());
        assertEquals(2, ranked.get(1).getFreeSlots());
        assertEquals(hostOrder(ranked), List.of("host-aaa", "host-bbb"));
    }
}