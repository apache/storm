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

package org.apache.storm.stats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.ComponentAggregateStats;
import org.apache.storm.generated.ComponentType;
import org.apache.storm.generated.ErrorInfo;
import org.apache.storm.generated.TopologyPageInfo;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.generated.WorkerSummary;
import org.apache.storm.scheduler.WorkerSlot;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestStatsUtil {

    /*
     * aggWorkerStats tests
     */
    private Map<Integer, String> task2Component = new HashMap<Integer, String>();
    private Map<List<Integer>, Map<String, Object>> beats = new HashMap<List<Integer>, Map<String, Object>>();
    private Map<List<Long>, List<Object>> exec2NodePort = new HashMap<List<Long>, List<Object>>();
    private Map<String, String> nodeHost = new HashMap<String, String>();
    private Map<WorkerSlot, WorkerResources> worker2Resources = new HashMap<WorkerSlot, WorkerResources>();

    private List<Long> makeExecutorId(int firstTask, int lastTask) {
        return Arrays.asList(new Long(firstTask), new Long(lastTask));
    }

    public void makeTopoInfo() {
        List<Object> hostPort = new ArrayList<Object>();
        hostPort.add("node1");
        hostPort.add(new Long(1));

        exec2NodePort.put(makeExecutorId(1, 1), hostPort);

        nodeHost.put("node1", "host1");
        nodeHost.put("node2", "host2");
        nodeHost.put("node3", "host3");

        List<Integer> exec1 = new ArrayList<Integer>();
        exec1.add(1);
        exec1.add(1);
        HashMap<String, Object> exec1Beat = new HashMap<String, Object>();
        exec1Beat.put("uptime", 100);
        exec1Beat.put("type", "bolt");
        exec1Beat.put("stats", createBeatBoltStats());

        // should not be returned since this executor is not part of the topology's assignment
        List<Integer> exec2 = new ArrayList<Integer>();
        exec2.add(2);
        exec2.add(4);
        HashMap<String, Object> exec2Beat = new HashMap<String, Object>();
        exec2Beat.put("uptime", 200);
        exec2Beat.put("type", "bolt");
        exec2Beat.put("stats", createBeatBoltStats());

        beats.put(exec1, exec1Beat);
        beats.put(exec2, exec2Beat);

        task2Component.put(1, "my-component");
        task2Component.put(2, "__sys1");
        task2Component.put(3, "__sys2");
        task2Component.put(4, "__sys3");
        task2Component.put(5, "__sys4");
        task2Component.put(6, "__sys4");
        task2Component.put(7, "my-component2");

        WorkerResources ws1 = new WorkerResources();
        ws1.set_mem_on_heap(1);
        ws1.set_mem_off_heap(2);
        ws1.set_cpu(3);
        worker2Resources.put(new WorkerSlot("node1", 1), ws1);

        WorkerResources ws2 = new WorkerResources();
        ws2.set_mem_on_heap(4);
        ws2.set_mem_off_heap(8);
        ws2.set_cpu(12);
        worker2Resources.put(new WorkerSlot("node2", 2), ws2);

        WorkerResources ws3 = new WorkerResources();
        ws3.set_mem_on_heap(16);
        ws3.set_mem_off_heap(32);
        ws3.set_cpu(48);
        worker2Resources.put(new WorkerSlot("node3", 3), ws3);
    }

    /**
     * Utility method for creating a template for Bolt stats.
     * @return Empty template map for Bolt statistics.
     */
    private Map<String, Object> createBeatBoltStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("type", "bolt");

        stats.put("acked", new HashMap<>());
        stats.put("emitted", new HashMap<>());
        stats.put("executed", new HashMap<>());
        stats.put("failed", new HashMap<>());
        stats.put("transferred", new HashMap<>());

        stats.put("execute-latencies", new HashMap<>());
        stats.put("process-latencies", new HashMap<>());

        stats.put("rate", 0D);

        return stats;
    }

    /**
     * Utility method for creating a template for Spout stats.
     * @return Empty template map for Spout statistics.
     */
    private Map<String, Object> createBeatSpoutStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("type", "spout");

        stats.put("acked", new HashMap<>());
        stats.put("emitted", new HashMap<>());
        stats.put("failed", new HashMap<>());
        stats.put("transferred", new HashMap<>());

        stats.put("complete-latencies", new HashMap<>());
        stats.put("rate", 0D);

        return stats;
    }

    private void makeTopoInfoWithSysWorker() {
        makeTopoInfo();

        List<Object> secondWorker = new ArrayList<Object>();
        secondWorker.add("node2");
        secondWorker.add(new Long(2));
        exec2NodePort.put(makeExecutorId(2, 4), secondWorker);
    }

    private void makeTopoInfoWithMissingBeats() {
        makeTopoInfo();

        List<Object> thirdWorker = new ArrayList<Object>();
        thirdWorker.add("node3");
        thirdWorker.add(new Long(3));
        exec2NodePort.put(makeExecutorId(5, 7), thirdWorker);
    }

    /**
     * Utility method for creating dependencies for TopologyInfo
     * that include one spout component.
     */
    private void makeTopoInfoWithSpout() {
        makeTopoInfo();

        // Add spout
        task2Component.put(8, "my-spout");

        HashMap<String, Object> exec3Beat = new HashMap<String, Object>();
        exec3Beat.put("uptime", 300);
        exec3Beat.put("type", "spout");
        exec3Beat.put("stats", createBeatSpoutStats());

        List<Integer> exec3 = new ArrayList<Integer>();
        exec3.add(8);
        exec3.add(8);

        List<Object> hostPort = new ArrayList<Object>();
        hostPort.add("node4");
        hostPort.add(new Long(4));

        exec2NodePort.put(makeExecutorId(8, 8), hostPort);

        beats.put(exec3, exec3Beat);
    }

    private List<WorkerSummary> checkWorkerStats(boolean includeSys, boolean userAuthorized, String filterSupervisor) {
        List<WorkerSummary> summaries =
            StatsUtil.aggWorkerStats("my-storm-id", "my-storm-name",
                                     task2Component, beats, exec2NodePort, nodeHost, worker2Resources,
                                     includeSys, userAuthorized, filterSupervisor, null);
        for (WorkerSummary ws : summaries) {
            String host = ws.get_host();
            int port = ws.get_port();
            Assert.assertEquals("my-storm-id", ws.get_topology_id());
            Assert.assertEquals("my-storm-name", ws.get_topology_name());
            boolean includeSupervisor = filterSupervisor == null || filterSupervisor.equals(host);
            switch (port) {
                case 1:
                    Assert.assertEquals("host1", ws.get_host());
                    Assert.assertEquals("node1", ws.get_supervisor_id());
                    Assert.assertEquals(1, ws.get_num_executors());
                    Assert.assertEquals(100, ws.get_uptime_secs());
                    Assert.assertEquals(1.0, ws.get_assigned_memonheap(), 0.001);
                    Assert.assertEquals(2.0, ws.get_assigned_memoffheap(), 0.001);
                    Assert.assertEquals(3.0, ws.get_assigned_cpu(), 0.001);
                    break;
                case 2:
                    Assert.assertEquals("host2", ws.get_host());
                    Assert.assertEquals("node2", ws.get_supervisor_id());
                    Assert.assertEquals(1, ws.get_num_executors());
                    Assert.assertEquals(200, ws.get_uptime_secs());
                    Assert.assertEquals(4.0, ws.get_assigned_memonheap(), 0.001);
                    Assert.assertEquals(8.0, ws.get_assigned_memoffheap(), 0.001);
                    Assert.assertEquals(12.0, ws.get_assigned_cpu(), 0.001);
                    break;
                case 3:
                    Assert.assertEquals("host3", ws.get_host());
                    Assert.assertEquals("node3", ws.get_supervisor_id());
                    Assert.assertEquals(1, ws.get_num_executors());
                    // no heartbeat for this one, should be 0
                    Assert.assertEquals(0, ws.get_uptime_secs());
                    Assert.assertEquals(16.0, ws.get_assigned_memonheap(), 0.001);
                    Assert.assertEquals(32.0, ws.get_assigned_memoffheap(), 0.001);
                    Assert.assertEquals(48.0, ws.get_assigned_cpu(), 0.001);
                    break;
            }
        }

        // get the worker count back s.t. we can assert in each test function
        return summaries;
    }

    private WorkerSummary getWorkerSummaryForPort(List<WorkerSummary> summaries, int port) {
        //iterate of WorkerSummary and find the one with the port
        for (WorkerSummary ws : summaries) {
            if (ws.get_port() == port) {
                return ws;
            }
        }
        return null;
    }

    @Test
    public void aggWorkerStats() {
        makeTopoInfo();
        List<WorkerSummary> summaries = checkWorkerStats(true /*include sys*/,
                                                         true /*user authorized*/,
                                                         null /*filter supervisor*/);
        WorkerSummary ws = getWorkerSummaryForPort(summaries, 1);
        Assert.assertEquals(1, ws.get_component_to_num_tasks().size());
        Assert.assertEquals(1, ws.get_component_to_num_tasks().get("my-component").intValue());
        Assert.assertEquals(1, summaries.size());
    }

    @Test
    public void aggWorkerStatsWithSystemComponents() {
        makeTopoInfoWithSysWorker();
        List<WorkerSummary> summaries = checkWorkerStats(true /*include sys*/,
                                                         true /*user authorized*/,
                                                         null /*filter supervisor*/);
        WorkerSummary ws = getWorkerSummaryForPort(summaries, 2);
        // since we made sys components visible, the component map has all system components
        Assert.assertEquals(3, ws.get_component_to_num_tasks().size());
        Assert.assertEquals(1, ws.get_component_to_num_tasks().get("__sys1").intValue());
        Assert.assertEquals(1, ws.get_component_to_num_tasks().get("__sys2").intValue());
        Assert.assertEquals(1, ws.get_component_to_num_tasks().get("__sys3").intValue());
        Assert.assertEquals(2, summaries.size());
    }

    @Test
    public void aggWorkerStatsWithHiddenSystemComponents() {
        makeTopoInfoWithSysWorker();
        List<WorkerSummary> summaries = checkWorkerStats(false /*DON'T include sys*/,
                                                         true  /*user authorized*/,
                                                         null  /*filter supervisor*/);
        WorkerSummary ws1 = getWorkerSummaryForPort(summaries, 1);
        WorkerSummary ws2 = getWorkerSummaryForPort(summaries, 2);
        Assert.assertEquals(1, ws1.get_component_to_num_tasks().size());
        // since we made sys components hidden, the component map is empty for this worker
        Assert.assertEquals(0, ws2.get_component_to_num_tasks().size());
        Assert.assertEquals(2, summaries.size());
    }

    @Test
    public void aggWorkerStatsForUnauthorizedUser() {
        makeTopoInfoWithSysWorker();
        List<WorkerSummary> summaries = checkWorkerStats(true  /*include sys (should not matter)*/,
                                                         false /*user NOT authorized*/,
                                                         null  /*filter supervisor*/);
        WorkerSummary ws1 = getWorkerSummaryForPort(summaries, 1);
        WorkerSummary ws2 = getWorkerSummaryForPort(summaries, 2);
        // since we made user not authorized, component map is empty
        Assert.assertEquals(0, ws1.get_component_to_num_tasks().size());
        Assert.assertEquals(0, ws2.get_component_to_num_tasks().size());
        Assert.assertEquals(2, summaries.size());
    }

    @Test
    public void aggWorkerStatsFilterSupervisor() {
        makeTopoInfoWithMissingBeats();
        List<WorkerSummary> summaries = checkWorkerStats(true    /*include sys*/,
                                                         true    /*user authorized*/,
                                                         "node3" /*filter supervisor*/);
        WorkerSummary ws = getWorkerSummaryForPort(summaries, 3);
        // only host3 should be returned given filter
        Assert.assertEquals(2, ws.get_component_to_num_tasks().size());
        Assert.assertEquals(2, ws.get_component_to_num_tasks().get("__sys4").intValue());
        Assert.assertEquals(1, ws.get_component_to_num_tasks().get("my-component2").intValue());
        Assert.assertEquals(1, summaries.size());
    }

    @Test
    public void aggWorkerStatsFilterSupervisorAndHideSystemComponents() {
        makeTopoInfoWithMissingBeats();
        List<WorkerSummary> summaries = checkWorkerStats(false   /*DON'T include sys*/,
                                                         true    /*user authorized*/,
                                                         "node3" /*filter supervisor*/);

        WorkerSummary ws = getWorkerSummaryForPort(summaries, 3);
        // hidden sys component
        Assert.assertEquals(1, ws.get_component_to_num_tasks().size());
        Assert.assertEquals(1, ws.get_component_to_num_tasks().get("my-component2").intValue());
        Assert.assertEquals(1, summaries.size());
    }

    /**
     * Targeted validation against StatsUtil.aggTopoExecsStats()
     * to verify that when a bolt or spout has an error reported,
     * it is included in the returned TopologyPageInfo result.
     */
    @Test
    public void aggTopoExecsStats_boltAndSpoutsHaveLastErrorReported() {
        // Define inputs
        final String expectedBoltErrorMsg = "This is my test bolt error message";
        final int expectedBoltErrorTime = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        final int expectedBoltErrorPort = 4321;
        final String expectedBoltErrorHost = "my.errored.host";

        final String expectedSpoutErrorMsg = "This is my test spout error message";
        final int expectedSpoutErrorTime = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        final int expectedSpoutErrorPort = 1234;
        final String expectedSpoutErrorHost = "my.errored.host2";

        // Define our Last Error for the bolt
        final ErrorInfo expectedBoltLastError = new ErrorInfo(expectedBoltErrorMsg, expectedBoltErrorTime);
        expectedBoltLastError.set_port(expectedBoltErrorPort);
        expectedBoltLastError.set_host(expectedBoltErrorHost);

        // Define our Last Error for the spout
        final ErrorInfo expectedSpoutLastError = new ErrorInfo(expectedSpoutErrorMsg, expectedSpoutErrorTime);
        expectedSpoutLastError.set_port(expectedSpoutErrorPort);
        expectedSpoutLastError.set_host(expectedSpoutErrorHost);

        // Create mock StormClusterState
        final IStormClusterState mockStormClusterState = mock(IStormClusterState.class);
        when(mockStormClusterState.lastError(eq("my-storm-id"), eq("my-component")))
            .thenReturn(expectedBoltLastError);
        when(mockStormClusterState.lastError(eq("my-storm-id"), eq("my-spout")))
            .thenReturn(expectedSpoutLastError);

        // Setup inputs.
        makeTopoInfoWithSpout();

        // Call method under test.
        final TopologyPageInfo topologyPageInfo = StatsUtil.aggTopoExecsStats(
            "my-storm-id",
            exec2NodePort,
            task2Component,
            beats,
            null,
            ":all-time",
            false,
            mockStormClusterState
        );

        // Validate result
        assertNotNull(topologyPageInfo, "Should never be null");
        assertEquals("my-storm-id", topologyPageInfo.get_id());
        assertEquals(8, topologyPageInfo.get_num_tasks(), "Should have 7 tasks.");
        assertEquals(2, topologyPageInfo.get_num_workers(), "Should have 2 workers.");
        assertEquals(2, topologyPageInfo.get_num_executors(), "Should have only a single executor.");

        // Validate Spout aggregate statistics
        assertNotNull(topologyPageInfo.get_id_to_spout_agg_stats(), "Should be non-null");
        assertEquals(1, topologyPageInfo.get_id_to_spout_agg_stats().size());
        assertEquals(1, topologyPageInfo.get_id_to_spout_agg_stats_size());

        assertTrue(topologyPageInfo.get_id_to_spout_agg_stats().containsKey("my-spout"));
        assertNotNull(topologyPageInfo.get_id_to_spout_agg_stats().get("my-spout"));
        ComponentAggregateStats componentStats = topologyPageInfo.get_id_to_spout_agg_stats().get("my-spout");
        assertEquals(ComponentType.SPOUT, componentStats.get_type(), "Should be of type spout");

        assertNotNull(componentStats.get_last_error(), "Last error should not be null");
        ErrorInfo lastError = componentStats.get_last_error();
        assertEquals(expectedSpoutErrorMsg, lastError.get_error());
        assertEquals(expectedSpoutErrorHost, lastError.get_host());
        assertEquals(expectedSpoutErrorPort, lastError.get_port());
        assertEquals(expectedSpoutErrorTime, lastError.get_error_time_secs());

        // Validate Bolt aggregate statistics
        assertNotNull(topologyPageInfo.get_id_to_bolt_agg_stats(), "Should be non-null");
        assertEquals(1, topologyPageInfo.get_id_to_bolt_agg_stats().size());
        assertEquals(1, topologyPageInfo.get_id_to_bolt_agg_stats_size());

        assertTrue(topologyPageInfo.get_id_to_bolt_agg_stats().containsKey("my-component"));
        assertNotNull(topologyPageInfo.get_id_to_bolt_agg_stats().get("my-component"));
        componentStats = topologyPageInfo.get_id_to_bolt_agg_stats().get("my-component");
        assertEquals(ComponentType.BOLT, componentStats.get_type(), "Should be of type bolt");

        assertNotNull(componentStats.get_last_error(), "Last error should not be null");
        lastError = componentStats.get_last_error();
        assertEquals(expectedBoltErrorMsg, lastError.get_error());
        assertEquals(expectedBoltErrorHost, lastError.get_host());
        assertEquals(expectedBoltErrorPort, lastError.get_port());
        assertEquals(expectedBoltErrorTime, lastError.get_error_time_secs());

        // Verify mock interactions
        verify(mockStormClusterState, times(1))
            .lastError(eq("my-storm-id"), eq("my-component"));
        verify(mockStormClusterState, times(1))
            .lastError(eq("my-storm-id"), eq("my-spout"));
    }

    /**
     * Targeted validation against StatsUtil.aggTopoExecsStats()
     * to verify that when a bolt and spout does NOT have an error reported,
     * it gracefully handles not having a value.
     */
    @Test
    public void aggTopoExecsStats_boltAndSpoutsHaveNoLastErrorReported() {
        // Create mock StormClusterState
        final IStormClusterState mockStormClusterState = mock(IStormClusterState.class);
        when(mockStormClusterState.lastError(eq("my-storm-id"), eq("my-component")))
            .thenReturn(null);
        when(mockStormClusterState.lastError(eq("my-storm-id"), eq("my-spout")))
            .thenReturn(null);

        // Setup inputs.
        makeTopoInfoWithSpout();

        // Call method under test.
        final TopologyPageInfo topologyPageInfo = StatsUtil.aggTopoExecsStats(
            "my-storm-id",
            exec2NodePort,
            task2Component,
            beats,
            null,
            ":all-time",
            false,
            mockStormClusterState
        );

        // Validate result
        assertNotNull(topologyPageInfo, "Should never be null");
        assertEquals("my-storm-id", topologyPageInfo.get_id());
        assertEquals(8, topologyPageInfo.get_num_tasks(), "Should have 7 tasks.");
        assertEquals(2, topologyPageInfo.get_num_workers(), "Should have 2 workers.");
        assertEquals(2, topologyPageInfo.get_num_executors(), "Should have only a single executor.");

        // Validate Spout aggregate statistics
        assertNotNull(topologyPageInfo.get_id_to_spout_agg_stats(), "Should be non-null");
        assertEquals(1, topologyPageInfo.get_id_to_spout_agg_stats().size());
        assertEquals(1, topologyPageInfo.get_id_to_spout_agg_stats_size());

        assertTrue(topologyPageInfo.get_id_to_spout_agg_stats().containsKey("my-spout"));
        assertNotNull(topologyPageInfo.get_id_to_spout_agg_stats().get("my-spout"));
        ComponentAggregateStats componentStats = topologyPageInfo.get_id_to_spout_agg_stats().get("my-spout");
        assertEquals(ComponentType.SPOUT, componentStats.get_type(), "Should be of type spout");
        assertNull(componentStats.get_last_error(), "Last error should not be null");

        // Validate Bolt aggregate statistics
        assertNotNull(topologyPageInfo.get_id_to_bolt_agg_stats(), "Should be non-null");
        assertEquals(1, topologyPageInfo.get_id_to_bolt_agg_stats().size());
        assertEquals(1, topologyPageInfo.get_id_to_bolt_agg_stats_size());

        assertTrue(topologyPageInfo.get_id_to_bolt_agg_stats().containsKey("my-component"));
        assertNotNull(topologyPageInfo.get_id_to_bolt_agg_stats().get("my-component"));
        componentStats = topologyPageInfo.get_id_to_bolt_agg_stats().get("my-component");
        assertEquals(ComponentType.BOLT, componentStats.get_type(), "Should be of type bolt");
        assertNull(componentStats.get_last_error(), "Last error should not be null");

        // Verify mock interactions
        verify(mockStormClusterState, times(1))
            .lastError(eq("my-storm-id"), eq("my-component"));
        verify(mockStormClusterState, times(1))
            .lastError(eq("my-storm-id"), eq("my-spout"));
    }
}
