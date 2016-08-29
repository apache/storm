/**
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
package org.apache.storm.stats;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.generated.WorkerSummary;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

public class TestStatsUtil {

    /*
     * aggWorkerStats tests
     */
    private Map<Integer, String> task2Component = new HashMap<Integer, String>();
    private Map<List<Integer>, Map<String, Object>> beats = new HashMap<List<Integer>, Map<String, Object>>();
    private Map<List<Long>, List<Object>> exec2NodePort = new HashMap<List<Long>, List<Object>>();
    private Map<String, String> nodeHost = new HashMap<String, String>();
    private Map<WorkerSlot, WorkerResources> worker2Resources = new HashMap<WorkerSlot, WorkerResources>();

    private List<Long> makeExecutorId(int firstTask, int lastTask){
        return Arrays.asList(new Long(firstTask), new Long(lastTask));
    }

    public void makeTopoInfo() {
        List<Object> hostPort = new ArrayList<Object>();
        hostPort.add("node1");
        hostPort.add(new Long(1));

        exec2NodePort.put(makeExecutorId(1,1), hostPort);

        nodeHost.put("node1", "host1");
        nodeHost.put("node2", "host2");
        nodeHost.put("node3", "host3");

        List<Integer> exec1 = new ArrayList<Integer>();
        exec1.add(1);
        exec1.add(1);
        HashMap<String, Object> exec1Beat = new HashMap<String, Object>();
        exec1Beat.put("uptime", 100);

        // should not be returned since this executor is not part of the topology's assignment
        List<Integer> exec2 = new ArrayList<Integer>();
        exec2.add(2);
        exec2.add(4);
        HashMap<String, Object> exec2Beat = new HashMap<String, Object>();
        exec2Beat.put("uptime", 200);

        Map<String, Object> beat1 = new HashMap<String, Object>();
        beat1.put("heartbeat", exec1Beat);

        Map<String, Object> beat2 = new HashMap<String, Object>();
        beat2.put("heartbeat", exec2Beat);

        beats.put(exec1, beat1);
        beats.put(exec2, beat2);

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

    private List<WorkerSummary> checkWorkerStats(boolean includeSys, boolean userAuthorized, String filterSupervisor) {
        List<WorkerSummary> summaries = 
            StatsUtil.aggWorkerStats("my-storm-id", "my-storm-name", 
                                     task2Component, beats, exec2NodePort, nodeHost, worker2Resources, 
                                     includeSys, userAuthorized, filterSupervisor);
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
}
