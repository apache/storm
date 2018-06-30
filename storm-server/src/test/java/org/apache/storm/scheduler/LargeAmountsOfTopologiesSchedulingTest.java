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

package org.apache.storm.scheduler;

import com.google.common.collect.Lists;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.daemon.nimbus.Nimbus;
import org.apache.storm.daemon.nimbus.TopoCache;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.nimbus.AssignmentDistributionService;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.scheduler.utils.ISchedulingTracer;
import org.apache.storm.testing.InProcessZookeeper;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.Utils;

import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.AdditionalAnswers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;

/**
 * This class is used to test master scheduling time cost.
 * If you wanna test time cost on some key points, try to add a {@link ISchedulingTracer} to Nimbus instance
 * and use it to trace the actions that you wanna check out. See {@link SchedulerTestUtils.TestSchedulingTracer}
 * for an example.
 *
 * <p>The main purpose for this test is testing scheduling performance when there are hundreds of topologies on
 * cluster. You can tweak the args in {@link #prepareData} to control the num of topologies, or add sequence of numbers
 * to test, the default num is 400.
 *
 * <p>Other configurations:
 * <ul>
 * <li>200 supervisor nodes, 40 slots each.</li>
 * <li>10 workers, 10 spout tasks, 40 bolt tasks for every topology.</li>
 * </ul>
 */
@RunWith(Parameterized.class)
public class LargeAmountsOfTopologiesSchedulingTest {
    private static final Logger LOG = LoggerFactory.getLogger(LargeAmountsOfTopologiesSchedulingTest.class);

    private Map<String, Object> conf;
    private InProcessZookeeper zookeeper;
    private int numTopologies;
    private int numSupervisors = 0;

    private final Map<String, Map<String, Object>> idToTopoConf = new HashMap<>();
    private final Map<String, StormTopology> idToStormTopology = new HashMap<>();
    private final Map<String, StormBase> idToStormBase = new HashMap<>();
    private final Map<String, SupervisorInfo> allSupervisorInfo = new HashMap<>();

    @Parameterized.Parameters
    public static Collection prepareData() {
        Object[][] object = {{400}};
        return Arrays.asList(object);
    }

    public LargeAmountsOfTopologiesSchedulingTest(Object numTopologies) {
        this.numTopologies = (Integer) numTopologies;
    }

    @Before
    public void setUp() throws Exception {
        this.zookeeper = new InProcessZookeeper();
        this.conf = Utils.readStormConfig();
        // set to max integer to let executors never timed-out.
        conf.put(DaemonConfig.NIMBUS_TASK_LAUNCH_SECS, Integer.MAX_VALUE);
        conf.put(Config.STORM_ZOOKEEPER_PORT, zookeeper.getPort());
        conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("localhost"));

        this.numSupervisors = 200;
        initializeTopologyConfs("topo-", this.numTopologies, 10);
        initializeStormTopologies();
        normalizeStormTopologies();
        initializeStormBases();
        initializeSupervisorInfo(this.numSupervisors, 40);
    }

    @After
    public void clean() throws Exception {
        this.zookeeper.close();
        this.idToTopoConf.clear();
        this.idToStormTopology.clear();
        this.idToStormBase.clear();
        this.allSupervisorInfo.clear();
    }

    @Test
    public void testNimbusMkAssignmentsTimeCost() throws Exception {
        SchedulerTestUtils.INimbusTest iNimbus = new SchedulerTestUtils.INimbusTest();
        final Map<String, Assignment> idToAssignment = new HashMap<>();

        IStormClusterState stormClusterState = mock(IStormClusterState.class);
        when(stormClusterState.assignments(any()))
            .thenAnswer(invocation -> Lists.newArrayList(idToAssignment.keySet()));
        when(stormClusterState.assignmentInfo(anyString(), any()))
            .thenAnswer(invocation -> idToAssignment.get(invocation.getArgument(0)));
        when(stormClusterState.stormBase(anyString(), any()))
            .thenAnswer(invocation -> idToStormBase.get(invocation.getArgument(0)));

        when(stormClusterState.isAssignmentsBackendSynchronized()).thenReturn(true);
        when(stormClusterState.activeStorms()).thenReturn(Lists.newArrayList(idToTopoConf.keySet()));
        when(stormClusterState.topologyBases(anySet())).thenAnswer(
            invocation -> {
                Set<String> ids = invocation.getArgument(0);
                return idToStormBase.entrySet()
                    .stream()
                    .filter(entry -> ids.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            });
        when(stormClusterState.allSupervisorInfo()).thenReturn(allSupervisorInfo);
        doAnswer(AdditionalAnswers.answerVoid(
            (String id, Assignment ass, Map<String, Object> conf) -> idToAssignment.put(id, ass)))
            .when(stormClusterState)
            .setAssignment(anyString(), any(Assignment.class), any());

        ILeaderElector leaderElector = mock(ILeaderElector.class);
        when(leaderElector.isLeader()).thenReturn(true);

        TopoCache topoCache = mock(TopoCache.class);
        when(topoCache.readTopoConf(anyString(), any()))
            .thenAnswer(invocation -> idToTopoConf.get(invocation.getArgument(0)));
        when(topoCache.readTopology(anyString(), any()))
            .thenAnswer(invocation -> idToStormTopology.get(invocation.getArgument(0)));

        AssignmentDistributionService assignmentDistributionService = mock(AssignmentDistributionService.class);
        doAnswer(AdditionalAnswers.answerVoid((id, node, port, assignments) -> {
        }))
            .when(assignmentDistributionService)
            .addAssignmentsForNode(anyString(), anyString(), anyInt(), any());

        ISchedulingTracer tracer = new SchedulerTestUtils.TestSchedulingTracer();
        Nimbus nimbus = new Nimbus(
            conf,
            iNimbus,
            stormClusterState,
            null,
            null,
            topoCache,
            leaderElector,
            null);
        nimbus.getHeartbeatsReadyFlag().getAndSet(true);
        nimbus.setSchedulingTracer(tracer);
        nimbus.setAssignmentsDistributer(assignmentDistributionService);

        nimbus.mkAssignments();
        tracer.reset();
        // All the topologies should have been scheduled here.
        assert idToAssignment.size() == this.numTopologies;
        // Mock that one topology executors are all dead.
        idToAssignment.remove(this.idToTopoConf.keySet().stream().findFirst().get());
        // First make a full scheduling, set scheduling resource cache not initialized
        // to trigger.
        iNimbus.setResourceCacheInitialized(false);
        long startTimeSecs1 = System.currentTimeMillis() / 1000L;
        nimbus.mkAssignments();
        long endTimeSecs1 = System.currentTimeMillis() / 1000L;
        assert idToAssignment.size() == this.numTopologies;

        // Mock that one topology executors are all dead.
        idToAssignment.remove(this.idToTopoConf.keySet().stream().findFirst().get());
        long startTimeSecs2 = System.currentTimeMillis() / 1000L;
        nimbus.mkAssignments();
        long endTimeSecs2 = System.currentTimeMillis() / 1000L;
        assert idToAssignment.size() == this.numTopologies;
        // the second scheduling for 1 topology should be fast enough.
        assert (endTimeSecs2 - startTimeSecs2) < 5;
    }

    private void normalizeStormTopologies() throws InvalidTopologyException {
        Map<String, StormTopology> ret = new HashMap<>();

        for (Map.Entry<String, Map<String, Object>> entry : idToTopoConf.entrySet()) {
            String topoId = entry.getKey();
            StormTopology stormTopology = idToStormTopology.get(topoId);
            assert stormTopology != null;
            ret.put(topoId, normalizeTopology(entry.getValue(), stormTopology));
        }
        this.idToStormTopology.putAll(ret);
    }

    // Copied from Nimbus.java
    private static StormTopology normalizeTopology(Map<String, Object> topoConf, StormTopology topology)
        throws InvalidTopologyException {
        StormTopology ret = topology.deepCopy();
        for (Object comp : StormCommon.allComponents(ret).values()) {
            Map<String, Object> mergedConf = StormCommon.componentConf(comp);
            mergedConf.put(Config.TOPOLOGY_TASKS, ServerUtils.getComponentParallelism(topoConf, comp));
            String jsonConf = JSONValue.toJSONString(mergedConf);
            StormCommon.getComponentCommon(comp).set_json_conf(jsonConf);
        }
        return ret;
    }

    private void initializeTopologyConfs(String namePrefix, int numTopologies, int numWorkers) {
        for (int i = 0; i < numTopologies; i++) {
            Map<String, Object> topoConf = new HashMap<>();
            String topoId = namePrefix + i;
            topoConf.put(Config.TOPOLOGY_NAME, topoId);
            topoConf.put(Config.TOPOLOGY_WORKERS, numWorkers);
            topoConf.put(Config.TOPOLOGY_TASKS, 100);
            topoConf = Utils.merge(topoConf, this.conf);
            idToTopoConf.put(topoId, topoConf);
        }
    }

    private void initializeStormTopologies() {
        for (Map.Entry<String, Map<String, Object>> entry : idToTopoConf.entrySet()) {
            StormTopology stormTopology = SchedulerTestUtils.buildTopology(10, 40, 10, 40);
            idToStormTopology.put(entry.getKey(), stormTopology);
        }
    }

    private void initializeStormBases() throws InvalidTopologyException {
        for (Map.Entry<String, Map<String, Object>> entry : idToTopoConf.entrySet()) {
            StormBase stormBase = SchedulerTestUtils.buildStormBase(
                entry.getKey(),
                entry.getValue(),
                TopologyStatus.ACTIVE,
                idToStormTopology.get(entry.getKey()));
            idToStormBase.put(entry.getKey(), stormBase);
        }
    }

    private void initializeSupervisorInfo(int numSupervisors, int numPorts) {
        Map<String, SupervisorDetails> supervisorDetails =
            SchedulerTestUtils.genSupervisors(numSupervisors, numPorts);
        for (Map.Entry<String, SupervisorDetails> entry : supervisorDetails.entrySet()) {
            SupervisorDetails sd = entry.getValue();
            SupervisorInfo supervisorInfo = new SupervisorInfo();
            supervisorInfo.set_hostname(sd.getHost());
            supervisorInfo.set_assignment_id(sd.getId());
            supervisorInfo.set_meta(sd.getAllPorts().stream().map(Long::valueOf).collect(Collectors.toList()));
            supervisorInfo.set_server_port(9728);
            allSupervisorInfo.put(entry.getKey(), supervisorInfo);
        }
    }
}
