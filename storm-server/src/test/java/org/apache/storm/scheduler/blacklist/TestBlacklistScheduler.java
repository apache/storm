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
package org.apache.storm.scheduler.blacklist;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.DefaultScheduler;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.ResourceAwareScheduler;
import org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategyOld;
import org.apache.storm.scheduler.resource.strategies.scheduling.GenericResourceAwareStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.GenericResourceAwareStrategyOld;
import org.apache.storm.scheduler.resource.strategies.scheduling.RoundRobinResourceAwareStrategy;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.resource.normalization.ResourceMetrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBlacklistScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(TestBlacklistScheduler.class);

    private int currentTime = 1468216504;
    private IScheduler scheduler = null;

    @AfterEach
    public void cleanup() {
        if (scheduler != null) {
            scheduler.cleanup();
            scheduler = null;
        }
    }

    @Test
    public void testBlacklistResumeWhenAckersWontFit() throws InvalidTopologyException {
        // 3 supervisors exist with 4 slots, 2 are blacklisted
        // topology with given worker heap size would fit in 4 slots if ignoring ackers, needs 5 slots with ackers.
        // verify that one of the supervisors will be resumed and topology will schedule.

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_TIME, 300);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_COUNT, 3);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_RESUME_TIME, 1800);
        config.put(DaemonConfig.STORM_WORKER_MIN_CPU_PCORE_PERCENT, 100);
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 10);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_ASSUME_SUPERVISOR_BAD_BASED_ON_BAD_SLOT, false);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_COUNT, 3);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 128);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_STRATEGY, "org.apache.storm.scheduler.blacklist.strategies.RasBlacklistStrategy");
        config.put(Config.TOPOLOGY_RAS_ACKER_EXECUTORS_PER_WORKER, 1);
        config.setNumWorkers(1);
        config.put(Config.TOPOLOGY_ACKER_EXECUTORS, 4);
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, "org.apache.storm.scheduler.resource.strategies.scheduling.GenericResourceAwareStrategy");
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 512);
        config.put(Config.WORKER_HEAP_MEMORY_MB, 768);
        config.put(Config.TOPOLOGY_NAME, "testTopology");

        INimbus iNimbus = new TestUtilsForBlacklistScheduler.INimbusTest();
        StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
        ResourceMetrics resourceMetrics = new ResourceMetrics(metricsRegistry);
        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(3, 4, 400.0d, 4096.0d);
        Topologies noTopologies = new Topologies();
        Cluster cluster = new Cluster(iNimbus, resourceMetrics, supMap, new HashMap<>(), noTopologies, config);

        scheduler = new BlacklistScheduler(new ResourceAwareScheduler());
        scheduler.prepare(config, metricsRegistry);
        scheduler.schedule(noTopologies, cluster);

        Map<String, SupervisorDetails> removedSup0 = TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0");
        Map<String, SupervisorDetails> removedSup0Sup1 = TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(removedSup0, "sup-1");

        cluster = new Cluster(iNimbus, resourceMetrics, removedSup0Sup1,
                TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), noTopologies, config);
        scheduler.schedule(noTopologies, cluster);
        scheduler.schedule(noTopologies, cluster);
        scheduler.schedule(noTopologies, cluster);

        // 2 supervisors blacklisted at this point.  Let's schedule the topology.

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        TopologyDetails topo1 = createResourceTopo(config);
        topoMap.put(topo1.getId(), topo1);
        Topologies topologies = new Topologies(topoMap);

        cluster = new Cluster(iNimbus, resourceMetrics, supMap,
                TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        boolean enableTraceLogging = false; // for scheduling debug
        if (enableTraceLogging) {
            Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.TRACE);
        }
        scheduler.schedule(topologies, cluster);

        // topology should be fully scheduled with 1 host remaining blacklisted
        String topoScheduleStatus = cluster.getStatus("testTopology-id");
        assertTrue(topoScheduleStatus.contains("Running - Fully Scheduled"));
        assertEquals(1, cluster.getBlacklistedHosts().size());
    }

    public TopologyDetails createResourceTopo(Config conf) throws InvalidTopologyException {
        int spoutParallelism = 6;
        int boltParallelism = 5;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word", new TestUtilsForResourceAwareScheduler.TestSpout(),
                spoutParallelism);
        builder.setBolt("exclaim1", new TestUtilsForResourceAwareScheduler.TestBolt(),
                boltParallelism).shuffleGrouping("word");
        builder.setBolt("exclaim2", new TestUtilsForResourceAwareScheduler.TestBolt(),
                boltParallelism).shuffleGrouping("exclaim1");

        StormTopology stormTopology = builder.createTopology();

        TopologyDetails topo = new TopologyDetails("testTopology-id", conf, stormTopology, 0,
                genExecsAndComps(StormCommon.systemTopology(conf, stormTopology)), currentTime, "user");
        return topo;
    }

    public static Map<ExecutorDetails, String> genExecsAndComps(StormTopology topology) {
        Map<ExecutorDetails, String> retMap = new HashMap<>();
        int startTask = 1;
        int endTask = 1;
        for (Map.Entry<String, SpoutSpec> entry : topology.get_spouts().entrySet()) {
            SpoutSpec spout = entry.getValue();
            String spoutId = entry.getKey();
            int spoutParallelism = spout.get_common().get_parallelism_hint();
            for (int i = 0; i < spoutParallelism; i++) {
                retMap.put(new ExecutorDetails(startTask, endTask), spoutId);
                startTask++;
                endTask++;
            }
        }

        for (Map.Entry<String, Bolt> entry : topology.get_bolts().entrySet()) {
            String boltId = entry.getKey();
            Bolt bolt = entry.getValue();
            int boltParallelism = bolt.get_common().get_parallelism_hint();
            for (int i = 0; i < boltParallelism; i++) {
                retMap.put(new ExecutorDetails(startTask, endTask), boltId);
                startTask++;
                endTask++;
            }
        }
        return retMap;
    }

    @Test
    public void TestBadSupervisor() {
        INimbus iNimbus = new TestUtilsForBlacklistScheduler.INimbusTest();

        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(3, 4);

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_TIME, 200);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_COUNT, 2);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_RESUME_TIME, 300);

        Map<String, TopologyDetails> topoMap = new HashMap<>();

        TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, true);
        topoMap.put(topo1.getId(), topo1);

        Topologies topologies = new Topologies(topoMap);
        StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
        ResourceMetrics resourceMetrics = new ResourceMetrics(metricsRegistry);
        Cluster cluster = new Cluster(iNimbus, resourceMetrics, supMap, new HashMap<>(), topologies, config);
        scheduler = new BlacklistScheduler(new DefaultScheduler());
        scheduler.prepare(config, metricsRegistry);
        scheduler.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, resourceMetrics, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        scheduler.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, resourceMetrics, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        scheduler.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, resourceMetrics, supMap, TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        scheduler.schedule(topologies, cluster);
        assertEquals(Collections.singleton("host-0"), cluster.getBlacklistedHosts(), "blacklist");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void TestBadSlot(boolean blacklistOnBadSlot) {
        INimbus iNimbus = new TestUtilsForBlacklistScheduler.INimbusTest();

        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(3, 4);

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_TIME, 200);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_COUNT, 2);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_RESUME_TIME, 300);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_ASSUME_SUPERVISOR_BAD_BASED_ON_BAD_SLOT, blacklistOnBadSlot);

        Map<String, TopologyDetails> topoMap = new HashMap<>();

        TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, true);
        topoMap.put(topo1.getId(), topo1);

        Topologies topologies = new Topologies(topoMap);
        StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
        ResourceMetrics resourceMetrics = new ResourceMetrics(metricsRegistry);
        Cluster cluster = new Cluster(iNimbus, resourceMetrics, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        scheduler = new BlacklistScheduler(new DefaultScheduler());
        scheduler.prepare(config, metricsRegistry);
        scheduler.schedule(topologies, cluster);

        cluster = new Cluster(iNimbus, resourceMetrics, TestUtilsForBlacklistScheduler.removePortFromSupervisors(supMap,
                "sup-0", 0), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        scheduler.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, resourceMetrics, TestUtilsForBlacklistScheduler.removePortFromSupervisors(supMap, "sup-0", 0),
                TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        scheduler.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, resourceMetrics, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        scheduler.schedule(topologies, cluster);

        if (blacklistOnBadSlot) {
            assertEquals(Collections.singleton("host-0"), cluster.getBlacklistedHosts(), "blacklist");
        } else {
            assertEquals(Collections.emptySet(), cluster.getBlacklistedHosts(), "blacklist");
        }
    }

    @Test
    public void TestResumeBlacklist() {
        INimbus iNimbus = new TestUtilsForBlacklistScheduler.INimbusTest();

        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(3, 4);

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_TIME, 200);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_COUNT, 2);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_RESUME_TIME, 300);

        Map<String, TopologyDetails> topoMap = new HashMap<>();

        TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, true);
        topoMap.put(topo1.getId(), topo1);

        Topologies topologies = new Topologies(topoMap);
        StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
        ResourceMetrics resourceMetrics = new ResourceMetrics(metricsRegistry);
        Cluster cluster = new Cluster(iNimbus, resourceMetrics, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        scheduler = new BlacklistScheduler(new DefaultScheduler());
        scheduler.prepare(config, metricsRegistry);
        scheduler.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, resourceMetrics, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        scheduler.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, resourceMetrics, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        scheduler.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, resourceMetrics, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        scheduler.schedule(topologies, cluster);
        assertEquals(Collections.singleton("host-0"), cluster.getBlacklistedHosts(), "blacklist");
        for (int i = 0; i < 300 / 10 - 2; i++) {
            scheduler.schedule(topologies, cluster);
        }
        assertEquals(Collections.singleton("host-0"), cluster.getBlacklistedHosts(), "blacklist");
        scheduler.schedule(topologies, cluster);
        assertEquals(Collections.emptySet(), cluster.getBlacklistedHosts(), "blacklist");
    }

    @Test
    public void TestReleaseBlacklist() {
        INimbus iNimbus = new TestUtilsForBlacklistScheduler.INimbusTest();

        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(3, 4);

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_TIME, 200);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_COUNT, 2);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_RESUME_TIME, 300);

        Map<String, TopologyDetails> topoMap = new HashMap<>();

        TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, true);
        TopologyDetails topo2 = TestUtilsForBlacklistScheduler.getTopology("topo-2", config, 5, 15, 1, 1, currentTime - 8, true);
        TopologyDetails topo3 = TestUtilsForBlacklistScheduler.getTopology("topo-3", config, 5, 15, 1, 1, currentTime - 16, true);
        TopologyDetails topo4 = TestUtilsForBlacklistScheduler.getTopology("topo-4", config, 5, 15, 1, 1, currentTime - 32, true);
        topoMap.put(topo1.getId(), topo1);

        Topologies topologies = new Topologies(topoMap);
        StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
        ResourceMetrics resourceMetrics = new ResourceMetrics(metricsRegistry);
        Cluster cluster = new Cluster(iNimbus, resourceMetrics, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        scheduler = new BlacklistScheduler(new DefaultScheduler());
        scheduler.prepare(config, metricsRegistry);
        scheduler.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, resourceMetrics, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        scheduler.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, resourceMetrics, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        scheduler.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, resourceMetrics, supMap, TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        scheduler.schedule(topologies, cluster);
        assertEquals(Collections.singleton("host-0"), cluster.getBlacklistedHosts(), "blacklist");
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);
        topologies = new Topologies(topoMap);
        cluster = new Cluster(iNimbus, resourceMetrics, supMap, TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        scheduler.schedule(topologies, cluster);
        assertEquals(Collections.emptySet(), cluster.getBlacklistedHosts(), "blacklist");
    }

    @Test
    public void TestGreylist() {
        INimbus iNimbus = new TestUtilsForBlacklistScheduler.INimbusTest();

        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(2, 3);

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_TIME, 200);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_COUNT, 2);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_RESUME_TIME, 300);
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 0.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 0);
        config.put(Config.TOPOLOGY_RAS_ONE_EXECUTOR_PER_WORKER, true);

        Class[] strategyClasses = {
                DefaultResourceAwareStrategy.class,
                DefaultResourceAwareStrategyOld.class,
                RoundRobinResourceAwareStrategy.class,
                GenericResourceAwareStrategy.class,
                GenericResourceAwareStrategyOld.class,
        };
        for (Class strategyClass: strategyClasses) {
            String strategyClassName = strategyClass.getName();
            config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, strategyClassName);
            {
                Map<String, TopologyDetails> topoMap = new HashMap<>();

                TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 1, 1, 1, 1, currentTime - 2, true);
                TopologyDetails topo2 = TestUtilsForBlacklistScheduler.getTopology("topo-2", config, 1, 1, 1, 1, currentTime - 8, true);
                Topologies topologies = new Topologies(topoMap);

                StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
                ResourceMetrics resourceMetrics = new ResourceMetrics(metricsRegistry);
                Cluster cluster = new Cluster(iNimbus, resourceMetrics, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
                scheduler = new BlacklistScheduler(new ResourceAwareScheduler());

                scheduler.prepare(config, metricsRegistry);
                scheduler.schedule(topologies, cluster);
                cluster = new Cluster(iNimbus, resourceMetrics, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
                scheduler.schedule(topologies, cluster);
                cluster = new Cluster(iNimbus, resourceMetrics, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
                scheduler.schedule(topologies, cluster);
                cluster = new Cluster(iNimbus, resourceMetrics, supMap, TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
                scheduler.schedule(topologies, cluster);
                assertEquals(Collections.singleton("host-0"), cluster.getBlacklistedHosts(), "blacklist");

                topoMap.put(topo1.getId(), topo1);
                topoMap.put(topo2.getId(), topo2);
                topologies = new Topologies(topoMap);
                cluster = new Cluster(iNimbus, resourceMetrics, supMap, TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
                scheduler.schedule(topologies, cluster);
                assertEquals(Collections.emptySet(), cluster.getBlacklistedHosts(), "blacklist using " + strategyClassName);
                assertEquals(Collections.singletonList("sup-0"), cluster.getGreyListedSupervisors(), "greylist using" +  strategyClassName);
                LOG.debug("{}: Now only these slots remain available: {}", strategyClassName, cluster.getAvailableSlots());
                if (strategyClass == RoundRobinResourceAwareStrategy.class) {
                    // available slots will be across supervisors
                    assertFalse(cluster.getAvailableSlots(supMap.get("sup-0")).containsAll(cluster.getAvailableSlots()), "using " + strategyClassName);
                } else {
                    assertTrue(cluster.getAvailableSlots(supMap.get("sup-0")).containsAll(cluster.getAvailableSlots()), "using " + strategyClassName);
                }
            }
        }
    }

    @Test
    public void TestList() {
        INimbus iNimbus = new TestUtilsForBlacklistScheduler.INimbusTest();
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_TIME, 200);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_COUNT, 2);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_RESUME_TIME, 300);

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, true);
        TopologyDetails topo2 = TestUtilsForBlacklistScheduler.getTopology("topo-2", config, 5, 15, 1, 1, currentTime - 2, true);
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);
        Topologies topologies = new Topologies(topoMap);
        scheduler = new BlacklistScheduler(new DefaultScheduler());
        scheduler.prepare(config, new StormMetricsRegistry());

        List<Map<Integer, List<Integer>>> faultList = new ArrayList<>();

        faultList.add(new HashMap<>());
        faultList.add(ImmutableMap.of(0, ImmutableList.of(0, 1)));
        faultList.add(ImmutableMap.of(0, new ArrayList<>()));
        for (int i = 0; i < 17; i++) {
            faultList.add(new HashMap<>());
        }
        faultList.add(ImmutableMap.of(0, ImmutableList.of(0, 1)));
        faultList.add(ImmutableMap.of(1, ImmutableList.of(1)));
        for (int i = 0; i < 8; i++) {
            faultList.add(new HashMap<>());
        }
        faultList.add(ImmutableMap.of(0, ImmutableList.of(1)));
        faultList.add(ImmutableMap.of(1, ImmutableList.of(1)));
        for (int i = 0; i < 30; i++) {
            faultList.add(new HashMap<>());
        }

        List<Map<String, SupervisorDetails>> supervisorsList = FaultGenerateUtils.getSupervisorsList(3, 4, faultList);
        Cluster cluster = null;
        int count = 0;
        for (Map<String, SupervisorDetails> supervisors : supervisorsList) {
            cluster = FaultGenerateUtils.nextCluster(cluster, supervisors, iNimbus, config, topologies);
            scheduler.schedule(topologies, cluster);
            if (count == 0) {
                Set<String> hosts = new HashSet<>();
                assertEquals(hosts, cluster.getBlacklistedHosts(), "blacklist");
            } else if (count == 2) {
                Set<String> hosts = new HashSet<>();
                assertEquals(hosts, cluster.getBlacklistedHosts(), "blacklist");
            } else if (count == 3) {
                Set<String> hosts = new HashSet<>();
                hosts.add("host-0");
                assertEquals(hosts, cluster.getBlacklistedHosts(), "blacklist");
            } else if (count == 30) {
                Set<String> hosts = new HashSet<>();
                hosts.add("host-0");
                assertEquals(hosts, cluster.getBlacklistedHosts(), "blacklist");
            } else if (count == 31) {
                Set<String> hosts = new HashSet<>();
                hosts.add("host-0");
                hosts.add("host-1");
                assertEquals(hosts, cluster.getBlacklistedHosts(), "blacklist");
            } else if (count == 32) {
                Set<String> hosts = new HashSet<>();
                hosts.add("host-0");
                hosts.add("host-1");
                assertEquals(hosts, cluster.getBlacklistedHosts(), "blacklist");
            } else if (count == 60) {
                Set<String> hosts = new HashSet<>();
                hosts.add("host-0");
                hosts.add("host-1");
                assertEquals(hosts, cluster.getBlacklistedHosts(), "blacklist");
            } else if (count == 61) {
                Set<String> hosts = new HashSet<>();
                hosts.add("host-0");
                assertEquals(hosts, cluster.getBlacklistedHosts(), "blacklist");
            } else if (count == 62) {
                Set<String> hosts = new HashSet<>();
                assertEquals(hosts, cluster.getBlacklistedHosts(), "blacklist");
            }
            count++;
        }
    }

    @Test
    public void removeLongTimeDisappearFromCache(){
        INimbus iNimbus = new TestUtilsForBlacklistScheduler.INimbusTest();

        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(3,4);

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_TIME,200);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_COUNT,2);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_RESUME_TIME,300);
        Map<String, TopologyDetails> topoMap = new HashMap<>();

        TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2,true);
        topoMap.put(topo1.getId(), topo1);

        Topologies topologies = new Topologies(topoMap);
        StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
        ResourceMetrics resourceMetrics = new ResourceMetrics(metricsRegistry);
        Cluster cluster = new Cluster(iNimbus, resourceMetrics, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        BlacklistScheduler bs = new BlacklistScheduler(new DefaultScheduler());
        scheduler = bs;
        bs.prepare(config, metricsRegistry);
        bs.schedule(topologies,cluster);
        cluster = new Cluster(iNimbus, resourceMetrics, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"),
                TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        for (int i = 0 ; i < 20 ; i++){
            bs.schedule(topologies,cluster);
        }
        Set<String> cached = new HashSet<>();
        cached.add("sup-1");
        cached.add("sup-2");
        assertEquals(cached, bs.cachedSupervisors.keySet());
        cluster = new Cluster(iNimbus, resourceMetrics, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        bs.schedule(topologies,cluster);
        cluster = new Cluster(iNimbus, resourceMetrics, TestUtilsForBlacklistScheduler.removePortFromSupervisors(supMap, "sup-0", 0),
                TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        for (int i = 0 ;i < 20 ; i++){
            bs.schedule(topologies, cluster);
        }
        Set<Integer> cachedPorts = Sets.newHashSet(1, 2, 3);
        assertEquals(cachedPorts, bs.cachedSupervisors.get("sup-0"));
    }

    @Test
    public void blacklistSupervisorWithAddedPort() {
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_TIME,10);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_COUNT,2);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_RESUME_TIME,300);

        StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
        scheduler = new BlacklistScheduler(new DefaultScheduler());
        scheduler.prepare(config, metricsRegistry);

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 5,
                15, 1, 1, currentTime - 2, true);
        topoMap.put(topo1.getId(), topo1);
        Topologies topologies = new Topologies(topoMap);

        INimbus iNimbus = new TestUtilsForBlacklistScheduler.INimbusTest();
        ResourceMetrics resourceMetrics = new ResourceMetrics(metricsRegistry);
        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(3,4);
        Cluster cluster = new Cluster(iNimbus, resourceMetrics, supMap, new HashMap<String, SchedulerAssignmentImpl>(),
                topologies, config);

        // allow blacklist scheduler to cache the supervisor
        scheduler.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, resourceMetrics, TestUtilsForBlacklistScheduler.addPortToSupervisors(supMap,
                "sup-0", 4),TestUtilsForBlacklistScheduler.assignmentMapToImpl(
                        cluster.getAssignments()), topologies, config);
        // allow blacklist scheduler to cache the supervisor with an added port
        scheduler.schedule(topologies, cluster);
        // remove the port from the supervisor and make sure the blacklist scheduler can remove the port without
        // throwing an exception
        cluster = new Cluster(iNimbus, resourceMetrics, TestUtilsForBlacklistScheduler.removePortFromSupervisors(supMap,
                "sup-0", 4),TestUtilsForBlacklistScheduler.assignmentMapToImpl(
                        cluster.getAssignments()), topologies, config);
        scheduler.schedule(topologies, cluster);
    }

}
