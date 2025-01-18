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

package org.apache.storm.daemon.nimbus;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.KeySequenceNumber;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.GenericResourceAwareStrategyOld;
import org.apache.storm.scheduler.resource.strategies.scheduling.RoundRobinResourceAwareStrategy;
import org.apache.storm.security.auth.IGroupMappingServiceProvider;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NimbusTest {
    private static final String BLOB_FILE_KEY = "file-key";

    @Mock
    private StormMetricsRegistry metricRegistry;
    @Mock
    private INimbus iNimbus;
    @Mock
    private IStormClusterState stormClusterState;
    @Mock
    private NimbusInfo nimbusInfo;
    @Mock
    private LocalFsBlobStore localBlobStore;
    @Mock
    private ILeaderElector leaderElector;
    @Mock
    private IGroupMappingServiceProvider groupMapper;

    private Nimbus nimbus;

    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();

        Map<String, Object> conf = Map.of(DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10);
        nimbus = new Nimbus(conf, iNimbus, stormClusterState, nimbusInfo, localBlobStore, leaderElector, groupMapper, metricRegistry);
    }

    @Test
    public void testMemoryLoadLargerThanMaxHeapSize() {
        // Topology will not be able to be successfully scheduled: Config TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB=128.0 < 129.0,
        // Largest memory requirement of a component in the topology).
        TopologyBuilder builder1 = new TopologyBuilder();
        builder1.setSpout("wordSpout1", new TestWordSpout(), 4);
        StormTopology stormTopology1 = builder1.createTopology();
        Config config1 = new Config();
        config1.put(Config.STORM_NETWORK_TOPOGRAPHY_PLUGIN, "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping");
        config1.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, DefaultSchedulingPriorityStrategy.class.getName());

        config1.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 10.0);
        config1.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 0.0);
        config1.put(Config.TOPOLOGY_PRIORITY, 0);
        config1.put(Config.TOPOLOGY_SUBMITTER_USER, "zhuo");
        config1.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 128.0);
        config1.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 129.0);
        Class[] strategyClasses = {
                DefaultResourceAwareStrategy.class,
                RoundRobinResourceAwareStrategy.class,
                GenericResourceAwareStrategyOld.class};
        for (Class strategyClass: strategyClasses) {
            String strategyClassName = strategyClass.getName();
            config1.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, strategyClassName);
            try {
                ServerUtils.validateTopologyWorkerMaxHeapSizeConfigs(config1, stormTopology1, 768.0);
                fail("Expected exception not thrown when using Strategy " + strategyClassName);
            } catch (InvalidTopologyException e) {
                //Expected...
            }
        }
    }

    @Test
    public void uploadedBlobPersistsMinimumTime() {
        Set<String> idleTopologies = new HashSet<>();
        idleTopologies.add("topology1");
        Map<String, Object> conf = new HashMap<>();
        conf.put(DaemonConfig.NIMBUS_TOPOLOGY_BLOBSTORE_DELETION_DELAY_MS, 300000);

        try (Time.SimulatedTime ignored = new Time.SimulatedTime(null)) {
            Set<String> toDelete = Nimbus.getExpiredTopologyIds(idleTopologies, conf);
            assertTrue(toDelete.isEmpty());

            Time.advanceTime(10 * 60 * 1000L);

            toDelete = Nimbus.getExpiredTopologyIds(idleTopologies, conf);
            assertTrue(toDelete.contains("topology1"));
            assertEquals(1, toDelete.size());

        }
    }

    @Test
    public void validateNoTopoConfOverrides() {
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap<>());
        topology.set_bolts(new HashMap<>());
        topology.set_state_spouts(new HashMap<>());

        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_MESSAGING_NETTY_AUTHENTICATION, false);
        conf.put(Config.TOPOLOGY_WORKER_NIMBUS_THRIFT_CLIENT_USE_TLS, false);
        conf.put(Config.STORM_WORKERS_ARTIFACTS_DIR, "a");
        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(Config.STORM_WORKERS_ARTIFACTS_DIR, "b");
        Map<String, Object> normalized = Nimbus.normalizeConf(conf, topoConf, topology);
        assertNull(normalized.get(Config.STORM_WORKERS_ARTIFACTS_DIR));
    }

    @Test
    void testCreateStateInZookeeper() throws TException {
        nimbus.createStateInZookeeper(BLOB_FILE_KEY);

        verify(stormClusterState).setupBlob(eq(BLOB_FILE_KEY), eq(nimbusInfo), any());
    }

    @Test
    void testCreateStateInZookeeperWithoutLocalFsBlobStoreInstanceShouldNotCreate() throws Exception {
        BlobStore blobStore = mock(BlobStore.class);
        Map<String, Object> conf = Map.of(DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10);
        nimbus = new Nimbus(conf, iNimbus, stormClusterState, nimbusInfo, blobStore, leaderElector, groupMapper, metricRegistry);

        nimbus.createStateInZookeeper(BLOB_FILE_KEY);

        verify(stormClusterState, never()).setupBlob(eq(BLOB_FILE_KEY), eq(nimbusInfo), any());
    }

    @Test
    void testCreateStateInZookeeperWhenFailToSetupBlobWithRuntimeExceptionThrowsRuntimeException() {
        doThrow(new RuntimeException("Failed to setup blob")).when(stormClusterState).setupBlob(eq(BLOB_FILE_KEY), eq(nimbusInfo), any());

        assertThrows(RuntimeException.class, () -> nimbus.createStateInZookeeper(BLOB_FILE_KEY));
        verify(stormClusterState).setupBlob(eq(BLOB_FILE_KEY), eq(nimbusInfo), any());
    }

    @Test
    void testCreateStateInZookeeperWhenKeyNotFoundHandlesException() throws Exception {
        try (MockedConstruction<KeySequenceNumber> keySequenceNumber = mockConstruction(KeySequenceNumber.class, (mock, context) ->
                when(mock.getKeySequenceNumber(any())).thenThrow(new KeyNotFoundException("Failed to setup blob")))) {
            nimbus.createStateInZookeeper(BLOB_FILE_KEY);

            verify(keySequenceNumber.constructed().get(0)).getKeySequenceNumber(any());
            verify(stormClusterState, never()).setupBlob(eq(BLOB_FILE_KEY), eq(nimbusInfo), any());
        }
    }
}
