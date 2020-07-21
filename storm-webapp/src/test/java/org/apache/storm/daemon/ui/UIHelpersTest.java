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

package org.apache.storm.daemon.ui;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.generated.BoltAggregateStats;
import org.apache.storm.generated.CommonAggregateStats;
import org.apache.storm.generated.ComponentAggregateStats;
import org.apache.storm.generated.ErrorInfo;
import org.apache.storm.generated.SpecificAggregateStats;
import org.apache.storm.generated.TopologyPageInfo;
import org.apache.storm.generated.TopologyStats;
import org.json.simple.JSONValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UIHelpersTest {
    private static final String TOPOLOGY_ID = "Test-Topology-Id";
    private static final long TOPOLOGY_MESSAGE_TIMEOUT_SECS = 100L;
    private static final String WINDOW = ":all-time";

    /**
     * Default empty TopologyPageInfo instance to be extended in each test case.
     */
    private TopologyPageInfo topoPageInfo;

    /**
     * Setups up bare minimum TopologyPageInfo instance such that we can pass to
     * UIHelpers.getTopologySummary() without it throwing a NPE.
     *
     * This should provide a base for which other tests can be written, but will
     * require populating additional values as needed for each test case.
     */
    @BeforeEach
    void setup() {
        // Create topology config and serialize to JSON.
        final Map<String, Object> topologyConfig = new HashMap<>();
        topologyConfig.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, TOPOLOGY_MESSAGE_TIMEOUT_SECS);
        final String topoConfigJson = JSONValue.toJSONString(topologyConfig);

        // Create empty TopologyStats instance
        final TopologyStats topologyStats = new TopologyStats();
        topologyStats.set_window_to_emitted(new HashMap<>());
        topologyStats.set_window_to_transferred(new HashMap<>());
        topologyStats.set_window_to_acked(new HashMap<>());
        topologyStats.set_window_to_complete_latencies_ms(new HashMap<>());
        topologyStats.set_window_to_failed(new HashMap<>());

        // Create empty AggregateStats instances.
        final Map<String,ComponentAggregateStats> idToSpoutAggStats = new HashMap<>();

        final Map<String,ComponentAggregateStats> idToBoltAggStats = new HashMap<>();

        // Build up TopologyPageInfo instance
        topoPageInfo = new TopologyPageInfo();
        topoPageInfo.set_topology_conf(topoConfigJson);
        topoPageInfo.set_id(TOPOLOGY_ID);
        topoPageInfo.set_topology_stats(topologyStats);
        topoPageInfo.set_id_to_spout_agg_stats(idToSpoutAggStats);
        topoPageInfo.set_id_to_bolt_agg_stats(idToBoltAggStats);
    }

    /**
     * Very narrow test case to validate that 'last error' fields are populated for a bolt
     * with an error is present.
     */
    @Test
    void test_getTopologyBoltAggStatsMap_includesLastError() {
        // Define inputs
        final String expectedBoltId = "MyBoltId";
        final String expectedErrorMsg = "This is my test error message";
        final int expectedErrorTime = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        final int expectedErrorPort = 4321;
        final String expectedErrorHost = "my.errored.host";

        // Define our Last Error
        final ErrorInfo expectedLastError = new ErrorInfo(expectedErrorMsg, expectedErrorTime);
        expectedLastError.set_port(expectedErrorPort);
        expectedLastError.set_host(expectedErrorHost);

        // Build stats instance for our bolt
        final ComponentAggregateStats aggregateStats = buildAggregateStatsBase();
        aggregateStats.set_last_error(expectedLastError);
        addBoltStats(expectedBoltId, aggregateStats);

        // Call method under test.
        final Map<String, Object> result = UIHelpers.getTopologySummary(
            topoPageInfo,
            WINDOW,
            new HashMap<>(),
            "spp"
        );

        // Validate
        assertNotNull(result, "Should never return null");

        // Validate our Bolt result
        final Map<String, Object> boltResult = getBoltStatsFromTopologySummaryResult(result, expectedBoltId);
        assertNotNull(boltResult, "Should have an entry for bolt");

        // Verify each piece
        assertEquals(expectedBoltId, boltResult.get("boltId"));
        assertEquals(expectedBoltId, boltResult.get("encodedBoltId"));

        // Verify error
        assertEquals(expectedErrorMsg, boltResult.get("error"));
        assertEquals(expectedErrorMsg, boltResult.get("lastError"));
        assertEquals(expectedErrorPort, boltResult.get("errorPort"));
        assertEquals(expectedErrorHost, boltResult.get("errorHost"));
        assertEquals(expectedErrorTime, boltResult.get("errorTime"));

        // Fuzzy matching
        assertTrue(((int) boltResult.get("errorLapsedSecs")) >= 0);
        assertTrue(((int) boltResult.get("errorLapsedSecs")) <= 5);
    }

    /**
     * Very narrow test case to validate that 'last error' fields are NOT populated for a bolt
     * that does NOT have a last error associated.
     */
    @Test
    void test_getTopologyBoltAggStatsMap_hasNoLastError() {
        // Define inputs
        final String expectedBoltId = "MyBoltId";

        // Build stats instance for our bolt
        final ComponentAggregateStats aggregateStats = buildAggregateStatsBase();
        addBoltStats(expectedBoltId, aggregateStats);

        // Call method under test.
        final Map<String, Object> result = UIHelpers.getTopologySummary(
            topoPageInfo,
            WINDOW,
            new HashMap<>(),
            "spp"
        );

        // Validate
        assertNotNull(result, "Should never return null");

        // Validate our Bolt result
        final Map<String, Object> boltResult = getBoltStatsFromTopologySummaryResult(result, expectedBoltId);
        assertNotNull(boltResult, "Should have an entry for bolt");

        // Verify each piece
        assertEquals(expectedBoltId, boltResult.get("boltId"));
        assertEquals(expectedBoltId, boltResult.get("encodedBoltId"));

        // Verify error fields are not populated.
        assertEquals("", boltResult.get("lastError"), "Should have empty value");
        assertFalse(boltResult.containsKey("error"));
        assertFalse(boltResult.containsKey("errorPort"));
        assertFalse(boltResult.containsKey("errorHost"));
        assertFalse(boltResult.containsKey("errorTime"));
        assertFalse(boltResult.containsKey("errorLapsedSecs"));
    }

    /**
     * A more general test case that a bolt's aggregate stats are
     * correctly populated into the resulting map.
     */
    @Test
    void test_getTopologyBoltAggStatsMap_generalFields() {
        // Define inputs
        final String expectedBoltId = "MyBoltId";
        final float expectedCapacity = 0.97f;
        final double expectedProcessLatency = 432.0D;
        final double expectedExecuteLatency = 122.0D;
        final long expectedExecuted = 153343L;
        final long expectedEmitted = 43234L;
        final long expectedAcked = 5553L;
        final long expectedFailed = 220L;
        final int expectedExecutors = 2;
        final int expectedTasks = 3;
        final long expectedTransferred = 3423423L;
        final double expectedOnMemoryHeap = 1024D;
        final double expectedOffMemoryHeap = 2048D;
        final double expectedCpuCorePercent = 75D;

        // Build stats instance for our bolt
        final ComponentAggregateStats aggregateStats = buildAggregateStatsBase();

        // Common stats
        final CommonAggregateStats commonStats = aggregateStats.get_common_stats();
        commonStats.set_acked(expectedAcked);
        commonStats.set_emitted(expectedEmitted);
        commonStats.set_failed(expectedFailed);
        commonStats.set_num_executors(expectedExecutors);
        commonStats.set_num_tasks(expectedTasks);
        commonStats.set_transferred(expectedTransferred);

        // Bolt stats
        final BoltAggregateStats boltStats = aggregateStats.get_specific_stats().get_bolt();
        boltStats.set_capacity(expectedCapacity);
        boltStats.set_execute_latency_ms(expectedExecuteLatency);
        boltStats.set_process_latency_ms(expectedProcessLatency);
        boltStats.set_executed(expectedExecuted);

        // Build Resources Map
        final Map<String, Double> resourcesMap = new HashMap<>();
        resourcesMap.put(Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME, expectedOnMemoryHeap);
        resourcesMap.put(Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME, expectedOffMemoryHeap);
        resourcesMap.put(Constants.COMMON_CPU_RESOURCE_NAME, expectedCpuCorePercent);
        commonStats.set_resources_map(resourcesMap);

        // Add to TopologyPageInfo
        addBoltStats(expectedBoltId, aggregateStats);

        // Call method under test.
        final Map<String, Object> result = UIHelpers.getTopologySummary(
            topoPageInfo,
            WINDOW,
            new HashMap<>(),
            "spp"
        );

        // Validate
        assertNotNull(result, "Should never return null");

        // Validate our Bolt result
        final Map<String, Object> boltResult = getBoltStatsFromTopologySummaryResult(result, expectedBoltId);
        assertNotNull(boltResult, "Should have an entry for bolt");

        // Validate fields
        assertEquals(expectedBoltId, boltResult.get("boltId"));
        assertEquals(expectedBoltId, boltResult.get("encodedBoltId"));
        assertEquals(expectedTransferred, boltResult.get("transferred"));
        assertEquals(String.format("%.3f", expectedExecuteLatency), boltResult.get("executeLatency"));
        assertEquals(String.format("%.3f", expectedProcessLatency), boltResult.get("processLatency"));
        assertEquals(expectedExecuted, boltResult.get("executed"));
        assertEquals(expectedFailed, boltResult.get("failed"));
        assertEquals(expectedAcked, boltResult.get("acked"));
        assertEquals(String.format("%.3f", expectedCapacity), boltResult.get("capacity"));
        assertEquals(expectedEmitted, boltResult.get("emitted"));
        assertEquals(expectedExecutors, boltResult.get("executors"));
        assertEquals(expectedTasks, boltResult.get("tasks"));

        // Validate resources
        assertEquals(expectedOnMemoryHeap, (double) boltResult.get("requestedMemOnHeap"), 0.01);
        assertEquals(expectedOffMemoryHeap, (double) boltResult.get("requestedMemOffHeap"), 0.01);
        assertEquals(expectedCpuCorePercent, (double) boltResult.get("requestedCpu"), 0.01);
        assertEquals("", boltResult.get("requestedGenericResourcesComp"));

        // We expect there to be no error populated.
        assertEquals("", boltResult.get("lastError"), "Should have empty value");
    }

    /**
     * Add an AggregateStats entry to the TopologyPageInfo instance.
     * @param boltId Id of the bolt to add the entry for.
     * @param aggregateStats Defines the entry.
     */
    private void addBoltStats(final String boltId, final ComponentAggregateStats aggregateStats) {
        topoPageInfo.get_id_to_bolt_agg_stats().put(boltId, aggregateStats);
    }

    /**
     * Builds an empty ComponentAggregateStats instance.
     * @return empty ComponentAggregateStats instance.
     */
    private ComponentAggregateStats buildAggregateStatsBase() {
        final CommonAggregateStats commonStats = new CommonAggregateStats();
        final BoltAggregateStats boltAggregateStats = new BoltAggregateStats();

        final SpecificAggregateStats specificStats = new SpecificAggregateStats();
        specificStats.set_bolt(boltAggregateStats);

        final ComponentAggregateStats aggregateStats = new ComponentAggregateStats();
        aggregateStats.set_common_stats(commonStats);
        aggregateStats.set_specific_stats(specificStats);

        return aggregateStats;
    }

    /**
     * Given the results Map from UIHelper.getTopologySummary(), return the entry for
     * the requested boltId.
     *
     * @param result Map from UIHelper.getTopologySummary()
     * @param boltId Id of the bolt to return the entry for.
     * @return Map for the given boltId.
     * @throws IllegalArgumentException if passed an invalid BoltId.
     */
    private Map<String, Object> getBoltStatsFromTopologySummaryResult(final Map<String, Object> result, final String boltId) {
        assertNotNull(result.get("bolts"), "Should have non-null 'bolts' property");
        final List<HashMap<String, Object>> bolts = (List<HashMap<String, Object>>) result.get("bolts");

        return bolts.stream()
            .filter((entry) -> boltId.equals(entry.get("boltId")))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unable to find entry for boltId '" + boltId + "'"));
    }
}