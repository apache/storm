package org.apache.storm.daemon.ui;

import org.apache.storm.Config;
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