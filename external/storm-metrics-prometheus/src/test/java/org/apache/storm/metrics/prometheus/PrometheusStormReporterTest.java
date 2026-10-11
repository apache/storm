/*
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

package org.apache.storm.metrics.prometheus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.storm.Config;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.task.TopologyContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PrometheusStormReporterTest {

    private static final String TOPOLOGY_ID = "wordcount-1-1700000000";
    private static final int WORKER_PORT = 6700;

    private StormMetricRegistry registry;
    private PrometheusStormReporter reporter;

    @BeforeEach
    void setUp() {
        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(Config.STORM_ID, TOPOLOGY_ID);
        topoConf.put(Config.TOPOLOGY_NAME, "wordcount");
        registry = new StormMetricRegistry();
        registry.start(topoConf, WORKER_PORT);

        reporter = new PrometheusStormReporter();
        reporter.prepare(registry, topoConf, Map.of("port", 0, "jvm.metrics", false));
        reporter.start();
    }

    @AfterEach
    void tearDown() {
        reporter.stop();
        registry.stop();
    }

    @Test
    void servesTaskMetrics() throws Exception {
        registry.counter("fetcher_counter.status_200", "fetcher", 3).inc(5);
        registry.gauge("activethreads", (Gauge<Integer>) () -> 7, "fetcher", 3);
        registry.gauge("status.count",
                (Gauge<Map<String, Long>>) () -> Map.of("FETCHED", 12L, "DISCOVERED", 30L),
                "status", 4);
        registry.rateCounter("__ack-count-spout:default", TOPOLOGY_ID, "status", 4, WORKER_PORT,
                "default").inc(2);

        TopologyContext context = mock(TopologyContext.class);
        when(context.getStormId()).thenReturn(TOPOLOGY_ID);
        when(context.getThisComponentId()).thenReturn("fetcher");
        when(context.getThisTaskId()).thenReturn(3);
        when(context.getThisWorkerPort()).thenReturn(WORKER_PORT);
        Histogram histogram = registry.histogram("fetcher_average_perdoc.time_in_queues", context);
        histogram.update(100);
        histogram.update(300);
        registry.meter("fetcher_average_persec.bytes_fetched_perSec", context).mark(1000);
        registry.timer("query_time", context).update(2, TimeUnit.SECONDS);

        String body = scrape();

        Map<String, String> fetcher =
                Map.of("topology", "wordcount", "component", "fetcher", "task", "3", "port", "6700");
        assertSample(body, "fetcher_counter_total", fetcher, "scope", "status_200", 5);
        assertSample(body, "activethreads", fetcher, null, null, 7);
        assertSample(body, "status_count", Map.of("component", "status"), "key", "FETCHED", 12);
        assertSample(body, "status_count", Map.of("component", "status"), "key", "DISCOVERED", 30);
        assertSample(body, "storm_ack_count_total", Map.of("component", "status", "stream", "default"),
                "source_component", "spout", 2);
        assertFalse(body.contains("m1_rate"), body);
        assertSample(body, "fetcher_average_perdoc_count", fetcher, "scope", "time_in_queues", 2);
        assertSample(body, "fetcher_average_perdoc_max", fetcher, "scope", "time_in_queues", 300);
        assertSample(body, "fetcher_average_perdoc_mean", fetcher, "scope", "time_in_queues", 200);
        assertSample(body, "fetcher_average_persec_total", fetcher, "scope",
                "bytes_fetched_perSec", 1000);
        assertSample(body, "query_time_seconds_max", fetcher, null, null, 2);
    }

    @Test
    void resolvePort() {
        assertEquals(PrometheusStormReporter.DEFAULT_PORT,
                PrometheusStormReporter.resolvePort(Map.of(), null));
        assertEquals(7700, PrometheusStormReporter.resolvePort(Map.of(), "6700"));
        assertEquals(6800, PrometheusStormReporter.resolvePort(Map.of("port.offset", 100), "6700"));
        assertEquals(9999, PrometheusStormReporter.resolvePort(Map.of("port", 9999), "6700"));
    }

    private String scrape() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(
                URI.create("http://localhost:" + reporter.getBoundPort() + "/metrics")).build();
        HttpResponse<String> response =
                HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        return response.body();
    }

    /** Checks that a line of the text format has the given name, labels and value. */
    private static void assertSample(String body, String name, Map<String, String> labels,
                                     String extraLabel, String extraValue, double expected) {
        Map<String, String> wanted = new HashMap<>(labels);
        if (extraLabel != null) {
            wanted.put(extraLabel, extraValue);
        }
        for (String line : body.split("\n")) {
            if (!line.startsWith(name + "{")) {
                continue;
            }
            String labelPart = line.substring(name.length() + 1, line.lastIndexOf('}'));
            boolean matches = wanted.entrySet().stream()
                    .allMatch(e -> labelPart.contains(e.getKey() + "=\"" + e.getValue() + "\""));
            if (matches) {
                double value = Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
                assertEquals(expected, value, 0.0001, line);
                return;
            }
        }
        fail("No sample " + name + " with labels " + wanted + " in\n" + body);
    }
}
