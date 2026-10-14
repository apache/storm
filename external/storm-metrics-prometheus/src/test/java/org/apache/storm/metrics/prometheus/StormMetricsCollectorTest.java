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

import io.prometheus.metrics.model.snapshots.Labels;
import java.util.Map;
import org.apache.storm.metrics.prometheus.StormMetricsCollector.Name;
import org.junit.jupiter.api.Test;

class StormMetricsCollectorTest {

    @Test
    void scopedMetric() {
        assertEquals(new Name("fetcher_counter", Labels.of("scope", "status_200")),
                StormMetricsCollector.parse("fetcher_counter.status_200", null));
        // only the first dot separates the scope
        assertEquals(new Name("fetcher_counter", Labels.of("scope", "robots.fetched")),
                StormMetricsCollector.parse("fetcher_counter.robots.fetched", null));
    }

    @Test
    void plainMetric() {
        assertEquals(new Name("activethreads", Labels.EMPTY),
                StormMetricsCollector.parse("activethreads", null));
        assertEquals(new Name("trailing_", Labels.EMPTY),
                StormMetricsCollector.parse("trailing.", null));
    }

    @Test
    void stormMetricWithStream() {
        assertEquals(new Name("storm_emit_count", Labels.EMPTY),
                StormMetricsCollector.parse("__emit-count-default", "default"));
        assertEquals(new Name("storm_capacity", Labels.EMPTY),
                StormMetricsCollector.parse("__capacity", "default"));
    }

    @Test
    void stormMetricWithSourceComponent() {
        assertEquals(new Name("storm_ack_count", Labels.of("source_component", "spout")),
                StormMetricsCollector.parse("__ack-count-spout:default", "default"));
        assertEquals(new Name("storm_process_latency", Labels.of("source_component", "url-partitioner")),
                StormMetricsCollector.parse("__process-latency-url-partitioner:status", "status"));
    }

    @Test
    void stormMetricWithoutStream() {
        assertEquals(new Name("storm_receive_population", Labels.EMPTY),
                StormMetricsCollector.parse("__receive.population", null));
    }

    @Test
    void stormConnectionMetric() {
        assertEquals(new Name("storm_send_iconnection_sent",
                        Labels.of("remote_host", "worker-2.example.com", "remote_port", "6701")),
                StormMetricsCollector.parse("__send-iconnection-sent-worker-2.example.com:6701", null));
        assertEquals(new Name("storm_send_iconnection_lostOnSend",
                        Labels.of("remote_host", "10.0.0.5", "remote_port", "6700")),
                StormMetricsCollector.parse("__send-iconnection-lostOnSend-10.0.0.5:6700", null));
    }

    @Test
    void sanitize() {
        assertEquals("bytes_fetched_perSec", StormMetricsCollector.sanitize("bytes_fetched_perSec"));
        assertEquals("GC_G1_Young_Generation", StormMetricsCollector.sanitize("GC.G1-Young-Generation"));
        assertEquals("_2xx", StormMetricsCollector.sanitize("2xx"));
        assertEquals("pages", StormMetricsCollector.sanitize("pages_total"));
    }

    @Test
    void taskLabels() {
        Map<String, String> dimensions = Map.of(
                "topologyId", "wordcount-1-1700000000",
                "hostname", "worker1",
                "port", "6700",
                "componentId", "fetcher",
                "taskid", "3");
        assertEquals(Labels.of(
                        "component", "fetcher",
                        "host", "worker1",
                        "port", "6700",
                        "task", "3",
                        "topology", "wordcount"),
                StormMetricsCollector.taskLabels("wordcount", dimensions));
    }
}
