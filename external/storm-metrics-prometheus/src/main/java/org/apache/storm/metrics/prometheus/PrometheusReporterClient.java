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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import io.prometheus.metrics.exporter.pushgateway.PushGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * This reporter pushes common cluster metrics towards a Prometheus Pushgateway.
 */
public class PrometheusReporterClient extends ScheduledReporter {
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusReporterClient.class);

    private static final Map<String, Object> CLUSTER_SUMMARY_METRICS = new HashMap<>();

    private static final TimeUnit DURATION_UNIT = TimeUnit.MILLISECONDS;
    private static final TimeUnit RATE_UNIT = TimeUnit.SECONDS;

    private final PushGateway prometheus;

    /**
     * Creates a new {@link PrometheusReporterClient} instance.
     *
     * @param registry   the {@link MetricRegistry} containing the metrics this
     *                   reporter will report
     * @param prometheus the {@link PushGateway} which is responsible for sending metrics
     *                   via a transport protocol
     */
    protected PrometheusReporterClient(MetricRegistry registry, PushGateway prometheus) {
        super(registry, "prometheus-reporter", MetricFilter.ALL, RATE_UNIT, DURATION_UNIT, null, true, Collections.emptySet());
        this.prometheus = prometheus;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        try {
            if (CLUSTER_SUMMARY_METRICS.isEmpty()) {
                initClusterMetrics();
            }

            for (Map.Entry<String, Gauge> e : gauges.entrySet()) {

                final io.prometheus.metrics.core.metrics.Gauge pGauge = (io.prometheus.metrics.core.metrics.Gauge) CLUSTER_SUMMARY_METRICS.get(e.getKey());
                if (pGauge != null) {
                    try {
                        pGauge.set(toDouble(e.getValue().getValue()));
                    } catch (NumberFormatException ignored) {
                        LOG.warn("Invalid type for Gauge {}: {}", e.getKey(), e.getValue().getClass().getName());
                    }
                }
            }

            for (Map.Entry<String, Histogram> e : histograms.entrySet()) {
                final io.prometheus.metrics.core.metrics.Histogram pHisto = (io.prometheus.metrics.core.metrics.Histogram) CLUSTER_SUMMARY_METRICS.get(e.getKey());
                if (pHisto != null) {
                    final Snapshot s = e.getValue().getSnapshot();
                    for (double d : s.getValues()) {
                        pHisto.observe(d);
                    }
                }
            }

            // Counters, Timers and Meters are not implemented (yet),
            // since we don't need them for Cluster Summary Metrics, cf. https://storm.apache.org/releases/current/ClusterMetrics.html

            prometheus.push();
        } catch (IOException e) {
            LOG.warn("Failed to push metrics to configured Prometheus Pushgateway,", e);
        }
    }

    private double toDouble(Object obj) {
        double value;
        if (obj instanceof Number) {
            value = ((Number) obj).doubleValue();
        } else if (obj instanceof Boolean) {
            value = ((Boolean) obj) ? 1 : 0;
        } else {
            value = Double.parseDouble(obj.toString());
        }
        return value;

    }

    private static void initClusterMetrics() {
        CLUSTER_SUMMARY_METRICS.put("summary.cluster:num-nimbus-leaders", io.prometheus.metrics.core.metrics.Gauge.builder()
                .name("summary_cluster_num_nimbus_leaders")
                .help("Number of nimbuses marked as a leader. This should really only ever be 1 in a healthy cluster, or 0 for a short period of time while a fail over happens.")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.cluster:num-nimbuses", io.prometheus.metrics.core.metrics.Gauge.builder()
                .name("summary_cluster_num_nimbuses")
                .help("Number of nimbuses, leader or standby.")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.cluster:num-supervisors", io.prometheus.metrics.core.metrics.Gauge.builder()
                .name("summary_cluster_num_supervisors")
                .help("Number of supervisors.")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.cluster:num-topologies", io.prometheus.metrics.core.metrics.Gauge.builder()
                .name("summary_cluster_num_topologies")
                .help("Number of topologies.")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.cluster:num-total-used-workers", io.prometheus.metrics.core.metrics.Gauge.builder()
                .name("summary_cluster_num_total_used_workers")
                .help("Number of used workers/slots.")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.cluster:num-total-workers", io.prometheus.metrics.core.metrics.Gauge.builder()
                .name("summary_cluster_num_total_workers")
                .help("Number of workers/slots.")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.cluster:total-fragmented-cpu-non-negative", io.prometheus.metrics.core.metrics.Gauge.builder()
                .name("summary_cluster_total_fragmented_cpu_non_negative")
                .help("Total fragmented CPU (% of core). This is CPU that the system thinks it cannot use because other resources on the node are used up.")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.cluster:total-fragmented-memory-non-negative", io.prometheus.metrics.core.metrics.Gauge.builder()
                .name("summary_cluster_total_fragmented_memory_non_negative")
                .help("Total fragmented memory (MB). This is memory that the system thinks it cannot use because other resources on the node are used up.")
                .register());

        CLUSTER_SUMMARY_METRICS.put("nimbus:available-cpu-non-negative", io.prometheus.metrics.core.metrics.Gauge.builder()
                .name("nimbus_available_cpu_non_negative")
                .help("Available cpu on the cluster (% of a core).")
                .register());

        CLUSTER_SUMMARY_METRICS.put("nimbus:total-cpu", io.prometheus.metrics.core.metrics.Gauge.builder()
                .name("nimbus_total_cpu")
                .help("total CPU on the cluster (% of a core)")
                .register());

        CLUSTER_SUMMARY_METRICS.put("nimbus:total-memory", io.prometheus.metrics.core.metrics.Gauge.builder()
                .name("nimbus_total_memory")
                .help("total memory on the cluster MB")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.topologies:assigned-cpu", io.prometheus.metrics.core.metrics.Histogram.builder()
                .name("summary_topologies_assigned_cpu")
                .help("CPU scheduled per topology (% of a core)")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.topologies:assigned-mem-off-heap", io.prometheus.metrics.core.metrics.Histogram.builder()
                .name("summary_topologies_assigned_mem_off_heap")
                .help("Off heap memory scheduled per topology (MB)")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.topologies:assigned-mem-on-heap", io.prometheus.metrics.core.metrics.Histogram.builder()
                .name("summary_topologies_assigned_mem_on_heap")
                .help("On heap memory scheduled per topology (MB)")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.topologies:num-executors", io.prometheus.metrics.core.metrics.Histogram.builder()
                .name("summary_topologies_num_executors")
                .help("Number of executors per topology")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.topologies:num-tasks", io.prometheus.metrics.core.metrics.Histogram.builder()
                .name("summary_topologies_num_tasks")
                .help("Number of tasks per topology")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.topologies:num-workers", io.prometheus.metrics.core.metrics.Histogram.builder()
                .name("summary_topologies_num_workers")
                .help("Number of workers per topology")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.topologies:replication-count", io.prometheus.metrics.core.metrics.Histogram.builder()
                .name("summary_topologies_replication_count")
                .help("Replication count per topology")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.topologies:requested-cpu", io.prometheus.metrics.core.metrics.Histogram.builder()
                .name("summary_topologies_requested_cpu")
                .help("CPU requested per topology (% of a core)")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.topologies:requested-mem-off-heap", io.prometheus.metrics.core.metrics.Histogram.builder()
                .name("summary_topologies_requested_mem_off_heap")
                .help("Off heap memory requested per topology (MB)")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.topologies:requested-mem-on-heap", io.prometheus.metrics.core.metrics.Histogram.builder()
                .name("summary_topologies_requested_mem_on_heap")
                .help("On heap memory requested per topology (MB)")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.topologies:uptime-secs", io.prometheus.metrics.core.metrics.Histogram.builder()
                .name("summary_topologies_uptime_secs")
                .help("Uptime per topology (seconds)")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.supervisors:fragmented-cpu", io.prometheus.metrics.core.metrics.Histogram.builder()
                .name("supervisors_fragmented_cpu")
                .help("fragmented CPU per supervisor (% of a core)")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.supervisors:fragmented-mem", io.prometheus.metrics.core.metrics.Histogram.builder()
                .name("supervisors_fragmented_mem")
                .help("fragmented memory per supervisor (MB)")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.supervisors:num-used-workers", io.prometheus.metrics.core.metrics.Histogram.builder()
                .name("supervisors_num_used_workers")
                .help("workers used per supervisor")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.supervisors:num-workers", io.prometheus.metrics.core.metrics.Histogram.builder()
                .name("supervisors_num_workers")
                .help("number of workers per supervisor")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.supervisors:uptime-secs", io.prometheus.metrics.core.metrics.Histogram.builder()
                .name("supervisors_uptime_secs")
                .help("uptime of supervisors")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.supervisors:used-cpu", io.prometheus.metrics.core.metrics.Histogram.builder()
                .name("supervisors_used_cpu")
                .help("CPU used per supervisor (% of a core)")
                .register());

        CLUSTER_SUMMARY_METRICS.put("summary.supervisors:used-mem", io.prometheus.metrics.core.metrics.Histogram.builder()
                .name("supervisors_used_mem")
                .help("memory used per supervisor (MB)")
                .register());

    }

}
