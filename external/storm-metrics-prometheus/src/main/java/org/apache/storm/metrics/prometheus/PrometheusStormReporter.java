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

import com.codahale.metrics.MetricRegistry;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.storm.Config;
import org.apache.storm.metrics2.MetricRegistryProvider;
import org.apache.storm.metrics2.reporters.ScheduledStormReporter;
import org.apache.storm.metrics2.reporters.StormReporter;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics V2 reporter serving the metrics of a worker over HTTP for Prometheus to scrape.
 *
 * <pre>
 * topology.metrics.reporters:
 *   - class: "org.apache.storm.metrics.prometheus.PrometheusStormReporter"
 *     port.offset: 1000
 * </pre>
 *
 * <p>Each worker listens on its own port, which is the worker port plus <code>port.offset</code>,
 * e.g. 7700 for a worker on 6700. When the worker port is unknown, as in local mode, the endpoint
 * listens on <code>port</code>, which can also be set explicitly. The workers of a local cluster
 * share the same endpoint.
 */
public class PrometheusStormReporter implements StormReporter {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusStormReporter.class);

    /** Port to listen on, takes precedence over {@link #PORT_OFFSET_KEY}. */
    public static final String PORT_KEY = "port";

    /** Added to the worker port to get the port to listen on, 1000 by default. */
    public static final String PORT_OFFSET_KEY = "port.offset";

    /** Address to bind to, all interfaces by default. */
    public static final String HOST_KEY = "host";

    /** Whether to expose the JVM metrics (memory, GC, threads, start time), true by default. */
    public static final String JVM_METRICS_KEY = "jvm.metrics";

    static final int DEFAULT_PORT_OFFSET = 1000;

    /** Port used when the worker port is unknown, e.g. in local mode. */
    static final int DEFAULT_PORT = 9400;

    /** System property set by the supervisor when launching a worker. */
    private static final String WORKER_PORT_PROPERTY = "worker.port";

    /** Endpoints by port, shared by the reporters within a JVM. */
    private static final Map<Integer, Endpoint> ENDPOINTS = new HashMap<>();

    private StormMetricsCollector.Source source;
    private int port;
    private String host;
    private boolean jvmMetrics;

    @Override
    public void prepare(MetricRegistryProvider metricRegistryProvider, Map<String, Object> topoConf,
                        Map<String, Object> reporterConf) {
        String topology = ObjectReader.getString(topoConf.get(Config.TOPOLOGY_NAME),
                ObjectReader.getString(topoConf.get(Config.STORM_ID), null));
        source = new StormMetricsCollector.Source(metricRegistryProvider, topology,
                ScheduledStormReporter.getMetricsFilter(reporterConf));
        port = resolvePort(reporterConf, System.getProperty(WORKER_PORT_PROPERTY));
        host = ObjectReader.getString(reporterConf.get(HOST_KEY), null);
        jvmMetrics = ObjectReader.getBoolean(reporterConf.get(JVM_METRICS_KEY), true);
    }

    @Override
    public void prepare(MetricRegistry metricsRegistry, Map<String, Object> topoConf,
                        Map<String, Object> reporterConf) {
        throw new UnsupportedOperationException(
                "PrometheusStormReporter needs a MetricRegistryProvider to get the task "
                        + "dimensions");
    }

    static int resolvePort(Map<String, Object> reporterConf, String workerPort) {
        Integer port = ObjectReader.getInt(reporterConf.get(PORT_KEY), null);
        if (port != null) {
            if (workerPort != null) {
                LOG.warn("{} is set to {} for worker port {}, workers sharing this host "
                        + "will fail to bind, use {} instead",
                        PORT_KEY, port, workerPort, PORT_OFFSET_KEY);
            }
            return port;
        }
        if (workerPort == null) {
            return DEFAULT_PORT;
        }
        int offset = ObjectReader.getInt(reporterConf.get(PORT_OFFSET_KEY), DEFAULT_PORT_OFFSET);
        return Integer.parseInt(workerPort) + offset;
    }

    @Override
    public void start() {
        synchronized (ENDPOINTS) {
            Endpoint endpoint = ENDPOINTS.get(port);
            if (endpoint == null) {
                try {
                    endpoint = new Endpoint(port, host, jvmMetrics);
                } catch (IOException | IllegalArgumentException e) {
                    LOG.error("Could not serve the metrics on port {}", port, e);
                    return;
                }
                ENDPOINTS.put(port, endpoint);
                LOG.info("Serving the metrics for Prometheus on port {}",
                        endpoint.server.getPort());
            } else if (!Objects.equals(host, endpoint.host) || jvmMetrics != endpoint.jvmMetrics) {
                LOG.warn("Endpoint on port {} already runs with host={} and jvm.metrics={}, "
                        + "ignoring host={} and jvm.metrics={}",
                        port, endpoint.host, endpoint.jvmMetrics, host, jvmMetrics);
            }
            endpoint.collector.add(source);
        }
    }

    @Override
    public void stop() {
        synchronized (ENDPOINTS) {
            Endpoint endpoint = ENDPOINTS.get(port);
            if (endpoint == null) {
                return;
            }
            endpoint.collector.remove(source);
            if (endpoint.collector.isEmpty()) {
                endpoint.server.close();
                ENDPOINTS.remove(port);
            }
        }
    }

    @Override
    public void close() {
        stop();
    }

    /** Port the endpoint of this reporter is bound to, -1 if it is not running. */
    int getBoundPort() {
        synchronized (ENDPOINTS) {
            Endpoint endpoint = ENDPOINTS.get(port);
            return endpoint == null ? -1 : endpoint.server.getPort();
        }
    }

    private static final class Endpoint {

        private final StormMetricsCollector collector = new StormMetricsCollector();
        private final HTTPServer server;
        private final String host;
        private final boolean jvmMetrics;

        Endpoint(int port, String host, boolean jvmMetrics) throws IOException {
            this.host = host;
            this.jvmMetrics = jvmMetrics;
            PrometheusRegistry registry = new PrometheusRegistry();
            registry.register(collector);
            if (jvmMetrics) {
                registry.register((MultiCollector) JvmRegistry.INSTANCE::scrape);
            }
            HTTPServer.Builder builder = HTTPServer.builder().port(port).registry(registry);
            if (host != null) {
                builder.hostname(host);
            }
            server = builder.buildAndStart();
        }
    }

    /** JVM metrics registered once per JVM, as {@link JvmMetrics} never releases a registry. */
    private static final class JvmRegistry {

        static final PrometheusRegistry INSTANCE = new PrometheusRegistry();

        static {
            JvmMetrics.builder().register(INSTANCE);
        }
    }
}
