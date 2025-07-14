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
package org.apache.storm.metrics.prometheus;

import com.codahale.metrics.MetricRegistry;
import org.apache.storm.metrics2.SimpleGauge;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Testcontainers(disabledWithoutDocker = true)
public class PrometheusPreparableReporterTest {

    private GenericContainer<?> pushGatewayContainer;

    @BeforeEach
    public void setUp() {
        pushGatewayContainer = new GenericContainer<>("prom/pushgateway:v1.8.0")
                .withExposedPorts(9091)
                .waitingFor(Wait.forListeningPort());
    }

    @AfterEach
    public void tearDown() {
        pushGatewayContainer.stop();
    }

    @Test
    public void testSimple() throws IOException {
        pushGatewayContainer.start();

        final PrometheusPreparableReporter sut = new PrometheusPreparableReporter();

        final Map<String, Object> daemonConf = Map.of(
                "storm.daemon.metrics.reporter.plugin.prometheus.job", "test_simple",
                "storm.daemon.metrics.reporter.plugin.prometheus.endpoint", "localhost:" + pushGatewayContainer.getMappedPort(9091),
                "storm.daemon.metrics.reporter.plugin.prometheus.scheme", "http"
        );

        runTest(sut, daemonConf);

    }

    @Test
    public void testBasicAuth() throws IOException {
        pushGatewayContainer
                .withCopyFileToContainer(MountableFile.forClasspathResource("/pushgateway-basicauth.yaml"), "/pushgateway/pushgateway-basicauth.yaml")
                .withCommand("--web.config.file", "pushgateway-basicauth.yaml")
                .start();

        final PrometheusPreparableReporter sut = new PrometheusPreparableReporter();

        final Map<String, Object> daemonConf = Map.of(
                "storm.daemon.metrics.reporter.plugin.prometheus.job", "test_simple",
                "storm.daemon.metrics.reporter.plugin.prometheus.endpoint", "localhost:" + pushGatewayContainer.getMappedPort(9091),
                "storm.daemon.metrics.reporter.plugin.prometheus.scheme", "http",
                "storm.daemon.metrics.reporter.plugin.prometheus.basic_auth_user", "my_user",
                "storm.daemon.metrics.reporter.plugin.prometheus.basic_auth_password", "secret_password"
        );

        runTest(sut, daemonConf);
    }

    @Test
    public void testTls() throws IOException {
        pushGatewayContainer
                .withCopyFileToContainer(MountableFile.forClasspathResource("/pushgateway-ssl.yaml"), "/pushgateway/pushgateway-ssl.yaml")
                .withCommand("--web.config.file", "pushgateway-ssl.yaml")
                .start();

        final PrometheusPreparableReporter sut = new PrometheusPreparableReporter();

        final Map<String, Object> daemonConf = Map.of(
                "storm.daemon.metrics.reporter.plugin.prometheus.job", "test_simple",
                "storm.daemon.metrics.reporter.plugin.prometheus.endpoint", "localhost:" + pushGatewayContainer.getMappedPort(9091),
                "storm.daemon.metrics.reporter.plugin.prometheus.scheme", "https",
                "storm.daemon.metrics.reporter.plugin.prometheus.skip_tls_validation", true
        );

        runTest(sut, daemonConf);
    }


    private void runTest(PrometheusPreparableReporter sut, Map<String, Object> daemonConf) throws IOException {
        // We fake the metrics here. In a real Storm environment, these metrics are generated.
        final MetricRegistry r = new MetricRegistry();
        final SimpleGauge<Integer> supervisor = new SimpleGauge<>(5);
        r.register("summary.cluster:num-supervisors", supervisor);
        r.register("nimbus:total-memory", new SimpleGauge<>(5.6));
        r.register("nimbus:total-cpu", new SimpleGauge<>("500"));

        sut.prepare(r, daemonConf);

        //manually trigger a reporting here, in a real Storm environment, this is called by a scheduled executor.
        sut.getReporter().report();

        assertMetrics(
                List.of(
                        "# HELP summary_cluster_num_supervisors Number of supervisors.",
                        "# TYPE summary_cluster_num_supervisors gauge",
                        "summary_cluster_num_supervisors{instance=\"\",job=\"test_simple\"} 5",
                        "# HELP nimbus_total_memory total memory on the cluster MB",
                        "# TYPE nimbus_total_memory gauge",
                        "nimbus_total_memory{instance=\"\",job=\"test_simple\"} 5.6",
                        "# HELP nimbus_total_cpu total CPU on the cluster (% of a core)",
                        "# TYPE nimbus_total_cpu gauge",
                        "nimbus_total_cpu{instance=\"\",job=\"test_simple\"} 500"
                ), daemonConf.get("storm.daemon.metrics.reporter.plugin.prometheus.scheme") + "://" + daemonConf.get("storm.daemon.metrics.reporter.plugin.prometheus.endpoint") + "/metrics", daemonConf);

        //update a metric
        supervisor.set(100);

        //manually trigger a reporting here, in a real Storm environment, this is called by a scheduled executor.
        sut.getReporter().report();

        assertMetrics(
                List.of(
                        "# HELP summary_cluster_num_supervisors Number of supervisors.",
                        "# TYPE summary_cluster_num_supervisors gauge",
                        "summary_cluster_num_supervisors{instance=\"\",job=\"test_simple\"} 100",
                        "# HELP nimbus_total_memory total memory on the cluster MB",
                        "# TYPE nimbus_total_memory gauge",
                        "nimbus_total_memory{instance=\"\",job=\"test_simple\"} 5.6",
                        "# HELP nimbus_total_cpu total CPU on the cluster (% of a core)",
                        "# TYPE nimbus_total_cpu gauge",
                        "nimbus_total_cpu{instance=\"\",job=\"test_simple\"} 500"
                ), daemonConf.get("storm.daemon.metrics.reporter.plugin.prometheus.scheme") + "://" + daemonConf.get("storm.daemon.metrics.reporter.plugin.prometheus.endpoint") + "/metrics", daemonConf);
    }

    private void assertMetrics(List<String> elements, String endpoint, Map<String, Object> conf) throws IOException {
        final String content = readContent(endpoint, conf);
        Assertions.assertNotNull(content);
        final Set<String> contentLinesSet = new HashSet<>(Arrays.asList(content.split("\n")));
        elements.forEach(find -> Assertions.assertTrue(contentLinesSet.contains(find), "Did not find: " + find));
    }

    private String readContent(String url, Map<String, Object> conf) throws IOException {
        final URL obj = new URL(url);
        final HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod("GET");

        if (conf.containsKey("storm.daemon.metrics.reporter.plugin.prometheus.basic_auth_user")) {

            String auth = conf.get("storm.daemon.metrics.reporter.plugin.prometheus.basic_auth_user") + ":" + conf.get("storm.daemon.metrics.reporter.plugin.prometheus.basic_auth_password");
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
            String authHeaderValue = "Basic " + encodedAuth;
            con.setRequestProperty("Authorization", authHeaderValue);
        }


        try (BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
            String inputLine;
            StringBuilder response = new StringBuilder();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine).append("\n");
            }
            return response.toString();
        }

    }

}
