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

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import io.prometheus.metrics.exporter.pushgateway.HttpConnectionFactory;
import io.prometheus.metrics.exporter.pushgateway.PushGateway;
import io.prometheus.metrics.exporter.pushgateway.Scheme;
import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.metrics.reporters.PreparableReporter;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class PrometheusPreparableReporter implements PreparableReporter {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusPreparableReporter.class);

    private static final TrustManager INSECURE_TRUST_MANAGER = new X509TrustManager() {

        @Override
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return null;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
        }
    };

    private static final HttpConnectionFactory INSECURE_CONNECTION_FACTORY = url -> {
        try {
            final SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[]{INSECURE_TRUST_MANAGER}, null);
            SSLContext.setDefault(sslContext);

            final HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
            connection.setHostnameVerifier((hostname, session) -> true);
            return connection;
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new RuntimeException(e);
        }
    };

    private PrometheusReporterClient reporter;
    private Integer reportingIntervalSecs;

    protected PrometheusReporterClient getReporter() {
        return reporter;
    }

    @Override
    public void prepare(MetricRegistry metricsRegistry, Map<String, Object> daemonConf) {
        if (daemonConf != null) {

            final String jobName = (String) daemonConf.getOrDefault("storm.daemon.metrics.reporter.plugin.prometheus.job", "storm");
            final String endpoint = (String) daemonConf.getOrDefault("storm.daemon.metrics.reporter.plugin.prometheus.endpoint", "localhost:9091");
            final String schemeAsString = (String) daemonConf.getOrDefault("storm.daemon.metrics.reporter.plugin.prometheus.scheme", "http");

            Scheme scheme = Scheme.HTTP;

            try {
                scheme = Scheme.fromString(schemeAsString);
            } catch (IllegalArgumentException iae) {
                LOG.warn("Unsupported scheme. Expecting 'http' or 'https'. Was: {}", schemeAsString);
            }

            final String basicAuthUser = (String) daemonConf.getOrDefault("storm.daemon.metrics.reporter.plugin.prometheus.basic_auth_user", "");
            final String basicAuthPassword = (String) daemonConf.getOrDefault("storm.daemon.metrics.reporter.plugin.prometheus.basic_auth_password", "");
            final boolean skipTlsValidation = (boolean) daemonConf.getOrDefault("storm.daemon.metrics.reporter.plugin.prometheus.skip_tls_validation", false);

            final PushGateway.Builder builder = PushGateway.builder();

            builder.address(endpoint).job(jobName);

            if (!basicAuthUser.isBlank() && !basicAuthPassword.isBlank()) {
                builder.basicAuth(basicAuthUser, basicAuthPassword);
            }

            builder.scheme(scheme);

            if (scheme == Scheme.HTTPS && skipTlsValidation) {
                builder.connectionFactory(INSECURE_CONNECTION_FACTORY);
            }

            final PushGateway pushGateway = builder.build();

            reporter = new PrometheusReporterClient(metricsRegistry, pushGateway);
            reportingIntervalSecs = ObjectReader.getInt(daemonConf.get(DaemonConfig.STORM_DAEMON_METRICS_REPORTER_INTERVAL_SECS), 10);
        } else {
            LOG.warn("No daemonConfiguration was supplied. Don't initialize.");
        }
    }


    @Override
    public void start() {
        if (reporter != null) {
            LOG.debug("Starting...");
            reporter.start(reportingIntervalSecs, TimeUnit.SECONDS);
        } else {
            throw new IllegalStateException("Attempt to start without preparing " + getClass().getSimpleName());
        }
    }

    @Override
    public void stop() {
        if (reporter != null) {
            LOG.debug("Stopping...");
            reporter.report();
            reporter.stop();
        } else {
            throw new IllegalStateException("Attempt to stop without preparing " + getClass().getSimpleName());
        }
    }
}
