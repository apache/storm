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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.cassandra.client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Map;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.cassandra.context.BaseBeanFactory;


/**
 * Default interface to build cassandra Cluster from the a Storm Topology configuration.
 */
public class ClusterFactory extends BaseBeanFactory<Cluster> {

    /**
     * Creates a new Cluster based on the specified configuration.
     * @param stormConf the storm configuration.
     * @return a new a new {@link com.datastax.driver.core.Cluster} instance.
     */
    @Override
    protected Cluster make(Map<String, Object> stormConf) {
        CassandraConf cassandraConf = new CassandraConf(stormConf);

        Cluster.Builder cluster = Cluster.builder()
                .withoutJMXReporting()
                .withoutMetrics()
                .addContactPoints(cassandraConf.getNodes())
                .withPort(cassandraConf.getPort())
                .withRetryPolicy(cassandraConf.getRetryPolicy())
                .withReconnectionPolicy(new ExponentialReconnectionPolicy(
                        cassandraConf.getReconnectionPolicyBaseMs(),
                        cassandraConf.getReconnectionPolicyMaxMs()))
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));

        final String username = cassandraConf.getUsername();
        final String password = cassandraConf.getPassword();

        if( StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
            cluster.withAuthProvider(new PlainTextAuthProvider(username, password));
        }

        QueryOptions options = new QueryOptions()
                .setConsistencyLevel(cassandraConf.getConsistencyLevel());
        cluster.withQueryOptions(options);

        SslProps sslProps = cassandraConf.getSslProps();
        configureIfSsl(cluster, sslProps);

        return cluster.build();
    }

    /**
     * If sslProps passed, then set SSL configuration in clusterBuilder.
     *
     * @param clusterBuilder cluster builder
     * @param sslProps SSL properties
     * @return
     */
    private static Builder configureIfSsl(Builder clusterBuilder, SslProps sslProps) {
        if (sslProps == null || !sslProps.isSsl()) {
            return clusterBuilder;
        }

        SSLContext sslContext = getSslContext(sslProps.getTruststorePath(), sslProps.getTruststorePassword(), sslProps.getKeystorePath(),
                sslProps.getKeystorePassword());

        // Default cipher suites supported by C*
        // String[] cipherSuites = { "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA" };

        SSLOptions sslOptions = JdkSSLOptions.builder().withSSLContext(sslContext).build();
        return clusterBuilder.withSSL(sslOptions);
    }

    private static SSLContext getSslContext(String truststorePath, String truststorePassword, String keystorePath,
            String keystorePassword) {
        SSLContext ctx;
        try {
            ctx = SSLContext.getInstance("SSL");

            FileInputStream tsf = new FileInputStream(truststorePath);
            KeyStore ts = KeyStore.getInstance("JKS");
            ts.load(tsf, truststorePassword.toCharArray());
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);

            FileInputStream ksf = new FileInputStream(keystorePath);
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(ksf, keystorePassword.toCharArray());
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, keystorePassword.toCharArray());

            ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
        } catch (UnrecoverableKeyException | KeyManagementException | NoSuchAlgorithmException | KeyStoreException | CertificateException
                | IOException e) {
            throw new RuntimeException(e);
        }
        return ctx;
    }
}
