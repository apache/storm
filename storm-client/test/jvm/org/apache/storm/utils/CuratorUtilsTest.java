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

package org.apache.storm.utils;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.test.InstanceSpec;
import org.apache.storm.Config;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.shade.org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.storm.shade.org.apache.curator.framework.AuthInfo;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.storm.shade.org.apache.zookeeper.ClientCnxnSocketNetty;
import org.apache.storm.shade.org.apache.zookeeper.ZooKeeper;
import org.apache.storm.shade.org.apache.zookeeper.client.ZKClientConfig;
import org.apache.storm.shade.org.apache.zookeeper.common.ClientX509Util;
import org.junit.jupiter.api.Test;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CuratorUtilsTest {
    public static final Logger LOG = LoggerFactory.getLogger(CuratorUtilsTest.class);
    static final int JUTE_MAXBUFFER = 400000000;
    static final int SECURE_CLIENT_PORT = 6065;
    public static final boolean DELETE_DATA_DIRECTORY_ON_CLOSE = true;
    static final File ZK_DATA_DIR = new File("testZkSSLClientConnectionDataDir");
    private static final int SERVER_ID = 1;
    private static final int TICK_TIME = 100;
    private static final int MAX_CLIENT_CNXNS = 10;
    public static final int ELECTION_PORT = -1;
    public static final int QUORUM_PORT = -1;

    private TestingServer server;

    @Test
    public void newCuratorUsesExponentialBackoffTest() {
        final int expectedInterval = 2400;
        final int expectedRetries = 10;
        final int expectedCeiling = 3000;

        Map<String, Object> config = Utils.readDefaultConfig();
        config.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, expectedInterval);
        config.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, expectedRetries);
        config.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING, expectedCeiling);

        CuratorFramework curator = CuratorUtils.newCurator(config,
            Collections.singletonList("bogus_server"), 42, "", DaemonType.WORKER.getDefaultZkAcls(config));
        StormBoundedExponentialBackoffRetry policy =
            (StormBoundedExponentialBackoffRetry) curator.getZookeeperClient().getRetryPolicy();
        assertEquals(policy.getBaseSleepTimeMs(), expectedInterval);
        assertEquals(policy.getN(), expectedRetries);
        assertEquals(policy.getSleepTimeMs(10, 0), expectedCeiling);
    }

    @Test
    public void givenNoExhibitorServersBuilderUsesFixedProviderTest() {
        CuratorFrameworkFactory.Builder builder = setupBuilder(false);
        assertEquals(builder.getEnsembleProvider().getConnectionString(), "zk_connection_string");
        assertEquals(builder.getEnsembleProvider().getClass(), FixedEnsembleProvider.class);
    }

    @Test
    public void givenSchemeAndPayloadBuilderUsesAuthTest() {
        CuratorFrameworkFactory.Builder builder = setupBuilder(true /*with auth*/);
        List<AuthInfo> authInfos = builder.getAuthInfos();
        AuthInfo authInfo = authInfos.get(0);
        assertEquals(authInfo.getScheme(), "scheme");
        assertArrayEquals(authInfo.getAuth(), "abc".getBytes());
    }

    private CuratorFrameworkFactory.Builder setupBuilder(boolean withAuth) {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 0);
        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 0);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 0);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING, 0);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 0);
        String zkStr = "zk_connection_string";
        ZookeeperAuthInfo auth = null;
        if (withAuth) {
            auth = new ZookeeperAuthInfo("scheme", "abc".getBytes());
        }
        CuratorUtils.testSetupBuilder(builder, zkStr, conf, auth);
        return builder;
    }

    /**
     * A method to configure the test ZK server to accept secure client connection.
     * The self-signed certificates were generated for testing purposes as described below.
     * For the ZK client to connect with the ZK server, the ZK server's keystore and truststore
     * should be used.
     * For testing purposes the keystore and truststore were generated using default values.
     * 1. to generate the keystore.jks file:
     * # keytool -genkey -alias mockcert -keyalg RSA -keystore keystore.jks -keysize 2048
     * 2. generate the ca-cert and the ca-key:
     * # openssl req -new -x509 -keyout ca-key -out ca-cert
     * 3. to generate the certificate signing request (cert-file):
     * # keytool -keystore keystore.jks -alias mockcert -certreq -file certificate-request
     * 4. to generate the ca-cert.srl file and make the cert valid for 10 years:
     * # openssl x509 -req -CA ca-cert -CAkey ca-key -in certificate-request -out cert-signed
     * -days 3650 -CAcreateserial -passin pass:password
     * 5. add the ca-cert to the keystore.jks:
     * # keytool -keystore keystore.jks -alias mockca -import -file ca-cert
     * 6. install the signed certificate to the keystore:
     * # keytool -keystore keystore.jks -alias mockcert -import -file cert-signed
     * 7. add the certificate to the truststore:
     * # keytool -keystore truststore.jks -alias mockcert -import -file ca-cert
     * For our purpose, we only need the end result of this process: the keystore.jks and the
     * truststore.jks files.
     *
     * @return conf The method returns the updated Configuration.
     */
    public Map<String, Object> setUpSecureConfig(String testDataPath) throws Exception {
        System.setProperty("zookeeper.ssl.keyStore.location", testDataPath + "testKeyStore.jks");
        System.setProperty("zookeeper.ssl.keyStore.password", "testpass");
        System.setProperty("zookeeper.ssl.trustStore.location", testDataPath + "testTrustStore.jks");
        System.setProperty("zookeeper.ssl.trustStore.password", "testpass");
        System.setProperty("zookeeper.request.timeout", "12345");
        System.setProperty("zookeeper.serverCnxnFactory", "org.apache.zookeeper.server.NettyServerCnxnFactory");
        System.setProperty("jute.maxbuffer", String.valueOf(JUTE_MAXBUFFER));

        System.setProperty("javax.net.debug", "ssl");
        System.setProperty("zookeeper.authProvider.x509",
                "org.apache.zookeeper.server.auth" + ".X509AuthenticationProvider");

        // inject values to the ZK configuration file for secure connection
        Map<String, Object> customConfiguration = new HashMap<>();
        customConfiguration.put("secureClientPort", String.valueOf(SECURE_CLIENT_PORT));
        customConfiguration.put("audit.enable", "true");
        InstanceSpec spec =
                new InstanceSpec(ZK_DATA_DIR, SECURE_CLIENT_PORT, ELECTION_PORT, QUORUM_PORT,
                        DELETE_DATA_DIRECTORY_ON_CLOSE, SERVER_ID, TICK_TIME, MAX_CLIENT_CNXNS,
                        customConfiguration);

        this.server = new TestingServer(spec, false);
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.ZK_SSL_KEYSTORE_LOCATION,
                testDataPath + "testKeyStore.jks");
        conf.put(Config.ZK_SSL_KEYSTORE_PASSWORD, "testpass");
        conf.put(Config.ZK_SSL_TRUSTSTORE_LOCATION,
                testDataPath + "testTrustStore.jks");
        conf.put(Config.ZK_SSL_TRUSTSTORE_PASSWORD, "testpass");
        conf.put(Config.ZK_SSL_HOSTNAME_VERIFICATION, false);
        return conf;
    }

    @Test
    public void testSecureZKConfiguration() throws Exception {
        LOG.info("Entered to the testSecureZKConfiguration test case.");
        Map<String, Object> conf = setUpSecureConfig("test/resources/ssl/");
        conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 0);
        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 0);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 0);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING, 0);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 0);
        conf.put(Config.ZK_SSL_ENABLE, true);
        String zkStr = this.server.getConnectString();
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorUtils.testSetupBuilder(builder, zkStr, conf, null);
        CuratorFramework curatorFramework = builder.build();
        curatorFramework.start();
        ZooKeeper zk = curatorFramework.getZookeeperClient().getZooKeeper();
        validateSSLConfiguration(ObjectReader.getString(conf.get(Config.ZK_SSL_KEYSTORE_LOCATION)),
                ObjectReader.getString(conf.get(Config.ZK_SSL_KEYSTORE_PASSWORD)),
                ObjectReader.getString(conf.get(Config.ZK_SSL_TRUSTSTORE_LOCATION)),
                ObjectReader.getString(conf.get(Config.ZK_SSL_TRUSTSTORE_PASSWORD)),
                ObjectReader.getBoolean(conf.get(Config.ZK_SSL_HOSTNAME_VERIFICATION), true),
                zk);
        this.server.close();
    }

    private void validateSSLConfiguration(String keystoreLocation, String keystorePassword,
                                          String truststoreLocation, String truststorePassword,
                                          Boolean hostNameVerification, ZooKeeper zk) {
        try (ClientX509Util x509Util = new ClientX509Util()) {
            //testing if custom values are set properly
            assertEquals(keystoreLocation,
                    zk.getClientConfig().getProperty(x509Util.getSslKeystoreLocationProperty())
                    , "Validate that expected clientConfig is set in ZK config");
            assertEquals(keystorePassword,
                    zk.getClientConfig().getProperty(x509Util.getSslKeystorePasswdProperty())
            , "Validate that expected clientConfig is set in ZK config");
            assertEquals(truststoreLocation,
                    zk.getClientConfig().getProperty(x509Util.getSslTruststoreLocationProperty())
            , "Validate that expected clientConfig is set in ZK config");
            assertEquals(truststorePassword,
                    zk.getClientConfig().getProperty(x509Util.getSslTruststorePasswdProperty())
            , "Validate that expected clientConfig is set in ZK config");
            assertEquals(hostNameVerification.toString(),
                    zk.getClientConfig().getProperty(x509Util.getSslHostnameVerificationEnabledProperty())
                    , "Validate that expected clientConfig is set in ZK config");
        }
        //testing if constant values hardcoded into the code are set properly
        assertEquals(Boolean.TRUE.toString(),
                zk.getClientConfig().getProperty(ZKClientConfig.SECURE_CLIENT)
        , "Validate that expected clientConfig is set in ZK config");
        assertEquals(ClientCnxnSocketNetty.class.getCanonicalName(),
                zk.getClientConfig().getProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET)
        , "Validate that expected clientConfig is set in ZK config");
    }

    @Test
    public void testTruststoreKeystoreConfiguration() {
        LOG.info("Entered to the testTruststoreKeystoreConfiguration test case.");
    /*
      By default the truststore/keystore configurations are not set, hence the values are null.
      Validate that the null values are converted into empty strings by the class.
     */
        Map<String, Object> conf = new HashMap<>();
        CuratorUtils.TruststoreKeystore truststoreKeystore =
                new CuratorUtils.TruststoreKeystore(conf);

        assertEquals("",
                truststoreKeystore.getKeystoreLocation(), "Validate that null value is converted to empty string.");
        assertEquals("",
                truststoreKeystore.getKeystorePassword(), "Validate that null value is converted to empty string.");
        assertEquals("",
                truststoreKeystore.getTruststoreLocation(), "Validate that null value is converted to empty string.");
        assertEquals("",
                truststoreKeystore.getTruststorePassword(), "Validate that null value is converted to empty string.");
        assertEquals(true,
                truststoreKeystore.getHostnameVerification(), "Validate that null value is converted to false.");


        //Validate that non-null values will remain intact
        conf.put(Config.ZK_SSL_KEYSTORE_LOCATION, "/keystore.jks");
        conf.put(Config.ZK_SSL_KEYSTORE_PASSWORD, "keystorePassword");
        conf.put(Config.ZK_SSL_TRUSTSTORE_LOCATION, "/truststore.jks");
        conf.put(Config.ZK_SSL_TRUSTSTORE_PASSWORD, "truststorePassword");
        conf.put(Config.ZK_SSL_HOSTNAME_VERIFICATION, false);

        CuratorUtils.TruststoreKeystore truststoreKeystore1 =
                new CuratorUtils.TruststoreKeystore(conf);
        assertEquals("/keystore.jks",
                truststoreKeystore1.getKeystoreLocation(), "Validate that non-null value kept intact.");
        assertEquals("keystorePassword",
                truststoreKeystore1.getKeystorePassword(), "Validate that non-null value kept intact.");
        assertEquals("/truststore.jks",
                truststoreKeystore1.getTruststoreLocation(), "Validate that non-null value kept intact.");
        assertEquals("truststorePassword",
                truststoreKeystore1.getTruststorePassword(), "Validate that non-null value kept intact.");
        assertEquals(false,
                truststoreKeystore1.getHostnameVerification(), "Validate that non-null value kept intact.");
    }
}
