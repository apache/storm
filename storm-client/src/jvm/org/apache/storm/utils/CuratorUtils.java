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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.naming.ConfigurationException;
import org.apache.storm.Config;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.storm.shade.org.apache.curator.framework.api.ACLProvider;
import org.apache.storm.shade.org.apache.zookeeper.client.ZKClientConfig;
import org.apache.storm.shade.org.apache.zookeeper.common.ClientX509Util;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorUtils {
    public static final Logger LOG = LoggerFactory.getLogger(CuratorUtils.class);
    public static final String CLIENT_CNXN
            = org.apache.storm.shade.org.apache.zookeeper.ClientCnxnSocketNetty.class.getName();

    public static CuratorFramework newCurator(Map<String, Object> conf, List<String> servers, Object port, String root,
                                              List<ACL> defaultAcl) {
        return newCurator(conf, servers, port, root, null, defaultAcl);
    }

    public static CuratorFramework newCurator(Map<String, Object> conf, List<String> servers, Object port, ZookeeperAuthInfo auth,
                                              List<ACL> defaultAcl) {
        return newCurator(conf, servers, port, "", auth, defaultAcl);
    }

    public static CuratorFramework newCurator(Map<String, Object> conf, List<String> servers, Object port, String root,
                                              ZookeeperAuthInfo auth, final List<ACL> defaultAcl) {
        List<String> serverPorts = new ArrayList<>();
        for (String zkServer : servers) {
            serverPorts.add(zkServer + ":" + ObjectReader.getInt(port));
        }
        String zkStr = StringUtils.join(serverPorts, ",") + root;
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();

        setupBuilder(builder, zkStr, conf, auth);
        if (defaultAcl != null) {
            builder.aclProvider(new ACLProvider() {
                @Override
                public List<ACL> getDefaultAcl() {
                    return defaultAcl;
                }

                @Override
                public List<ACL> getAclForPath(String s) {
                    return null;
                }
            });
        }

        return builder.build();
    }

    protected static void setupBuilder(CuratorFrameworkFactory.Builder builder, final String zkStr, Map<String, Object> conf,
                                       ZookeeperAuthInfo auth) {
        builder.connectString(zkStr);
        builder
                .connectionTimeoutMs(ObjectReader.getInt(conf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)))
                .sessionTimeoutMs(ObjectReader.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)))
                .retryPolicy(new StormBoundedExponentialBackoffRetry(
                        ObjectReader.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL)),
                        ObjectReader.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING)),
                        ObjectReader.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES))));

        if (auth != null && auth.scheme != null && auth.payload != null) {
            builder.authorization(auth.scheme, auth.payload);
        }
        boolean sslEnabled = ObjectReader.getBoolean(conf.get(Config.ZK_SSL_ENABLE), false);
        if (sslEnabled) {
            SslConf sslConf = new SslConf(conf);
            ZKClientConfig zkClientConfig = new ZKClientConfig();
            try {
                setSslConfiguration(zkClientConfig, new ClientX509Util(), sslConf);
            } catch (ConfigurationException e) {
                throw new RuntimeException(e);
            }
            builder.zkClientConfig(zkClientConfig);
        }
    }

    /**
     * Configure ZooKeeper Client with SSL/TLS connection.
     * @param zkClientConfig ZooKeeper Client configuration
     * @param x509Util The X509 utility
     * @param sslConf The truststore and keystore configs
     */
    private static void setSslConfiguration(ZKClientConfig zkClientConfig,
                                            ClientX509Util x509Util, SslConf sslConf)
            throws ConfigurationException {
        validateSslConfiguration(sslConf);
        LOG.info("Configuring the ZooKeeper client to use SSL/TLS encryption for connecting to the "
                + "ZooKeeper server.");
        LOG.debug("Configuring the ZooKeeper client with {} location: {}.",
                sslConf.keystoreLocation,
                Config.STORM_ZOOKEEPER_SSL_KEYSTORE_PATH);
        LOG.debug("Configuring the ZooKeeper client with {} location: {}.",
                sslConf.truststoreLocation,
                Config.STORM_ZOOKEEPER_SSL_TRUSTSTORE_PATH);

        zkClientConfig.setProperty(ZKClientConfig.SECURE_CLIENT, "true");
        zkClientConfig.setProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET,
                CLIENT_CNXN);
        zkClientConfig.setProperty(x509Util.getSslKeystoreLocationProperty(),
                sslConf.keystoreLocation);
        zkClientConfig.setProperty(x509Util.getSslKeystorePasswdProperty(),
                sslConf.keystorePassword);
        zkClientConfig.setProperty(x509Util.getSslTruststoreLocationProperty(),
                sslConf.truststoreLocation);
        zkClientConfig.setProperty(x509Util.getSslTruststorePasswdProperty(),
                sslConf.truststorePassword);
        zkClientConfig.setProperty(x509Util.getSslHostnameVerificationEnabledProperty(),
                sslConf.hostnameVerification.toString());
    }

    private static void validateSslConfiguration(SslConf sslConf) throws ConfigurationException {
        if (StringUtils.isEmpty(sslConf.getKeystoreLocation())) {
            throw new ConfigurationException(
                    "The keystore location parameter is empty for the ZooKeeper client connection.");
        }
        if (StringUtils.isEmpty(sslConf.getKeystorePassword())) {
            throw new ConfigurationException(
                    "The keystore password parameter is empty for the ZooKeeper client connection.");
        }
        if (StringUtils.isEmpty(sslConf.getTruststoreLocation())) {
            throw new ConfigurationException(
                    "The truststore location parameter is empty for the ZooKeeper client connection" + ".");
        }
        if (StringUtils.isEmpty(sslConf.getTruststorePassword())) {
            throw new ConfigurationException(
                    "The truststore password parameter is empty for the ZooKeeper client connection" + ".");
        }
    }

    public static SslConf getSslConf(Map<String, Object> conf) {
        return new SslConf(conf);
    }
    /**
     * Helper class to contain the Truststore/Keystore paths for the ZK client connection over
     * SSL/TLS.
     */

    static final class SslConf {
        private final String keystoreLocation;
        private final String keystorePassword;
        private final String truststoreLocation;
        private final String truststorePassword;
        private final Boolean hostnameVerification;

        /**
         * Configuration for the ZooKeeper connection when SSL/TLS is enabled.
         *
         * @param conf configuration map
         */
        private SslConf(Map<String, Object> conf) {
            keystoreLocation = ObjectReader.getString(conf.get(Config.STORM_ZOOKEEPER_SSL_KEYSTORE_PATH), "");
            keystorePassword = ObjectReader.getString(conf.get(Config.STORM_ZOOKEEPER_SSL_KEYSTORE_PASSWORD), "");
            truststoreLocation = ObjectReader.getString(conf.get(Config.STORM_ZOOKEEPER_SSL_TRUSTSTORE_PATH), "");
            truststorePassword = ObjectReader.getString(conf.get(Config.STORM_ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD), "");
            hostnameVerification = ObjectReader.getBoolean(conf.get(Config.STORM_ZOOKEEPER_SSL_HOSTNAME_VERIFICATION), true);
        }

        public String getKeystoreLocation() {
            return keystoreLocation;
        }

        public String getKeystorePassword() {
            return keystorePassword;
        }

        public String getTruststoreLocation() {
            return truststoreLocation;
        }

        public String getTruststorePassword() {
            return truststorePassword;
        }

        public Boolean getHostnameVerification() {
            return hostnameVerification;
        }
    }

    public static void testSetupBuilder(CuratorFrameworkFactory.Builder
                                            builder, String zkStr, Map<String, Object> conf, ZookeeperAuthInfo auth) {
        setupBuilder(builder, zkStr, conf, auth);
    }

    public static CuratorFramework newCuratorStarted(Map<String, Object> conf, List<String> servers, Object port,
                                                     String root, ZookeeperAuthInfo auth, List<ACL> defaultAcl) {
        CuratorFramework ret = newCurator(conf, servers, port, root, auth, defaultAcl);
        LOG.info("Starting Utils Curator...");
        ret.start();
        return ret;
    }

    public static CuratorFramework newCuratorStarted(Map<String, Object> conf, List<String> servers, Object port,
                                                     ZookeeperAuthInfo auth, List<ACL> defaultAcl) {
        CuratorFramework ret = newCurator(conf, servers, port, auth, defaultAcl);
        LOG.info("Starting Utils Curator (2)...");
        ret.start();
        return ret;
    }
}
