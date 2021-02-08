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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.hdfs.security;

import static org.apache.storm.hdfs.security.HdfsSecurityUtil.HDFS_CREDENTIALS;
import static org.apache.storm.hdfs.security.HdfsSecurityUtil.STORM_KEYTAB_FILE_KEY;
import static org.apache.storm.hdfs.security.HdfsSecurityUtil.STORM_USER_NAME_KEY;
import static org.apache.storm.hdfs.security.HdfsSecurityUtil.TOPOLOGY_HDFS_URI;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.storm.Config;
import org.apache.storm.common.AbstractHadoopNimbusPluginAutoCreds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Auto credentials nimbus plugin for HDFS implementation. This class automatically
 * gets HDFS delegation tokens and push it to user's topology.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class AutoHDFSNimbus extends AbstractHadoopNimbusPluginAutoCreds {
    private static final Logger LOG = LoggerFactory.getLogger(AutoHDFSNimbus.class);

    private String hdfsKeyTab;
    private String hdfsPrincipal;

    @Override
    public void doPrepare(Map<String, Object> conf) {
        if (conf.containsKey(STORM_KEYTAB_FILE_KEY) && conf.containsKey(STORM_USER_NAME_KEY)) {
            hdfsKeyTab = (String) conf.get(STORM_KEYTAB_FILE_KEY);
            hdfsPrincipal = (String) conf.get(STORM_USER_NAME_KEY);
        }
    }

    @Override
    protected String getConfigKeyString() {
        return HdfsSecurityUtil.HDFS_CREDENTIALS_CONFIG_KEYS;
    }

    @Override
    public void shutdown() {
        //no op.
    }

    @Override
    protected  byte[] getHadoopCredentials(Map<String, Object> conf, String configKey, final String topologyOwnerPrincipal) {
        Configuration configuration = getHadoopConfiguration(conf, configKey);
        return getHadoopCredentials(conf, configuration, topologyOwnerPrincipal);
    }

    @Override
    protected byte[] getHadoopCredentials(Map<String, Object> conf, final String topologyOwnerPrincipal) {
        return getHadoopCredentials(conf, new Configuration(), topologyOwnerPrincipal);
    }

    @SuppressWarnings("unchecked")
    private byte[] getHadoopCredentials(Map<String, Object> conf, final Configuration configuration, final String topologySubmitterUser) {
        try {
            if (UserGroupInformation.isSecurityEnabled()) {
                login(configuration);

                final URI nameNodeUri = conf.containsKey(TOPOLOGY_HDFS_URI)
                        ? new URI(conf.get(TOPOLOGY_HDFS_URI).toString())
                        : FileSystem.getDefaultUri(configuration);

                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

                final UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(topologySubmitterUser, ugi);

                Credentials creds = (Credentials) proxyUser.doAs(new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            FileSystem fileSystem = FileSystem.get(nameNodeUri, configuration);
                            Credentials credential = proxyUser.getCredentials();

                            if (configuration.get(STORM_USER_NAME_KEY) == null) {
                                configuration.set(STORM_USER_NAME_KEY, hdfsPrincipal);
                            }

                            fileSystem.addDelegationTokens(configuration.get(STORM_USER_NAME_KEY), credential);
                            LOG.info("Delegation tokens acquired for user {}", topologySubmitterUser);
                            return credential;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });


                ByteArrayOutputStream bao = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bao);

                creds.write(out);
                out.flush();
                out.close();

                return bao.toByteArray();
            } else {
                throw new RuntimeException("Security is not enabled for HDFS");
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to get delegation tokens." , ex);
        }
    }

    private Configuration getHadoopConfiguration(Map<String, Object> topoConf, String configKey) {
        Configuration configuration = new Configuration();
        fillHadoopConfiguration(topoConf, configKey, configuration);
        return configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void doRenew(Map<String, String> credentials, Map<String, Object> topologyConf, final String topologyOwnerPrincipal) {
        List<String> confKeys = getConfigKeys(topologyConf);
        for (Pair<String, Credentials> cred : getCredentials(credentials, confKeys)) {
            try {
                Configuration configuration = getHadoopConfiguration(topologyConf, cred.getFirst());
                Collection<Token<? extends TokenIdentifier>> tokens = cred.getSecond().getAllTokens();

                if (tokens != null && !tokens.isEmpty()) {
                    for (Token<? extends TokenIdentifier> token : tokens) {
                        //We need to re-login some other thread might have logged into hadoop using
                        // their credentials (e.g. AutoHBase might be also part of nimbu auto creds)
                        login(configuration);
                        long expiration = token.renew(configuration);
                        LOG.info("HDFS delegation token renewed, new expiration time {}", expiration);
                    }
                } else {
                    LOG.debug("No tokens found for credentials, skipping renewal.");
                }
            } catch (Exception e) {
                LOG.warn("could not renew the credentials, one of the possible reason is tokens are beyond "
                                + "renewal period so attempting to get new tokens.",
                        e);
                populateCredentials(credentials, topologyConf, topologyOwnerPrincipal);
            }
        }
    }

    private void login(Configuration configuration) throws IOException {
        if (configuration.get(STORM_KEYTAB_FILE_KEY) == null) {
            configuration.set(STORM_KEYTAB_FILE_KEY, hdfsKeyTab);
        }
        if (configuration.get(STORM_USER_NAME_KEY) == null) {
            configuration.set(STORM_USER_NAME_KEY, hdfsPrincipal);
        }
        SecurityUtil.login(configuration, STORM_KEYTAB_FILE_KEY, STORM_USER_NAME_KEY);

        LOG.info("Logged into hdfs with principal {}", configuration.get(STORM_USER_NAME_KEY));
    }

    @Override
    public String getCredentialKey(String configKey) {
        return HDFS_CREDENTIALS + configKey;
    }
}

