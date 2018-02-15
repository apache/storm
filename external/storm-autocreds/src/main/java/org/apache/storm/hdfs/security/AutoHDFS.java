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

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.storm.Config;
import org.apache.storm.common.AbstractAutoCreds;
import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.storm.hdfs.security.HdfsSecurityUtil.STORM_KEYTAB_FILE_KEY;
import static org.apache.storm.hdfs.security.HdfsSecurityUtil.STORM_USER_NAME_KEY;

/**
 * Automatically get HDFS delegation tokens and push it to user's topology. The class
 * assumes that HDFS configuration files are in your class path.
 */
public class AutoHDFS extends AbstractAutoCreds {
    private static final Logger LOG = LoggerFactory.getLogger(AutoHDFS.class);
    public static final String HDFS_CREDENTIALS = "HDFS_CREDENTIALS";
    public static final String TOPOLOGY_HDFS_URI = "topology.hdfs.uri";

    private String hdfsKeyTab;
    private String hdfsPrincipal;

    @Override
    public void doPrepare(Map conf) {
        if(conf.containsKey(STORM_KEYTAB_FILE_KEY) && conf.containsKey(STORM_USER_NAME_KEY)) {
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

    private Configuration getHadoopConfiguration(Map topoConf, String configKey) {
        Configuration configuration = new Configuration();
        fillHadoopConfiguration(topoConf, configKey, configuration);
        return configuration;
    }

    @SuppressWarnings("unchecked")
    private byte[] getHadoopCredentials(Map<String, Object> conf, final Configuration configuration, final String topologyOwnerPrincipal) {
        try {
            if(UserGroupInformation.isSecurityEnabled()) {
                login(configuration);

                final URI nameNodeURI = conf.containsKey(TOPOLOGY_HDFS_URI) ? new URI(conf.get(TOPOLOGY_HDFS_URI).toString())
                        : FileSystem.getDefaultUri(configuration);

                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

                final UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(topologyOwnerPrincipal, ugi);

                Credentials creds = (Credentials) proxyUser.doAs(new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            FileSystem fileSystem = FileSystem.get(nameNodeURI, configuration);
                            Credentials credential= proxyUser.getCredentials();

                            if (configuration.get(STORM_USER_NAME_KEY) == null) {
                                configuration.set(STORM_USER_NAME_KEY, hdfsPrincipal);
                            }

                            fileSystem.addDelegationTokens(configuration.get(STORM_USER_NAME_KEY), credential);
                            LOG.info("Delegation tokens acquired for user {}", topologyOwnerPrincipal);
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

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public void doRenew(Map<String, String> credentials, Map<String, Object> topologyConf, String ownerPrincipal) {
        for (Pair<String, Credentials> cred : getCredentials(credentials)) {
            try {
                Configuration configuration = getHadoopConfiguration(topologyConf, cred.getFirst());
                Collection<Token<? extends TokenIdentifier>> tokens = cred.getSecond().getAllTokens();

                if (tokens != null && !tokens.isEmpty()) {
                    for (Token token : tokens) {
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
                LOG.warn("could not renew the credentials, one of the possible reason is tokens are beyond " +
                        "renewal period so attempting to get new tokens.", e);
                populateCredentials(credentials, topologyConf, ownerPrincipal);
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
    protected String getCredentialKey(String configKey) {
        return HDFS_CREDENTIALS + configKey;
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map<String, Object> conf = new HashMap();
        final String topologyOwnerPrincipal = args[0]; //with realm e.g. storm@WITZEND.COM
        conf.put(STORM_USER_NAME_KEY, args[1]); //with realm e.g. hdfs@WITZEND.COM
        conf.put(STORM_KEYTAB_FILE_KEY, args[2]);// /etc/security/keytabs/storm.keytab

        Configuration configuration = new Configuration();
        AutoHDFS autoHDFS = new AutoHDFS();
        autoHDFS.prepare(conf);

        Map<String,String> creds  = new HashMap<>();
        autoHDFS.populateCredentials(creds, conf, topologyOwnerPrincipal);
        LOG.info("Got HDFS credentials", autoHDFS.getCredentials(creds));

        Subject s = new Subject();
        autoHDFS.populateSubject(s, creds);
        LOG.info("Got a Subject "+ s);

        autoHDFS.renew(creds, conf, topologyOwnerPrincipal);
        LOG.info("renewed credentials", autoHDFS.getCredentials(creds));
    }

    @Override
    public void populateCredentials(Map<String, String> credentials, Map topoConf) {
        throw new IllegalStateException("SHOULD NOT BE CALLED");
    }

    @Override
    public void renew(Map<String, String> credentials, Map topologyConf) {
        throw new IllegalStateException("SHOULD NOT BE CALLED");
    }
}

