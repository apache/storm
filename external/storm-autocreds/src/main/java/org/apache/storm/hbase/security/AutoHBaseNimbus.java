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

package org.apache.storm.hbase.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.TokenUtil;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.storm.Config;
import org.apache.storm.common.AbstractHadoopNimbusPluginAutoCreds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.util.Map;

import static org.apache.storm.hbase.security.HBaseSecurityUtil.HBASE_CREDENTIALS;
import static org.apache.storm.hbase.security.HBaseSecurityUtil.HBASE_KEYTAB_FILE_KEY;
import static org.apache.storm.hbase.security.HBaseSecurityUtil.HBASE_PRINCIPAL_KEY;

/**
 * Auto credentials nimbus plugin for HBase implementation. This class automatically
 * gets HBase delegation tokens and push it to user's topology.
 */
public class AutoHBaseNimbus extends AbstractHadoopNimbusPluginAutoCreds {
    private static final Logger LOG = LoggerFactory.getLogger(AutoHBaseNimbus.class);

    @Override
    public void doPrepare(Map conf) {
        // we don't allow any cluster wide configuration
    }

    @Override
    protected String getConfigKeyString() {
        return HBaseSecurityUtil.HBASE_CREDENTIALS_CONFIG_KEYS;
    }

    @Override
    public void shutdown() {
        //no op.
    }

    @Override
    protected  byte[] getHadoopCredentials(Map conf, String configKey) {
        Configuration configuration = getHadoopConfiguration(conf, configKey);
        return getHadoopCredentials(conf, configuration);
    }

    @Override
    protected byte[] getHadoopCredentials(Map conf) {
        return getHadoopCredentials(conf, HBaseConfiguration.create());
    }

    private Configuration getHadoopConfiguration(Map topoConf, String configKey) {
        Configuration configuration = HBaseConfiguration.create();
        fillHadoopConfiguration(topoConf, configKey, configuration);
        return configuration;
    }

    @SuppressWarnings("unchecked")
    protected byte[] getHadoopCredentials(Map conf, Configuration hbaseConf) {
        try {
            if(UserGroupInformation.isSecurityEnabled()) {
                final String topologySubmitterUser = (String) conf.get(Config.TOPOLOGY_SUBMITTER_PRINCIPAL);

                UserProvider provider = UserProvider.instantiate(hbaseConf);
                provider.login(HBASE_KEYTAB_FILE_KEY, HBASE_PRINCIPAL_KEY, InetAddress.getLocalHost().getCanonicalHostName());

                LOG.info("Logged into Hbase as principal = " + conf.get(HBASE_PRINCIPAL_KEY));

                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

                final UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(topologySubmitterUser, ugi);

                if(User.isHBaseSecurityEnabled(hbaseConf)) {
                    TokenUtil.obtainAndCacheToken(hbaseConf, proxyUser);

                    LOG.info("Obtained HBase tokens, adding to user credentials.");

                    Credentials credential= proxyUser.getCredentials();
                    ByteArrayOutputStream bao = new ByteArrayOutputStream();
                    ObjectOutputStream out = new ObjectOutputStream(bao);
                    credential.write(out);
                    out.flush();
                    out.close();
                    return bao.toByteArray();
                } else {
                    throw new RuntimeException("Security is not enabled for HBase.");
                }
            } else {
                throw new RuntimeException("Security is not enabled for Hadoop");
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to get delegation tokens." , ex);
        }
    }

    @Override
    public void doRenew(Map<String, String> credentials, Map topologyConf) {
        //HBASE tokens are not renewable so we always have to get new ones.
        populateCredentials(credentials, topologyConf);
    }

    @Override
    public String getCredentialKey(String configKey) {
        return HBASE_CREDENTIALS + configKey;
    }

}

