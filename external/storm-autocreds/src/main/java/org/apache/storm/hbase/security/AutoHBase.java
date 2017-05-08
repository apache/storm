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

import org.apache.storm.Config;
import org.apache.storm.common.AbstractAutoCreds;
import org.apache.storm.hdfs.security.HdfsSecurityUtil;
import org.apache.storm.security.INimbusCredentialPlugin;
import org.apache.storm.security.auth.IAutoCredentials;
import org.apache.storm.security.auth.ICredentialsRenewer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.TokenUtil;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Automatically get hbase delegation tokens and push it to user's topology. The class
 * assumes that hadoop/hbase configuration files are in your class path.
 */
public class AutoHBase extends AbstractAutoCreds {
    private static final Logger LOG = LoggerFactory.getLogger(AutoHBase.class);

    public static final String HBASE_CREDENTIALS = "HBASE_CREDENTIALS";
    public static final String HBASE_KEYTAB_FILE_KEY = "hbase.keytab.file";
    public static final String HBASE_PRINCIPAL_KEY = "hbase.kerberos.principal";

    public String hbaseKeytab;
    public String hbasePrincipal;

    @Override
    public void doPrepare(Map conf) {
        if(conf.containsKey(HBASE_KEYTAB_FILE_KEY) && conf.containsKey(HBASE_PRINCIPAL_KEY)) {
            hbaseKeytab = (String) conf.get(HBASE_KEYTAB_FILE_KEY);
            hbasePrincipal = (String) conf.get(HBASE_PRINCIPAL_KEY);
        }
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

                if (hbaseConf.get(HBASE_KEYTAB_FILE_KEY) == null) {
                    hbaseConf.set(HBASE_KEYTAB_FILE_KEY, hbaseKeytab);
                }
                if (hbaseConf.get(HBASE_PRINCIPAL_KEY) == null) {
                    hbaseConf.set(HBASE_PRINCIPAL_KEY, hbasePrincipal);
                }
                provider.login(HBASE_KEYTAB_FILE_KEY, HBASE_PRINCIPAL_KEY, InetAddress.getLocalHost().getCanonicalHostName());

                LOG.info("Logged into Hbase as principal = " + conf.get(HBASE_PRINCIPAL_KEY));

                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

                final UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(topologySubmitterUser, ugi);

                User user = User.create(ugi);

                if(user.isHBaseSecurityEnabled(hbaseConf)) {
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
    protected String getCredentialKey(String configKey) {
        return HBASE_CREDENTIALS + configKey;
    }


    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map conf = new HashMap();
        conf.put(Config.TOPOLOGY_SUBMITTER_PRINCIPAL, args[0]); //with realm e.g. storm@WITZEND.COM
        conf.put(HBASE_PRINCIPAL_KEY,args[1]); // hbase principal storm-hbase@WITZEN.COM
        conf.put(HBASE_KEYTAB_FILE_KEY,args[2]); // storm hbase keytab /etc/security/keytabs/storm-hbase.keytab

        AutoHBase autoHBase = new AutoHBase();
        autoHBase.prepare(conf);

        Map<String,String> creds  = new HashMap<String, String>();
        autoHBase.populateCredentials(creds, conf);
        LOG.info("Got HBase credentials" + autoHBase.getCredentials(creds));

        Subject s = new Subject();
        autoHBase.populateSubject(s, creds);
        LOG.info("Got a Subject " + s);

        autoHBase.renew(creds, conf);
        LOG.info("renewed credentials" + autoHBase.getCredentials(creds));
    }
}

