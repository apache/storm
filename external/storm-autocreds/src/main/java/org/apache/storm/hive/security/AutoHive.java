/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.hive.security;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.storm.Config;
import org.apache.storm.common.AbstractAutoCreds;
import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Automatically get hive delegation tokens and push it to user's topology.
 */
public class AutoHive extends AbstractAutoCreds {
    private static final Logger LOG = LoggerFactory.getLogger(AutoHive.class);

    public static final String HIVE_CREDENTIALS = "HIVE_CREDENTIALS";
    public static final String HIVE_CREDENTIALS_CONFIG_KEYS = "hiveCredentialsConfigKeys";

    public static final String HIVE_KEYTAB_FILE_KEY = "hive.keytab.file";
    public static final String HIVE_PRINCIPAL_KEY = "hive.kerberos.principal";

    public String hiveKeytab;
    public String hivePrincipal;
    public String metaStoreURI;

    @Override
    public void doPrepare(Map conf) {
        if (conf.containsKey(HIVE_KEYTAB_FILE_KEY) && conf.containsKey(HIVE_PRINCIPAL_KEY)) {
            hiveKeytab = (String) conf.get(HIVE_KEYTAB_FILE_KEY);
            hivePrincipal = (String) conf.get(HIVE_PRINCIPAL_KEY);
            metaStoreURI = (String) conf.get(HiveConf.ConfVars.METASTOREURIS.varname);
        }
    }

    @Override
    protected String getConfigKeyString() {
        return HIVE_CREDENTIALS_CONFIG_KEYS;
    }

    @Override
    public void shutdown() {
        //no op.
    }

    @Override
    protected byte[] getHadoopCredentials(Map<String, Object> conf, String configKey, final String topologyOwnerPrincipal) {
        Configuration configuration = getHadoopConfiguration(conf, configKey);
        return getHadoopCredentials(conf, configuration, topologyOwnerPrincipal);
    }

    @Override
    protected byte[] getHadoopCredentials(Map<String, Object> conf, final String topologyOwnerPrincipal) {
        Configuration configuration = new Configuration();
        return getHadoopCredentials(conf, configuration, topologyOwnerPrincipal);
    }

    private Configuration getHadoopConfiguration(Map topoConf, String configKey) {
        Configuration configuration = new Configuration();
        fillHadoopConfiguration(topoConf, configKey, configuration);
        return configuration;
    }

    public HiveConf createHiveConf(String metaStoreURI, String hiveMetaStorePrincipal) throws IOException {
        HiveConf hcatConf = new HiveConf();
        hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreURI);
        hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hcatConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
        hcatConf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, true);
        hcatConf.set(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname, hiveMetaStorePrincipal);
        return hcatConf;
    }

    @SuppressWarnings("unchecked")
    protected byte[] getHadoopCredentials(Map<String, Object> conf, final Configuration configuration, final String topologyOwnerPrincipal) {
        try {
            if (UserGroupInformation.isSecurityEnabled()) {
                String hiveMetaStoreURI = getMetaStoreURI(configuration);
                String hiveMetaStorePrincipal = getMetaStorePrincipal(configuration);
                HiveConf hcatConf = createHiveConf(hiveMetaStoreURI, hiveMetaStorePrincipal);
                login(configuration);

                UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
                UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(topologyOwnerPrincipal, currentUser);
                try {
                    Token<DelegationTokenIdentifier> delegationTokenId =
                            getDelegationToken(hcatConf, hiveMetaStorePrincipal, topologyOwnerPrincipal);
                    proxyUser.addToken(delegationTokenId);
                    LOG.info("Obtained Hive tokens, adding to user credentials.");

                    Credentials credential = proxyUser.getCredentials();
                    ByteArrayOutputStream bao = new ByteArrayOutputStream();
                    ObjectOutputStream out = new ObjectOutputStream(bao);
                    credential.write(out);
                    out.flush();
                    out.close();
                    return bao.toByteArray();
                } catch (Exception ex) {
                    LOG.debug(" Exception" + ex.getMessage());
                    throw ex;
                }
            } else {
                throw new RuntimeException("Security is not enabled for Hadoop");
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to get delegation tokens.", ex);
        }
    }

    private Token<DelegationTokenIdentifier> getDelegationToken(HiveConf hcatConf,
                                                                String metaStoreServicePrincipal,
                                                                String topologySubmitterUser) throws IOException {
        LOG.info("Creating delegation tokens for principal={}", metaStoreServicePrincipal);

        HCatClient hcatClient = null;
        try {
            hcatClient = HCatClient.create(hcatConf);
            String delegationToken = hcatClient.getDelegationToken(topologySubmitterUser, metaStoreServicePrincipal);
            Token<DelegationTokenIdentifier> delegationTokenId = new Token<DelegationTokenIdentifier>();
            delegationTokenId.decodeFromUrlString(delegationToken);

            DelegationTokenIdentifier d = new DelegationTokenIdentifier();
            d.readFields(new DataInputStream(new ByteArrayInputStream(
                    delegationTokenId.getIdentifier())));
            LOG.info("Created Delegation Token for : " + d.getUser());

            return delegationTokenId;
        } finally {
            if (hcatClient != null)
                hcatClient.close();
        }
    }

    private String getMetaStoreURI(Configuration configuration) {
        if (configuration.get(HiveConf.ConfVars.METASTOREURIS.varname) == null)
            return metaStoreURI;
        else
            return configuration.get(HiveConf.ConfVars.METASTOREURIS.varname);
    }

    private String getMetaStorePrincipal(Configuration configuration) {
        if (configuration.get(HIVE_PRINCIPAL_KEY) == null)
            return hivePrincipal;
        else
            return configuration.get(HIVE_PRINCIPAL_KEY);
    }

    private void login(Configuration configuration) throws IOException {
        if (configuration.get(HIVE_KEYTAB_FILE_KEY) == null) {
            configuration.set(HIVE_KEYTAB_FILE_KEY, hiveKeytab);
        }
        if (configuration.get(HIVE_PRINCIPAL_KEY) == null) {
            configuration.set(HIVE_PRINCIPAL_KEY, hivePrincipal);
        }
        SecurityUtil.login(configuration, HIVE_KEYTAB_FILE_KEY, HIVE_PRINCIPAL_KEY);
        LOG.info("Logged into hive with principal {}", configuration.get(HIVE_PRINCIPAL_KEY));
    }

    @Override
    public void doRenew(Map<String, String> credentials, Map<String, Object> topologyConf, String ownerPrincipal) {
        for (Pair<String, Credentials> cred : getCredentials(credentials)) {
            try {
                Configuration configuration = getHadoopConfiguration(topologyConf, cred.getFirst());
                String hiveMetaStoreURI = getMetaStoreURI(configuration);
                String hiveMetaStorePrincipal = getMetaStorePrincipal(configuration);

                Collection<Token<? extends TokenIdentifier>> tokens = cred.getSecond().getAllTokens();
                login(configuration);

                if (tokens != null && !tokens.isEmpty()) {
                    for (Token token : tokens) {
                        long expiration = renewToken(token, hiveMetaStoreURI, hiveMetaStorePrincipal);
                        LOG.info("Hive delegation token renewed, new expiration time {}", expiration);
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

    private long renewToken(Token token, String metaStoreURI, String hiveMetaStorePrincipal) {
        HCatClient hcatClient = null;
        if (UserGroupInformation.isSecurityEnabled()) {
            try {
                String tokenStr = token.encodeToUrlString();
                HiveConf hcatConf = createHiveConf(metaStoreURI, hiveMetaStorePrincipal);
                LOG.debug("renewing delegation tokens for principal={}", hiveMetaStorePrincipal);
                hcatClient = HCatClient.create(hcatConf);
                Long expiryTime = hcatClient.renewDelegationToken(tokenStr);
                LOG.info("Renewed delegation token. new expiryTime={}", expiryTime);
                return expiryTime;
            } catch (Exception ex) {
                throw new RuntimeException("Failed to renew delegation tokens.", ex);
            } finally {
                if (hcatClient != null)
                    try {
                        hcatClient.close();
                    } catch (HCatException e) {
                        LOG.error(" Exception", e);
                    }
            }
        } else {
            throw new RuntimeException("Security is not enabled for Hadoop");
        }
    }


    @Override
    protected String getCredentialKey(String configKey) {
        return HIVE_CREDENTIALS + configKey;
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map<String, Object> conf = new HashMap<>();
        final String topologyOwnerPrincipal = args[0]; //with realm e.g. storm@WITZEND.COM
        conf.put(HIVE_PRINCIPAL_KEY, args[1]); // hive principal storm-hive@WITZEN.COM
        conf.put(HIVE_KEYTAB_FILE_KEY, args[2]); // storm hive keytab /etc/security/keytabs/storm-hive.keytab
        conf.put(HiveConf.ConfVars.METASTOREURIS.varname, args[3]); // hive.metastore.uris : "thrift://pm-eng1-cluster1.field.hortonworks.com:9083"

        AutoHive autoHive = new AutoHive();
        autoHive.prepare(conf);

        Map<String, String> creds = new HashMap<>();
        autoHive.populateCredentials(creds, conf, topologyOwnerPrincipal);
        LOG.info("Got Hive credentials" + autoHive.getCredentials(creds));

        Subject subject = new Subject();
        autoHive.populateSubject(subject, creds);
        LOG.info("Got a Subject " + subject);

        //autoHive.renew(creds, conf, topologyOwnerPrincipal);
        //LOG.info("Renewed credentials" + autoHive.getCredentials(creds));
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

