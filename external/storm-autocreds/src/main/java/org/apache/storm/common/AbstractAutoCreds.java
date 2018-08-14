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
package org.apache.storm.common;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.storm.security.INimbusCredentialPlugin;
import org.apache.storm.security.auth.IAutoCredentials;
import org.apache.storm.security.auth.ICredentialsRenewer;
import org.apache.storm.utils.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The base class that for auto credential plugins that abstracts out some of the common functionality.
 */
public abstract class AbstractAutoCreds implements IAutoCredentials, ICredentialsRenewer, INimbusCredentialPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractAutoCreds.class);
    public static final String CONFIG_KEY_RESOURCES = "resources";

    private Set<String> configKeys = new HashSet<>();
    private Map<String, Map<String, Object>> configMap = new HashMap<>();

    @Override
    public void prepare(Map conf) {
        doPrepare(conf);
        loadConfigKeys(conf);
        for (String key : configKeys) {
            if (conf.containsKey(key)) {
                Map<String, Object> config = (Map<String, Object>) conf.get(key);
                configMap.put(key, config);
                LOG.info("configKey = {}, config = {}", key, config);
            }
        }
    }

    @Override
    public void populateCredentials(Map<String, String> credentials, Map<String, Object> topoConf, final String topologyOwnerPrincipal) {
        try {
            List<String> loadedKeys = loadConfigKeys(topoConf);
            if (!loadedKeys.isEmpty()) {
                Map<String, Object> updatedConf = updateConfigs(topoConf);
                for (String configKey : loadedKeys) {
                    credentials.put(getCredentialKey(configKey),
                            DatatypeConverter.printBase64Binary(getHadoopCredentials(updatedConf, configKey, topologyOwnerPrincipal)));
                }
            } else {
                credentials.put(getCredentialKey(StringUtils.EMPTY),
                        DatatypeConverter.printBase64Binary(getHadoopCredentials(topoConf, topologyOwnerPrincipal)));
            }
            LOG.info("Tokens added to credentials map.");
        } catch (Exception e) {
            LOG.error("Could not populate credentials.", e);
        }
    }

    private Map<String, Object> updateConfigs(Map topologyConf) {
        Map<String, Object> res = new HashMap<>(topologyConf);
        for (String configKey : configKeys) {
            if (!res.containsKey(configKey) && configMap.containsKey(configKey)) {
                res.put(configKey, configMap.get(configKey));
            }
        }
        return res;
    }

    @Override
    public void renew(Map<String, String> credentials, Map<String, Object> topologyConf, String ownerPrincipal) {
        doRenew(credentials, updateConfigs(topologyConf), ownerPrincipal);
    }

    @Override
    public void populateCredentials(Map<String, String> credentials) {
        credentials.put(getCredentialKey(StringUtils.EMPTY),
                DatatypeConverter.printBase64Binary("dummy place holder".getBytes()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void populateSubject(Subject subject, Map<String, String> credentials) {
        addCredentialToSubject(subject, credentials);
        addTokensToUGI(subject);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void updateSubject(Subject subject, Map<String, String> credentials) {
        addCredentialToSubject(subject, credentials);
        addTokensToUGI(subject);
    }

    protected Set<Pair<String, Credentials>> getCredentials(Map<String, String> credentials) {
        Set<Pair<String, Credentials>> res = new HashSet<>();
        if (!configKeys.isEmpty()) {
            for (String configKey : configKeys) {
                Credentials cred = doGetCredentials(credentials, configKey);
                if (cred != null) {
                    res.add(new Pair(configKey, cred));
                }
            }
        } else {
            Credentials cred = doGetCredentials(credentials, StringUtils.EMPTY);
            if (cred != null) {
                res.add(new Pair(StringUtils.EMPTY, cred));
            }
        }
        return res;
    }

    protected void fillHadoopConfiguration(Map topoConf, String configKey, Configuration configuration) {
        Map<String, Object> config = (Map<String, Object>) topoConf.get(configKey);
        LOG.info("TopoConf {}, got config {}, for configKey {}", ConfigUtils.maskPasswords(topoConf),
                ConfigUtils.maskPasswords(config), configKey);
        if (config != null) {
            List<String> resourcesToLoad = new ArrayList<>();
            for (Map.Entry<String, Object> entry : config.entrySet()) {
                if (entry.getKey().equals(CONFIG_KEY_RESOURCES)) {
                    resourcesToLoad.addAll((List<String>) entry.getValue());
                } else {
                    configuration.set(entry.getKey(), String.valueOf(entry.getValue()));
                }
            }
            LOG.info("Resources to load {}", resourcesToLoad);
            // add configs from resources like hdfs-site.xml
            for (String pathStr : resourcesToLoad) {
                configuration.addResource(new Path(Paths.get(pathStr).toUri()));
            }
        }
        LOG.info("Initializing UGI with config {}", configuration);
        UserGroupInformation.setConfiguration(configuration);
    }

    /**
     * Prepare the plugin
     *
     * @param conf the storm cluster conf set via storm.yaml
     */
    protected abstract void doPrepare(Map conf);

    /**
     * The lookup key for the config key string
     *
     * @return the config key string
     */
    protected abstract String getConfigKeyString();

    /**
     * The key with which the credentials are stored in the credentials map
     */
    protected abstract String getCredentialKey(String configKey);

    protected abstract byte[] getHadoopCredentials(Map<String, Object> conf, String configKey, final String topologyOwnerPrincipal);

    protected abstract byte[] getHadoopCredentials(Map<String, Object> conf, final String topologyOwnerPrincipal);

    protected abstract void doRenew(Map<String, String> credentials, Map<String, Object> topologyConf, String ownerPrincipal);

    @SuppressWarnings("unchecked")
    private void addCredentialToSubject(Subject subject, Map<String, String> credentials) {
        try {
            for (Pair<String, Credentials> cred : getCredentials(credentials)) {
                subject.getPrivateCredentials().add(cred.getSecond());
                LOG.info("Credentials added to the subject.");
            }
        } catch (Exception e) {
            LOG.error("Failed to initialize and get UserGroupInformation.", e);
        }
    }

    private void addTokensToUGI(Subject subject) {
        if (subject != null) {
            Set<Credentials> privateCredentials = subject.getPrivateCredentials(Credentials.class);
            if (privateCredentials != null) {
                for (Credentials cred : privateCredentials) {
                    Collection<Token<? extends TokenIdentifier>> allTokens = cred.getAllTokens();
                    if (allTokens != null) {
                        for (Token<? extends TokenIdentifier> token : allTokens) {
                            try {

                                if (token == null) {
                                    LOG.debug("Ignoring null token");
                                    continue;
                                }

                                LOG.debug("Current user: {}", UserGroupInformation.getCurrentUser());
                                LOG.debug("Token from Credentials : {}", token);

                                TokenIdentifier tokenId = token.decodeIdentifier();
                                if (tokenId != null) {
                                    LOG.debug("Token identifier : {}", tokenId);
                                    LOG.debug("Username in token identifier : {}", tokenId.getUser());
                                }

                                UserGroupInformation.getCurrentUser().addToken(token);
                                LOG.info("Added delegation tokens to UGI.");
                            } catch (IOException e) {
                                LOG.error("Exception while trying to add tokens to ugi", e);
                            }
                        }
                    }
                }
            }
        }
    }

    private Credentials doGetCredentials(Map<String, String> credentials, String configKey) {
        Credentials credential = null;
        if (credentials != null && credentials.containsKey(getCredentialKey(configKey))) {
            try {
                byte[] credBytes = DatatypeConverter.parseBase64Binary(credentials.get(getCredentialKey(configKey)));
                ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(credBytes));

                credential = new Credentials();
                credential.readFields(in);
            } catch (Exception e) {
                LOG.error("Could not obtain credentials from credentials map.", e);
            }
        }
        return credential;

    }

    private List<String> loadConfigKeys(Map conf) {
        List<String> keys;
        if ((keys = (List<String>) conf.get(getConfigKeyString())) != null) {
            configKeys.addAll(keys);
        } else {
            keys = Collections.emptyList();
        }

        return keys;
    }

}
