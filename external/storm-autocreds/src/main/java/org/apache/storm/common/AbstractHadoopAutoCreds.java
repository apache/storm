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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import javax.xml.bind.DatatypeConverter;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.storm.security.auth.IAutoCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The base class that for auto credential plugins that abstracts out some of the common functionality.
 */
public abstract class AbstractHadoopAutoCreds implements IAutoCredentials, CredentialKeyProvider {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractHadoopAutoCreds.class);

    private Set<String> configKeys = new HashSet<>();

    @Override
    public void prepare(Map<String, Object> topoConf) {
        doPrepare(topoConf);
        loadConfigKeys(topoConf);
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
        addTokensToUgi(subject);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateSubject(Subject subject, Map<String, String> credentials) {
        addCredentialToSubject(subject, credentials);
        addTokensToUgi(subject);
    }

    public Set<Pair<String, Credentials>> getCredentials(Map<String, String> credentials) {
        return HadoopCredentialUtil.getCredential(this, credentials, configKeys);
    }

    /**
     * Prepare the plugin.
     *
     * @param topoConf the topology conf
     */
    protected abstract void doPrepare(Map<String, Object> topoConf);

    /**
     * The lookup key for the config key string.
     *
     * @return the config key string
     */
    protected abstract String getConfigKeyString();

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

    private void addTokensToUgi(Subject subject) {
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

    private void loadConfigKeys(Map<String, Object> conf) {
        List<String> keys;
        String configKeyString = getConfigKeyString();
        if ((keys = (List<String>) conf.get(configKeyString)) != null) {
            configKeys.addAll(keys);
        }
    }

}