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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.xml.bind.DatatypeConverter;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.security.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for getting credential for Hadoop.
 */
final class HadoopCredentialUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopCredentialUtil.class);

    private HadoopCredentialUtil() {
    }

    static Set<Pair<String, Credentials>> getCredential(CredentialKeyProvider provider,
            Map<String, String> credentials,
            Collection<String> configKeys) {
        Set<Pair<String, Credentials>> res = new HashSet<>();
        if (!configKeys.isEmpty()) {
            for (String configKey : configKeys) {
                Credentials cred = doGetCredentials(provider, credentials, configKey);
                if (cred != null) {
                    res.add(new Pair(configKey, cred));
                }
            }
        } else {
            Credentials cred = doGetCredentials(provider, credentials, StringUtils.EMPTY);
            if (cred != null) {
                res.add(new Pair(StringUtils.EMPTY, cred));
            }
        }
        return res;
    }

    private static Credentials doGetCredentials(CredentialKeyProvider provider,
            Map<String, String> credentials,
            String configKey) {
        Credentials credential = null;
        String credentialKey = provider.getCredentialKey(configKey);
        if (credentials != null && credentials.containsKey(credentialKey)) {
            try {
                byte[] credBytes = DatatypeConverter.parseBase64Binary(credentialKey);
                ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(credBytes));

                credential = new Credentials();
                credential.readFields(in);
            } catch (Exception e) {
                LOG.error("Could not obtain credentials from credentials map.", e);
            }
        }
        return credential;
    }

}
