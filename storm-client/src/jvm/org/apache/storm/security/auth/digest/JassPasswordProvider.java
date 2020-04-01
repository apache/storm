/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.security.auth.digest;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import org.apache.storm.security.auth.ClientAuthUtils;
import org.apache.storm.security.auth.sasl.PasswordProvider;

/**
 * Provides passwords out of a jaas conf for typical MD5-DIGEST authentication support.
 */
public class JassPasswordProvider implements PasswordProvider {
    /**
     * The system property that sets a super user password.  This can be used in addition to the jaas conf, and takes precedent over a
     * "super" user in the jaas conf if this is set.
     */
    public static final String SYSPROP_SUPER_PASSWORD = "storm.SASLAuthenticationProvider.superPassword";
    private static final String USER_PREFIX = "user_";
    private Map<String, char[]> credentials = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param topoConf the configuration containing the jaas conf to use.
     * @throws IOException if we could not read the Server section in the jaas conf.
     */
    public JassPasswordProvider(Map<String, Object> topoConf) throws IOException {

        Configuration configuration = ClientAuthUtils.getConfiguration(topoConf);
        if (configuration == null) {
            return;
        }

        AppConfigurationEntry[] configurationEntries = configuration.getAppConfigurationEntry(ClientAuthUtils.LOGIN_CONTEXT_SERVER);
        if (configurationEntries == null) {
            String errorMessage = "Could not find a '" + ClientAuthUtils.LOGIN_CONTEXT_SERVER
                                  + "' entry in this configuration: Server cannot start.";
            throw new IOException(errorMessage);
        }
        credentials.clear();
        for (AppConfigurationEntry entry : configurationEntries) {
            Map<String, ?> options = entry.getOptions();
            // Populate user -> password map with JAAS configuration entries from the "Server" section.
            // Usernames are distinguished from other options by prefixing the username with a "user_" prefix.
            for (Map.Entry<String, ?> pair : options.entrySet()) {
                String key = pair.getKey();
                if (key.startsWith(USER_PREFIX)) {
                    String userName = key.substring(USER_PREFIX.length());
                    credentials.put(userName, ((String) pair.getValue()).toCharArray());
                }
            }
        }

        String superPassword = System.getProperty(SYSPROP_SUPER_PASSWORD);
        if (superPassword != null) {
            credentials.put("super", superPassword.toCharArray());
        }
    }

    @Override
    public Optional<char[]> getPasswordFor(String user) {
        return Optional.ofNullable(credentials.get(user));
    }

    @Override
    public boolean isImpersonationAllowed() {
        return true;
    }
}
