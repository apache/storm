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

package org.apache.storm.security.auth.kerberos;

import java.io.IOException;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import org.apache.storm.security.auth.ClientAuthUtils;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.sasl.SaslTransportPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SASL server side callback handler for kerberos auth.
 */
public class ServerCallbackHandler implements CallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ServerCallbackHandler.class);
    private final boolean impersonationAllowed;

    public ServerCallbackHandler(Map<String, Object> topoConf, boolean impersonationAllowed) throws IOException {
        this.impersonationAllowed = impersonationAllowed;

        Configuration configuration = ClientAuthUtils.getConfiguration(topoConf);
        if (configuration == null) {
            return;
        }

        AppConfigurationEntry[] configurationEntries = configuration.getAppConfigurationEntry(ClientAuthUtils.LOGIN_CONTEXT_SERVER);
        if (configurationEntries == null) {
            String errorMessage = "Could not find a '" + ClientAuthUtils.LOGIN_CONTEXT_SERVER
                                  + "' entry in this configuration: Server cannot start.";
            LOG.error(errorMessage);
            throw new IOException(errorMessage);
        }
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        NameCallback nc = null;
        PasswordCallback pc = null;
        AuthorizeCallback ac = null;
        for (Callback callback : callbacks) {
            if (callback instanceof AuthorizeCallback) {
                ac = (AuthorizeCallback) callback;
            } else if (callback instanceof NameCallback) {
                nc = (NameCallback) callback;
            } else if (callback instanceof PasswordCallback) {
                pc = (PasswordCallback) callback;
            } else if (callback instanceof RealmCallback) {
                //Ignored...
            } else {
                throw new UnsupportedCallbackException(callback,
                                                       "Unrecognized SASL Callback");
            }
        }

        String userName = "UNKNOWN";
        if (nc != null) {
            LOG.debug("handleNameCallback");
            userName = nc.getDefaultName();
            nc.setName(nc.getDefaultName());
        }

        if (pc != null) {
            LOG.error("No password found for user: {}, validate klist matches jaas conf", userName);
        }

        if (ac != null) {
            String authenticationId = ac.getAuthenticationID();
            LOG.debug("Successfully authenticated client: authenticationID={}  authorizationID= {}", authenticationId,
                      ac.getAuthorizationID());

            //if authorizationId is not set, set it to authenticationId.
            if (ac.getAuthorizationID() == null) {
                ac.setAuthorizedID(authenticationId);
            }

            //When authNid and authZid are not equal , authNId is attempting to impersonate authZid, We
            //add the authNid as the real user in reqContext's subject which will be used during authorization.
            if (!ac.getAuthenticationID().equals(ac.getAuthorizationID())) {
                if (!impersonationAllowed) {
                    throw new IllegalArgumentException(ac.getAuthenticationID() + " attempting to impersonate " + ac.getAuthorizationID()
                                                       + ".  This is not allowed by this server.");
                }
                ReqContext.context().setRealPrincipal(new SaslTransportPlugin.User(ac.getAuthenticationID()));
            } else {
                ReqContext.context().setRealPrincipal(null);
            }

            ac.setAuthorized(true);
        }
    }
}
