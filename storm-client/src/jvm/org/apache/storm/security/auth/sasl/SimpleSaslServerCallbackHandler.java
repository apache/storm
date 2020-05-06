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

package org.apache.storm.security.auth.sasl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.streams.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSaslServerCallbackHandler implements CallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleSaslServerCallbackHandler.class);
    private final List<PasswordProvider> providers;
    private final boolean impersonationAllowed;

    /**
     * Constructor with different password providers.
     *
     * @param impersonationAllowed true if impersonation is allowed else false.
     * @param providers            what will provide a password.  They will be checked in order, and the first one to return a password
     *                             wins.
     */
    public SimpleSaslServerCallbackHandler(boolean impersonationAllowed, PasswordProvider... providers) {
        this(impersonationAllowed, Arrays.asList(providers));
    }

    /**
     * Constructor with different password providers.
     *
     * @param impersonationAllowed true if impersonation is allowed else false.
     * @param providers            what will provide a password.  They will be checked in order, and the first one to return a password
     *                             wins.
     */
    public SimpleSaslServerCallbackHandler(boolean impersonationAllowed, List<PasswordProvider> providers) {
        this.impersonationAllowed = impersonationAllowed;
        this.providers = new ArrayList<>(providers);
    }

    private static void log(String type, AuthorizeCallback ac, NameCallback nc, PasswordCallback pc, RealmCallback rc) {
        if (LOG.isDebugEnabled()) {
            String acs = "null";
            if (ac != null) {
                acs = "athz: " + ac.getAuthorizationID() + " athn: " + ac.getAuthenticationID() + " authorized: " + ac.getAuthorizedID();
            }

            String ncs = "null";
            if (nc != null) {
                ncs = "default: " + nc.getDefaultName() + " name: " + nc.getName();
            }

            String pcs = "null";
            if (pc != null) {
                char[] pwd = pc.getPassword();
                pcs = "password: " + (pwd == null ? "null" : "not null " + pwd.length);
            }

            String rcs = "null";
            if (rc != null) {
                rcs = "default: " + rc.getDefaultText() + " text: " + rc.getText();
            }
            LOG.debug("{}\nAC: {}\nNC: {}\nPC: {}\nRC: {}", type, acs, ncs, pcs, rcs);
        }
    }

    private Pair<String, Boolean> translateName(String orig) {
        for (PasswordProvider provider : providers) {
            try {
                String ret = provider.userName(orig);
                if (ret != null) {
                    return Pair.of(ret, provider.isImpersonationAllowed());
                }
            } catch (Exception e) {
                //Translating the name (this call) happens in a different callback from validating
                // the user name and password. This has to be stateless though, so we cannot save
                // the password provider away to be sure we got the same one that validated the password.
                // If the password providers are written correctly this should never happen,
                // because if they cannot read the name they would return a null.
                // But on the off chance that something goes wrong with the translation because of a mismatch
                // we try to skip the bad one.
                LOG.debug("{} could not read name from {}", provider, orig, e);
            }
        }
        // In the worst case we will return a serialized name after a password provider said that the password
        // was okay.  In that case the ACLs are likely to prevent the request from going through anyways.
        // But that is only if there is a bug in one of the password providers.
        return Pair.of(orig, false);
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException, IOException {
        NameCallback nc = null;
        PasswordCallback pc = null;
        AuthorizeCallback ac = null;
        RealmCallback rc = null;
        for (Callback callback : callbacks) {
            if (callback instanceof AuthorizeCallback) {
                ac = (AuthorizeCallback) callback;
            } else if (callback instanceof NameCallback) {
                nc = (NameCallback) callback;
            } else if (callback instanceof PasswordCallback) {
                pc = (PasswordCallback) callback;
            } else if (callback instanceof RealmCallback) {
                rc = (RealmCallback) callback;
            } else {
                throw new UnsupportedCallbackException(callback,
                                                       "Unrecognized SASL Callback");
            }
        }

        log("GOT", ac, nc, pc, rc);

        if (nc != null) {
            String userName = nc.getDefaultName();
            boolean passwordFound = false;
            for (PasswordProvider provider : providers) {
                Optional<char[]> password = provider.getPasswordFor(userName);
                if (password.isPresent()) {
                    pc.setPassword(password.get());
                    nc.setName(provider.userName(userName));
                    passwordFound = true;
                    break;
                }
            }
            if (!passwordFound) {
                LOG.warn("No password found for user: {}", userName);
                throw new IOException("NOT ALLOWED.");
            }
        }

        if (rc != null) {
            rc.setText(rc.getDefaultText());
        }

        if (ac != null) {
            boolean allowImpersonation = impersonationAllowed;
            String nid = ac.getAuthenticationID();
            if (nid != null) {
                Pair<String, Boolean> tmp = translateName(nid);
                nid = tmp.getFirst();
                allowImpersonation = allowImpersonation && tmp.getSecond();
            }

            String zid = ac.getAuthorizationID();
            if (zid != null) {
                Pair<String, Boolean> tmp = translateName(zid);
                zid = tmp.getFirst();
                allowImpersonation = allowImpersonation && tmp.getSecond();
            }
            LOG.debug("Successfully authenticated client: authenticationID = {} authorizationID = {}",
                     nid, zid);

            //if authorizationId is not set, set it to authenticationId.
            if (zid == null) {
                ac.setAuthorizedID(nid);
                zid = nid;
            } else {
                ac.setAuthorizedID(zid);
            }

            //When nid and zid are not equal, nid is attempting to impersonate zid, We
            //add the nid as the real user in reqContext's subject which will be used during authorization.
            if (!Objects.equals(nid, zid)) {
                LOG.info("Impersonation attempt  authenticationID = {} authorizationID = {}",
                         nid, zid);
                if (!allowImpersonation) {
                    throw new IllegalArgumentException(ac.getAuthenticationID() + " attempting to impersonate " + ac.getAuthorizationID()
                                                       + ".  This is not allowed.");
                }
                ReqContext.context().setRealPrincipal(new SaslTransportPlugin.User(nid));
            } else {
                ReqContext.context().setRealPrincipal(null);
            }

            ac.setAuthorized(true);
        }
        log("FINISHED", ac, nc, pc, rc);
    }
}
