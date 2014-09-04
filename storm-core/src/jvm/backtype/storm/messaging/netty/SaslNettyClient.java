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
package backtype.storm.messaging.netty;

import java.io.IOException;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements SASL logic for storm worker client processes.
 */
public class SaslNettyClient {

    private static final Logger LOG = LoggerFactory
            .getLogger(SaslNettyClient.class);

    /**
     * Used to respond to server's counterpart, SaslClient with SASL tokens
     * represented as byte arrays.
     */
    private SaslClient saslClient;
    
    /**
     * Used to wrap and unwrap payload based on the negotiated qop.
     */
    private boolean useWrapUnwrap = false;

    /**
     * Create a SaslNettyClient for authentication with servers.
     */
    public SaslNettyClient(String topologyName, byte[] token, Map conf) {
        try {
            LOG.debug("SaslNettyClient: Creating SASL "
                    + SaslUtils.AUTH_DIGEST_MD5
                    + " client to authenticate to server ");

            saslClient = Sasl.createSaslClient(
                    new String[] { SaslUtils.AUTH_DIGEST_MD5 }, null, null,
                    SaslUtils.DEFAULT_REALM, SaslUtils.getSaslProps(conf),
                    new SaslClientCallbackHandler(topologyName, token));

        } catch (IOException e) {
            LOG.error("SaslNettyClient: Could not obtain topology token for Netty "
                    + "Client to use to authenticate with a Netty Server.");
            saslClient = null;
        }
    }
    
    /**
     * Disposes any resources held during sasl authentication. 
     * @throws SaslException
     */
    public void dispose() throws SaslException {
        saslClient.dispose();
    }
    
    /**
     * Gets the negotiated qop.
     * @return String containing the negotiated qop.
     */
    public String getNegotiatedQop() {
        return (String) saslClient.getNegotiatedProperty(Sasl.QOP);
    }
    
    /**
     * Check if sasl authentication is completed.
     * @return true if success else false
     */

    public boolean isComplete() {
        return saslClient.isComplete();
    }
    
    /**
     * Check if wrap and unwrap is required.
     * @return true if wrap and unwrap required else false.
     */
    public boolean isUseWrapUnwrap() {
    	return this.useWrapUnwrap;
    }
    
    /**
     * Sets the wrap & unwrap feature if the negotiated property contains
     * "auth-int" or "auth-conf"
     */
    public void setUseWrapUnwrap() {
        String qop = (String) saslClient.getNegotiatedProperty(Sasl.QOP);
        this.useWrapUnwrap = qop != null && !"auth".equalsIgnoreCase(qop);
        LOG.debug("Setting SaslNettyClient useWrapUnwrap to "+ useWrapUnwrap);
    }

    /**
     * Unwrap the message payload using sasl client for incoming messages.
     * 
     * @param outgoing - wrapped message.
     * @param off - offset, usually starts at 0.
     * @param len - length of wrapped message 
     * @return
     * @throws SaslException
     */
    public byte[] unwrap(final byte[] outgoing, final int off, final int len)
            throws SaslException {
        return saslClient.unwrap(outgoing, off, len);
    }
    
    /**
     * Wrap the message payload using sasl client for outgoing messages.
     * 
     * @param outgoing - unwrapped message.
     * @param off - offset, usually starts at 0.
     * @param len - length of unwrapped message.
     * @return
     * @throws SaslException
     */
    public byte[] wrap(final byte[] outgoing, final int off, final int len)
            throws SaslException {
        return saslClient.wrap(outgoing, off, len);
    }
    

    /**
     * Respond to server's SASL token.
     * 
     * @param saslTokenMessage
     *            contains server's SASL token
     * @return client's response SASL token
     */
    public byte[] saslResponse(SaslMessageToken saslTokenMessage) {
        try {
            byte[] retval = saslClient.evaluateChallenge(saslTokenMessage
                    .getSaslToken());
            return retval;
        } catch (SaslException e) {
            LOG.error(
                    "saslResponse: Failed to respond to SASL server's token:",
                    e);
            return null;
        }
    }

    /**
     * Implementation of javax.security.auth.callback.CallbackHandler that works
     * with Storm topology tokens.
     */
    private static class SaslClientCallbackHandler implements CallbackHandler {
        /** Generated username contained in TopologyToken */
        private final String userName;
        /** Generated password contained in TopologyToken */
        private final char[] userPassword;

        /**
         * Set private members using topology token.
         * 
         * @param topologyToken
         */
        public SaslClientCallbackHandler(String topologyToken, byte[] token) {
            this.userName = SaslUtils
                    .encodeIdentifier(topologyToken.getBytes());
            this.userPassword = SaslUtils.encodePassword(token);
        }

        /**
         * Implementation used to respond to SASL tokens from server.
         * 
         * @param callbacks
         *            objects that indicate what credential information the
         *            server's SaslServer requires from the client.
         * @throws UnsupportedCallbackException
         */
        public void handle(Callback[] callbacks)
                throws UnsupportedCallbackException {
            NameCallback nc = null;
            PasswordCallback pc = null;
            RealmCallback rc = null;
            for (Callback callback : callbacks) {
                if (callback instanceof RealmChoiceCallback) {
                    continue;
                } else if (callback instanceof NameCallback) {
                    nc = (NameCallback) callback;
                } else if (callback instanceof PasswordCallback) {
                    pc = (PasswordCallback) callback;
                } else if (callback instanceof RealmCallback) {
                    rc = (RealmCallback) callback;
                } else {
                    throw new UnsupportedCallbackException(callback,
                            "handle: Unrecognized SASL client callback");
                }
            }
            if (nc != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("handle: SASL client callback: setting username: "
                            + userName);
                }
                nc.setName(userName);
            }
            if (pc != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("handle: SASL client callback: setting userPassword");
                }
                pc.setPassword(userPassword);
            }
            if (rc != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("handle: SASL client callback: setting realm: "
                            + rc.getDefaultText());
                }
                rc.setText(rc.getDefaultText());
            }
        }
    }
}
