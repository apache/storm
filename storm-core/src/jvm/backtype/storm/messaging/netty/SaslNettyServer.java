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
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SaslNettyServer {

	private static final Logger LOG = LoggerFactory
			.getLogger(SaslNettyServer.class);

	/**
     * Used to respond to clients, SaslServer with SASL tokens
     * represented as byte arrays.
     */
	private SaslServer saslServer;
	
	/**
     * Used to wrap and unwrap payload based on the negotiated qop.
     */
    private boolean useWrapUnwrap = false;

	SaslNettyServer(String topologyName, byte[] token, Map conf) throws IOException {
		LOG.debug("SaslNettyServer: Topology token is: " + topologyName
				+ " with authmethod " + SaslUtils.AUTH_DIGEST_MD5);

		try {

			SaslDigestCallbackHandler ch = new SaslNettyServer.SaslDigestCallbackHandler(
					topologyName, token);

			saslServer = Sasl.createSaslServer(SaslUtils.AUTH_DIGEST_MD5, null,
					SaslUtils.DEFAULT_REALM, SaslUtils.getSaslProps(conf), ch);

		} catch (SaslException e) {
			LOG.error("SaslNettyServer: Could not create SaslServer: " + e);
		}

	}
	
	/**
     * Disposes any resources held during sasl authentication. 
     * @throws SaslException
     */
    public void dispose() throws SaslException {
        saslServer.dispose();
    }
    
    /**
     * Gets the negotiated qop.
     * @return String containing the negotiated qop.
     */
    public String getNegotiatedQop() {
        return (String) saslServer.getNegotiatedProperty(Sasl.QOP);
    }
    
    /**
     * Check if sasl authentication is completed.
     * @return true if success else false
     */

    public boolean isComplete() {
        return saslServer.isComplete();
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
        String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
        this.useWrapUnwrap = qop != null && !"auth".equalsIgnoreCase(qop);
        LOG.debug("Setting SaslNettyServer useWrapUnwrap to "+ useWrapUnwrap);
    }
    
    /**
     * Unwrap the message payload using sasl server for incoming messages.
     * 
     * @param outgoing - wrapped message.
     * @param off - offset, usually starts at 0.
     * @param len - length of wrapped message 
     * @return
     * @throws SaslException
     */
    public byte[] unwrap(final byte[] outgoing, final int off, final int len)
            throws SaslException {
        return saslServer.unwrap(outgoing, off, len);
    }
    
    /**
     * Wrap the message payload using sasl server for outgoing messages.
     * 
     * @param outgoing - unwrapped message.
     * @param off - offset, usually starts at 0.
     * @param len - length of unwrapped message.
     * @return
     * @throws SaslException
     */
    public byte[] wrap(final byte[] outgoing, final int off, final int len)
            throws SaslException {
        return saslServer.wrap(outgoing, off, len);
    }

	public String getUserName() {
		return saslServer.getAuthorizationID();
	}

	/** CallbackHandler for SASL DIGEST-MD5 mechanism */
	public static class SaslDigestCallbackHandler implements CallbackHandler {

		/** Used to authenticate the clients */
		private byte[] userPassword;
		private String userName;

		public SaslDigestCallbackHandler(String topologyName, byte[] token) {
			LOG.debug("SaslDigestCallback: Creating SaslDigestCallback handler "
					+ "with topology token: " + topologyName);
			this.userName = topologyName;
			this.userPassword = token;
		}

		@Override
		public void handle(Callback[] callbacks) throws IOException,
				UnsupportedCallbackException {
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
					continue; // realm is ignored
				} else {
					throw new UnsupportedCallbackException(callback,
							"handle: Unrecognized SASL DIGEST-MD5 Callback");
				}
			}

			if (nc != null) {
				LOG.debug("handle: SASL server DIGEST-MD5 callback: setting "
						+ "username for client: " + userName);

				nc.setName(userName);
			}

			if (pc != null) {
				char[] password = SaslUtils.encodePassword(userPassword);

				LOG.debug("handle: SASL server DIGEST-MD5 callback: setting "
						+ "password for client: " + userPassword);

				pc.setPassword(password);
			}
			if (ac != null) {

				String authid = ac.getAuthenticationID();
				String authzid = ac.getAuthorizationID();

				if (authid.equals(authzid)) {
					ac.setAuthorized(true);
				} else {
					ac.setAuthorized(false);
				}

				if (ac.isAuthorized()) {
					LOG.debug("handle: SASL server DIGEST-MD5 callback: setting "
							+ "canonicalized client ID: " + userName);
					ac.setAuthorizedID(authzid);
				}
			}
		}
	}

	/**
	 * Used by SaslTokenMessage::processToken() to respond to server SASL
	 * tokens.
	 * 
	 * @param token
	 *            Server's SASL token
	 * @return token to send back to the server.
	 */
	public byte[] response(byte[] token) {
		try {
			LOG.debug("response: Responding to input token of length: "
					+ token.length);
			byte[] retval = saslServer.evaluateResponse(token);
			LOG.debug("response: Response token length: " + retval.length);
			return retval;
		} catch (SaslException e) {
			LOG.error("response: Failed to evaluate client token of length: "
					+ token.length + " : " + e);
			return null;
		}
	}
}