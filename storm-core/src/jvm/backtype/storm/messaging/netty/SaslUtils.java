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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.security.sasl.Sasl;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

class SaslUtils {
	private static final Logger LOG = LoggerFactory.getLogger(SaslUtils.class);
	
    public static final String AUTH_DIGEST_MD5 = "DIGEST-MD5";
    public static final String DEFAULT_REALM = "default";
    
    /**
     * Checks the negotiated quality of protection is included in the given SASL
     * properties and therefore acceptable.
     * 
     * @param sasl
     *            participant to check
     * @param conf
     *            configuration parameters
     * @throws IOException
     *             for any error
     */
    public static void checkSaslNegotiatedProtection(Object sasl, Map conf)
            throws IOException {

        List<String> requestedQop = new ArrayList<String>();
        requestedQop.add(getRequestedQoP(conf));

        String negotiatedQop = null;
        if (sasl instanceof SaslNettyClient) {
            negotiatedQop = ((SaslNettyClient) sasl).getNegotiatedQop();
        } else {
            negotiatedQop = ((SaslNettyServer) sasl).getNegotiatedQop();
        }

        LOG.debug("Verifying QOP, requested QOP = {}, negotiated QOP = {}", requestedQop, negotiatedQop);
        if (!requestedQop.contains(negotiatedQop)) {
            throw new IOException(
                    String.format(
                            "SASL handshake completed, but "
                                    + "channel does not have acceptable quality of protection, "
                                    + "requested = %s, negotiated = %s",
                            requestedQop, negotiatedQop));
        }
    }
    

    /**
     * Generates the sasl properties with the requested qop and server auth set.
     * @param conf
     * @return properties containing requested qop and server auth set.
     */
	static Map<String, String> getSaslProps(Map conf) {
		Map<String, String> properties = new TreeMap<String, String>();

		List<String> requestedQop = new ArrayList<String>();
		requestedQop.add(getRequestedQoP(conf));

		String[] qop = new String[requestedQop.size()];
		for (int i = 0; i < requestedQop.size(); i++) {
			qop[i] = requestedQop.get(i).toUpperCase();
		}

		properties.put(Sasl.QOP, SaslUtils.join(",", qop));
		properties.put(Sasl.SERVER_AUTH, "true");

		return properties;
	}

    /**
     * Encode a password as a base64-encoded char[] array.
     * 
     * @param password
     *            as a byte array.
     * @return password as a char array.
     */
    static char[] encodePassword(byte[] password) {
        return new String(Base64.encodeBase64(password), Charsets.UTF_8)
                .toCharArray();
    }

    /**
     * Encode a identifier as a base64-encoded char[] array.
     * 
     * @param identifier
     *            as a byte array.
     * @return identifier as a char array.
     */
    static String encodeIdentifier(byte[] identifier) {
        return new String(Base64.encodeBase64(identifier), Charsets.UTF_8);
    }

    static String getSecretKey(Map conf) {
        if (conf == null || conf.isEmpty())
            return null;

        String secretPayLoad = (String) conf
                .get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD);

        return secretPayLoad;
    }
    
    /**
     * Gets the requested quality of protection (qop).
     * 
     * @param conf
     * @return requested qop
     */
    static String getRequestedQoP(Map conf) {
    	if(conf==null || conf.isEmpty()) {
    		return null;
    	}
    	return (String) conf.get(Config.STORM_MESSAGING_NETTY_PROTECTION);
    }
    
    /**
     * Concatenates strings, using a separator.
     * 
     * @param separator
     *            to join with
     * @param strings
     *            to join
     * @return the joined string
     */
    static String join(final CharSequence separator,
            final String[] strings) {
        // Ideally we don't have to duplicate the code here if array is
        // iterable.
        final StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (final String s : strings) {
            if (first) {
                first = false;
            } else {
                sb.append(separator);
            }
            sb.append(s);
        }
        return sb.toString();
    }
    
    /**
     * Check if we need to use wrap & unwrap functionality.
     * 
     * @param obj
     * @return
     */
	static boolean isUseUnwrap(Object obj) {
		SaslNettyClient saslNettyClient = null;
		SaslNettyServer saslNettyServer = null;

		// If authentication is not completed yet and channel connection is not
		// established, then return the object as it is.
		if (obj == null) {
			return false;
		}

		if (obj instanceof SaslNettyClient) {
			saslNettyClient = (SaslNettyClient) obj;

			// If sasl authentication is not completed yet.
			if (saslNettyClient != null && !saslNettyClient.isComplete()) {
				return false;
			}

			// If wrap functionality is not required, then send
			// the object as it is.
			if (saslNettyClient != null && !saslNettyClient.isUseWrapUnwrap()) {
				return false;
			}
		} else {
			saslNettyServer = (SaslNettyServer) obj;

			// If sasl authentication is not completed yet.
			if (saslNettyServer != null && !saslNettyServer.isComplete()) {
				return false;
			}

			// If wrap functionality is not required, then send
			// the object as it is.
			if (saslNettyServer != null && !saslNettyServer.isUseWrapUnwrap()) {
				return false;
			}

		}

		return true;
	}

}
