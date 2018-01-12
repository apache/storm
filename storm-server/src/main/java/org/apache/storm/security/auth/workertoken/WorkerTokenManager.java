/*
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

package org.apache.storm.security.auth.workertoken;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import org.apache.storm.DaemonConfig;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.PrivateWorkerKey;
import org.apache.storm.generated.WorkerToken;
import org.apache.storm.generated.WorkerTokenInfo;
import org.apache.storm.generated.WorkerTokenServiceType;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The WorkerTokenManager manages the life cycle of worker tokens in nimbus.
 */
public class WorkerTokenManager {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerTokenManager.class);

    /**
     * The length of the random keys to use in bits.
     * This should be at least the length of WorkerTokenSigner.DEFAULT_HMAC_ALGORITHM.
     */
    private static final int KEY_LENGTH = 256;

    /**
     * Generate a new random secret key.
     * @return the new key
     */
    protected SecretKey generateSecret() {
        SecretKey key;
        synchronized (keyGen) {
            key = keyGen.generateKey();
        }
        return key;
    }

    /**
     * Get the secret that should be used to sign a token.  This may either reuse a secret or generate a new one so any user should
     * call this once and save the result.
     * @return the key to use.
     */
    protected SecretKey getCurrentSecret() {
        return generateSecret();
    }

    /**
     * Key generator to use.
     */
    private final KeyGenerator keyGen;
    private final IStormClusterState state;
    private final long tokenLifetimeMillis;

    /**
     * Constructor.  This assumes that state can store the tokens securely, and that they should be enabled at all.
     * Please use AuthUtils.areWorkerTokensEnabledServer to validate this first.
     * @param daemonConf the config for nimbus.
     * @param state the state used to store private keys.
     */
    public WorkerTokenManager(Map<String, Object> daemonConf, IStormClusterState state) {
        this.state = state;
        try {
            keyGen = KeyGenerator.getInstance(WorkerTokenSigner.DEFAULT_HMAC_ALGORITHM);
            keyGen.init(KEY_LENGTH);
        } catch (NoSuchAlgorithmException nsa) {
            throw new IllegalArgumentException("Can't find " + WorkerTokenSigner.DEFAULT_HMAC_ALGORITHM + " algorithm.");
        }
        this.tokenLifetimeMillis = TimeUnit.MILLISECONDS.convert(
            ObjectReader.getLong(daemonConf.get(DaemonConfig.STORM_WORKER_TOKEN_LIFE_TIME_HOURS),24L),
            TimeUnit.HOURS);
    }

    /**
     * Create or update an existing key.
     * @param serviceType the type of service to create a token for
     * @param user the user the token is for
     * @param topologyId the topology the token is for
     * @return a newly generated token that should be good to start using form now until it expires.
     */
    public WorkerToken createOrUpdateTokenFor(WorkerTokenServiceType serviceType, String user, String topologyId) {
        long nextVersion = state.getNextPrivateWorkerKeyVersion(serviceType, topologyId);
        SecretKey topoSecret = getCurrentSecret();
        long expirationTimeMillis = Time.currentTimeMillis() + tokenLifetimeMillis;
        WorkerTokenInfo info = new WorkerTokenInfo(user, topologyId, nextVersion, expirationTimeMillis);
        byte[] serializedInfo = AuthUtils.serializeWorkerTokenInfo(info);
        byte[] signature = WorkerTokenSigner.createPassword(serializedInfo, topoSecret);
        WorkerToken ret = new WorkerToken(serviceType, ByteBuffer.wrap(serializedInfo), ByteBuffer.wrap(signature));
        PrivateWorkerKey key = new PrivateWorkerKey(ByteBuffer.wrap(topoSecret.getEncoded()), user, expirationTimeMillis);
        state.addPrivateWorkerKey(serviceType, topologyId, nextVersion, key);
        LOG.info("Created new WorkerToken for user {} on service {}", user, serviceType);
        return ret;
    }

    /**
     * Get the maximum expiration token time that should be renewed.
     * @return any token with an expiration less than the returned value should be renewed.
     */
    public long getMaxExpirationTimeForRenewal() {
        return Time.currentTimeMillis() + (tokenLifetimeMillis / 2);
    }
}
