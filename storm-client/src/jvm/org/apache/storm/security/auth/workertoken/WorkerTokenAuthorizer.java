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

package org.apache.storm.security.auth.workertoken;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.PrivateWorkerKey;
import org.apache.storm.generated.WorkerTokenInfo;
import org.apache.storm.generated.WorkerTokenServiceType;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.storm.security.auth.sasl.PasswordProvider;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allow for SASL authentication using worker tokens.
 */
public class WorkerTokenAuthorizer implements PasswordProvider {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerTokenAuthorizer.class);

    private static IStormClusterState buildStateIfNeeded(Map<String, Object> conf, ThriftConnectionType connectionType) {
        IStormClusterState state = null;

        if (AuthUtils.areWorkerTokensEnabledServer(connectionType, conf)) {
            try {
                state = ClusterUtils.mkStormClusterState(conf, new ClusterStateContext(DaemonType.UNKNOWN, conf));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return state;
    }

    private final LoadingCache<WorkerTokenInfo, PrivateWorkerKey> keyCache;

    /**
     * Constructor.
     * @param conf the daemon config for the server.
     * @param connectionType the type of connection we are authorizing.
     */
    public WorkerTokenAuthorizer(Map<String, Object> conf, ThriftConnectionType connectionType) {
        this(connectionType.getWtType(), buildStateIfNeeded(conf, connectionType));
    }

    @VisibleForTesting
    WorkerTokenAuthorizer(final WorkerTokenServiceType serviceType, final IStormClusterState state) {
        LoadingCache<WorkerTokenInfo, PrivateWorkerKey> tmpKeyCache = null;
        if (state != null) {
            tmpKeyCache =
                CacheBuilder.newBuilder()
                    .maximumSize(2_000)
                    .expireAfterWrite(2, TimeUnit.HOURS)
                    .build(new CacheLoader<WorkerTokenInfo, PrivateWorkerKey>() {

                        @Override
                        public PrivateWorkerKey load(WorkerTokenInfo wtInfo) {
                            return state.getPrivateWorkerKey(serviceType,
                                wtInfo.get_topologyId(),
                                wtInfo.get_secretVersion());
                        }
                    });
        }
        keyCache = tmpKeyCache;
    }

    @VisibleForTesting
    byte[] getSignedPasswordFor(byte[] user, WorkerTokenInfo deser) {
        assert keyCache != null;

        if (deser.is_set_expirationTimeMillis() && deser.get_expirationTimeMillis() <= Time.currentTimeMillis()) {
            throw new IllegalArgumentException("Token is not valid, token has expired.");
        }

        PrivateWorkerKey key = keyCache.getUnchecked(deser);
        if (key == null) {
            throw new IllegalArgumentException("Token is not valid, private key not found.");
        }

        if (key.is_set_expirationTimeMillis() && key.get_expirationTimeMillis() <= Time.currentTimeMillis()) {
            throw new IllegalArgumentException("Token is not valid, key has expired.");
        }

        return WorkerTokenSigner.createPassword(user, new SecretKeySpec(key.get_key(), WorkerTokenSigner.DEFAULT_HMAC_ALGORITHM));
    }

    @Override
    public Optional<char[]> getPasswordFor(String userName) {
        if (keyCache == null) {
            return Optional.empty();
        }
        try {
            byte[] user = Base64.getDecoder().decode(userName);
            WorkerTokenInfo deser = Utils.deserialize(user, WorkerTokenInfo.class);
            byte[] password = getSignedPasswordFor(user, deser);
            return Optional.of(Base64.getEncoder().encodeToString(password).toCharArray());
        } catch (Exception e) {
            LOG.debug("Could not decode {}, might just be a plain digest request...", userName, e);
            return Optional.empty();
        }
    }

    @Override
    public String userName(String userName) {
        byte[] user = Base64.getDecoder().decode(userName);
        WorkerTokenInfo deser = Utils.deserialize(user, WorkerTokenInfo.class);
        return deser.get_userName();
    }
}