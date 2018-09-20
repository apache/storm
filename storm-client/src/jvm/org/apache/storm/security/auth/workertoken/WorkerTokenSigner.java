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

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.Mac;
import javax.crypto.SecretKey;

/**
 * Provides everything needed to sign a worker token with a secret key.
 */
class WorkerTokenSigner {
    /**
     * The name of the hashing algorithm.
     */
    static final String DEFAULT_HMAC_ALGORITHM = "HmacSHA256";

    /**
     * A thread local store for the Macs.
     */
    private static final ThreadLocal<Mac> threadLocalMac =
        ThreadLocal.withInitial(() -> {
            try {
                return Mac.getInstance(DEFAULT_HMAC_ALGORITHM);
            } catch (NoSuchAlgorithmException nsa) {
                throw new IllegalArgumentException("Can't find " + DEFAULT_HMAC_ALGORITHM + " algorithm.");
            }
        });

    /**
     * Compute HMAC of the identifier using the secret key and return the output as password.
     *
     * @param identifier the bytes of the identifier
     * @param key        the secret key
     * @return the bytes of the generated password
     */
    static byte[] createPassword(byte[] identifier, SecretKey key) {
        Mac mac = threadLocalMac.get();
        try {
            mac.init(key);
        } catch (InvalidKeyException ike) {
            throw new IllegalArgumentException("Invalid key to HMAC computation", ike);
        }
        return mac.doFinal(identifier);
    }
}
