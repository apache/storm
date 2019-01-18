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

package org.apache.storm.security.auth.workertoken;

import java.security.AccessController;
import java.util.Base64;
import javax.security.auth.Subject;
import org.apache.storm.generated.WorkerToken;
import org.apache.storm.generated.WorkerTokenServiceType;
import org.apache.storm.security.auth.ClientAuthUtils;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.storm.security.auth.sasl.SimpleSaslClientCallbackHandler;

/**
 * A Client callback handler for a WorkerToken.  In general a client that wants to support worker tokens should first check if a WorkerToken
 * is available for the specific connection type by calling findWorkerTokenInSubject.  If that returns a token, then proceed to create and
 * use this with a DIGEST-MD5 SaslClient. If not you should fall back to whatever other client auth you want to do.
 */
public class WorkerTokenClientCallbackHandler extends SimpleSaslClientCallbackHandler {

    /**
     * Constructor.
     *
     * @param token the token to use to authenticate.  This was probably retrieved by calling findWorkerTokenInSubject.
     */
    public WorkerTokenClientCallbackHandler(WorkerToken token) {
        super(Base64.getEncoder().encodeToString(token.get_info()),
              Base64.getEncoder().encodeToString(token.get_signature()));
    }

    /**
     * Look in the current subject for a WorkerToken.  This should really only happen when we are in a worker, because the tokens will not
     * be placed in anything else.
     *
     * @param type the type of connection we need a token for.
     * @return the found token or null.
     */
    public static WorkerToken findWorkerTokenInSubject(ThriftConnectionType type) {
        WorkerTokenServiceType serviceType = type.getWtType();
        WorkerToken ret = null;
        if (serviceType != null) {
            Subject subject = Subject.getSubject(AccessController.getContext());
            if (subject != null) {
                ret = ClientAuthUtils.findWorkerToken(subject, serviceType);
            }
        }
        return ret;
    }
}
