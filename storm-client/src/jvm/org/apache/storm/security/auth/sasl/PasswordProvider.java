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

import java.util.Optional;

/**
 * A very basic API that will provide a password for a given user name. This is intended to be used with the SimpleSaslServerCallbackHandler
 * to verify a user that is attempting to log in.
 */
public interface PasswordProvider {
    /**
     * Get an optional password for a user.  If no password for the user is found the option will be empty and another PasswordProvider
     * would be tried.
     *
     * @param user the user this is for.
     * @return the password if it is found.
     */
    Optional<char[]> getPasswordFor(String user);

    /**
     * Should impersonation be allowed by this password provider.  The default is false.
     *
     * @return true if it should else false.
     */
    default boolean isImpersonationAllowed() {
        return false;
    }

    /**
     * Convert the supplied user name to the actual user name that should be used in the system.  This may be called on any name.  If it
     * cannot be translated then a null may be returned or an exception thrown.  If getPassword returns successfully this should not return
     * null, nor throw an exception for the same user.
     *
     * @param user the SASL negotiated user name.
     * @return the user name that storm should use.
     */
    default String userName(String user) {
        return user;
    }
}
