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

package org.apache.storm.utils;

public class SecurityUtils {

    /**
     * Check if the suffix ends in pkcs12/p12/jks (case insensitive) and return PKCS12 ot JKS.
     * If not, then return null. The path can be url to a resource since only the ending is compared.
     *
     * @param path to the key resource file - can be embedded resource in a jar file.
     * @return PKCS12 or JKS or null
     */
    public static String inferKeyStoreTypeFromPath(String path) {
        if (path == null) {
            return null;
        }

        path = path.toLowerCase();
        if (path.endsWith(".p12") || path.endsWith(".pkcs12") || path.endsWith(".pfx")) {
            return "PKCS12";
        } else if (path.endsWith(".jks")) {
            return "JKS";
        } else {
            return null;
        }
    }

}
