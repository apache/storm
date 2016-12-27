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
package org.apache.storm.logging;
import java.net.InetAddress;
import java.security.Principal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftAccessLogger {
    private static final Logger LOG = LoggerFactory.getLogger(ThriftAccessLogger.class);

    private static String accessLogBase = "Request ID: {} access from: {} principal: {} operation: {}";

    public static void logAccessFunction(Integer requestId, InetAddress remoteAddress,
                                         Principal principal, String operation, 
                                         String function) {
        String functionPart = "";
        if (function != null) {
            functionPart = " function: {}";
        }

        LOG.info(accessLogBase + functionPart,
                 requestId, remoteAddress, principal, operation, function);
    }

    public static void logAccess(Integer requestId, InetAddress remoteAddress,
                                 Principal principal, String operation,
                                 String stormName, String accessResult) {
        if (stormName != null && accessResult != null) {
            LOG.info(accessLogBase + " storm-name: {} access result: {}",
                     requestId, remoteAddress, principal, operation, stormName, accessResult);
        } else if (accessResult != null) {
            LOG.info(accessLogBase + " access result: {}",
                     requestId, remoteAddress, principal, operation, accessResult);
        } else {
            // for completeness
            LOG.info(accessLogBase + " storm-name: {}",
                     requestId, remoteAddress, principal, operation, stormName);
        }
    }
}
