/*
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

package org.apache.storm.security.auth;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage a collection of {@link ThriftServer}. This class itself is not thread-safe.
 */
public class MultiThriftServer<T extends ThriftServer> {

    private static final Logger LOG = LoggerFactory.getLogger(MultiThriftServer.class);

    private final String name;

    private final Map<ThriftConnectionType, T> thriftServerMap = new HashMap<>();
    private final Map<ThriftConnectionType, Thread> thriftServerThreadMap = new HashMap<>();
    private final Map<ThriftConnectionType, Boolean> thriftServerIsServingMap = new HashMap<>();


    public MultiThriftServer(String name) {
        this.name = name;
    }

    public void add(T thriftServer) {
        thriftServerMap.put(thriftServer.getType(), thriftServer);
        thriftServerThreadMap.put(thriftServer.getType(), new Thread(thriftServer::serve, name + "-" + thriftServer.getPort()));
    }

    public void serve() {
        for (ThriftServer thriftServer: thriftServerMap.values()) {
            if (Boolean.TRUE.equals(thriftServerIsServingMap.get(thriftServer.getType()))) {
                throw new IllegalStateException("The MultiThriftServer "
                        + thriftServerThreadMap.get(thriftServer.getType()).getName() + " is already serving");
            }
            LOG.info("Starting thrift server {}", thriftServerThreadMap.get(thriftServer.getType()).getName());
            thriftServerThreadMap.get(thriftServer.getType()).start();
            thriftServerIsServingMap.put(thriftServer.getType(), true);
        }
    }

    public void stop() {
        for (ThriftServer thriftServer: thriftServerMap.values()) {
            if (Boolean.TRUE.equals(thriftServerIsServingMap.get(thriftServer.getType()))) {
                thriftServerThreadMap.get(thriftServer.getType()).interrupt();
                thriftServerMap.get(thriftServer.getType()).stop();
                thriftServerIsServingMap.put(thriftServer.getType(), false);
            } else {
                LOG.warn("Can't stop the " + thriftServerThreadMap.get(thriftServer.getType()).getName()
                        + " server since it is not currently serving");
            }
        }
    }

    public void stopTlsServer(ThriftConnectionType tlsConnectionType) {
        if (Boolean.TRUE.equals(thriftServerIsServingMap.get(tlsConnectionType))) {
            thriftServerThreadMap.get(tlsConnectionType).interrupt();
            thriftServerMap.get(tlsConnectionType).stop();
            thriftServerIsServingMap.put(tlsConnectionType, false);
        } else {
            LOG.warn("Can't stop the " + tlsConnectionType
                    + " server since it is not currently serving");
        }
    }

    /**
     * Check if worker tokens are supported by any one of the thrift servers.
     * @return true if any thrift server supports Worker Tokens.
     */
    public boolean supportsWorkerTokens() {
        for (T server : thriftServerMap.values()) {
            if (server.supportsWorkerTokens()) {
                return true;
            }
        }
        return false;
    }

    public T get(ThriftConnectionType type) {
        return thriftServerMap.get(type);
    }
}
