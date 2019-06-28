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

import java.net.ConnectException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.ILocalDRPC;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DistributedRPC;
import org.apache.storm.security.auth.ThriftClient;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class DRPCClient extends ThriftClient implements DistributedRPC.Iface {
    private static final Logger LOG = LoggerFactory.getLogger(DRPCClient.class);
    private static volatile ILocalDRPC localOverrideClient = null;
    private DistributedRPC.Iface client;
    private String host;
    private int port;

    private DRPCClient(DistributedRPC.Iface override) {
        super(new HashMap<>(), ThriftConnectionType.LOCAL_FAKE,
              "localhost", 1234, null, null);
        this.host = "localhost";
        this.port = 1234;
        this.client = override;
    }

    public DRPCClient(Map<String, Object> conf, String host, int port) throws TTransportException {
        this(conf, host, port, null);
        retryForever = true;
    }

    public DRPCClient(Map<String, Object> conf, String host, int port, Integer timeout) throws TTransportException {
        super(conf, localOverrideClient != null ? ThriftConnectionType.LOCAL_FAKE : ThriftConnectionType.DRPC,
              host, port, timeout, null);
        this.host = host;
        this.port = port;
        if (localOverrideClient != null) {
            this.client = localOverrideClient;
        } else {
            this.client = new DistributedRPC.Client(protocol);
        }
        retryForever = true;
    }

    /**
     * Check local override.
     * @return true of new clients will be overridden to connect to a local cluster and not the configured remote cluster
     */
    public static boolean isLocalOverride() {
        return localOverrideClient != null;
    }

    /**
     * Get override service ID.
     * @return the service ID of the local override DRPC instance
     */
    public static String getOverrideServiceId() {
        return localOverrideClient.getServiceId();
    }

    public static DRPCClient getConfiguredClient(Map<String, Object> conf) throws TTransportException {
        DistributedRPC.Iface override = localOverrideClient;
        if (override != null) {
            return new DRPCClient(override);
        }

        //Extend the config with defaults and the command line
        Map<String, Object> fullConf = Utils.readStormConfig();
        fullConf.putAll(Utils.readCommandLineOpts());
        fullConf.putAll(conf);

        int port = ObjectReader.getInt(fullConf.get(Config.DRPC_PORT), 3772);
        List<String> servers = (List<String>) fullConf.get(Config.DRPC_SERVERS);
        if (servers == null) {
            throw new IllegalStateException(Config.DRPC_SERVERS + " is not set, could not find any DRPC servers to connect to.");
        }
        Collections.shuffle(servers);
        RuntimeException excpt = null;
        for (String host : servers) {
            try {
                return new DRPCClient(fullConf, host, port);
            } catch (RuntimeException e) {
                if (Utils.exceptionCauseIsInstanceOf(ConnectException.class, e)) {
                    excpt = e;
                } else {
                    throw e;
                }
            }
        }
        if (excpt != null) {
            throw excpt;
        }
        throw new IllegalStateException("It appears that no drpc servers were configured.");
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String execute(String func, String args) throws TException, DRPCExecutionException, AuthorizationException {
        if (func == null) {
            throw new IllegalArgumentException("DRPC Function cannot be null");
        }
        LOG.debug("DRPC RUNNING \"{}\"(\"{}\")", func, args);
        return client.execute(func, args);
    }

    public DistributedRPC.Iface getClient() {
        return client;
    }

    public static class LocalOverride implements AutoCloseable {
        public LocalOverride(ILocalDRPC client) {
            localOverrideClient = client;
        }

        @Override
        public void close() throws Exception {
            localOverrideClient = null;
        }
    }
}