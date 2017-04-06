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
package org.apache.storm.utils;

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
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public class DRPCClient extends ThriftClient implements DistributedRPC.Iface {
    private static volatile ILocalDRPC _localOverrideClient = null;
    
    public static class LocalOverride implements AutoCloseable {
        public LocalOverride(ILocalDRPC client) {
            _localOverrideClient = client;
        }
        
        @Override
        public void close() throws Exception {
            _localOverrideClient = null;
        }
    }
    
    /**
     * @return true of new clients will be overridden to connect to a local cluster
     * and not the configured remote cluster.
     */
    public static boolean isLocalOverride() {
        return _localOverrideClient != null;
    }
    
    /**
     * @return the service ID of the local override DRPC instance
     */
    public static String getOverrideServiceId() {
        return _localOverrideClient.getServiceId();
    }

    public static DRPCClient getConfiguredClient(Map conf) throws TTransportException {
        DistributedRPC.Iface override = _localOverrideClient;
        if (override != null) {
            return new DRPCClient(override);
        }

        List<String> servers = (List<String>) conf.get(Config.DRPC_SERVERS);
        Collections.shuffle(servers);
        String host = servers.get(0);
        int port = Integer.parseInt(conf.get(Config.DRPC_PORT).toString());
        return new DRPCClient(conf, host, port);
    }
    
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
    
    public DRPCClient(Map conf, String host, int port) throws TTransportException {
        this(conf, host, port, null);
        _retryForever = true;
    }

    public DRPCClient(Map conf, String host, int port, Integer timeout) throws TTransportException {
        super(conf, _localOverrideClient != null ? ThriftConnectionType.LOCAL_FAKE : ThriftConnectionType.DRPC,
                host, port, timeout, null);
        this.host = host;
        this.port = port;
        if (_localOverrideClient != null) {
            this.client = _localOverrideClient;
        } else {
            this.client = new DistributedRPC.Client(_protocol);
        }
        _retryForever = true;
    }
        
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }
    
    public String execute(String func, String args) throws TException, DRPCExecutionException, AuthorizationException {
        return client.execute(func, args);
    }

    public DistributedRPC.Iface getClient() {
        return client;
    }
}
