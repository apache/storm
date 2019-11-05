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

package org.apache.storm.security.auth;

import java.util.Map;
import javax.security.auth.login.Configuration;
import org.apache.storm.Config;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.protocol.TProtocol;
import org.apache.storm.thrift.transport.TSocket;
import org.apache.storm.thrift.transport.TTransport;
import org.apache.storm.utils.ObjectReader;

public class ThriftClient implements AutoCloseable {
    protected TProtocol protocol;
    protected boolean retryForever = false;
    private TTransport transport;
    private String host;
    private Integer port;
    private Integer timeout;
    private Map conf;
    private ThriftConnectionType type;
    private String asUser;

    public ThriftClient(Map<String, Object> topoConf, ThriftConnectionType type, String host) {
        this(topoConf, type, host, null, null, null);
    }

    public ThriftClient(Map<String, Object> topoConf, ThriftConnectionType type, String host, Integer port, Integer timeout) {
        this(topoConf, type, host, port, timeout, null);
    }

    public ThriftClient(Map<String, Object> topoConf, ThriftConnectionType type, String host, Integer port, Integer timeout,
                        String asUser) {
        //create a socket with server
        if (host == null) {
            throw new IllegalArgumentException("host is not set");
        }

        if (port == null) {
            port = type.getPort(topoConf);
        }

        if (timeout == null) {
            timeout = type.getSocketTimeOut(topoConf);
        }

        if (port <= 0 && !type.isFake()) {
            throw new IllegalArgumentException("invalid port: " + port);
        }

        this.host = host;
        this.port = port;
        this.timeout = timeout;
        conf = topoConf;
        this.type = type;
        this.asUser = asUser;
        if (!type.isFake()) {
            reconnect();
        }
    }

    public synchronized TTransport transport() {
        return transport;
    }

    public synchronized void reconnect() {
        close();
        TSocket socket = null;
        try {
            socket = new TSocket(host, port);
            if (timeout != null) {
                socket.setTimeout(timeout);
            }

            //construct a transport plugin
            ITransportPlugin transportPlugin = ClientAuthUtils.getTransportPlugin(type, conf);

            //TODO get this from type instead of hardcoding to Nimbus.
            //establish client-server transport via plugin
            //do retries if the connect fails
            TBackoffConnect connectionRetry
                = new TBackoffConnect(
                ObjectReader.getInt(conf.get(Config.STORM_NIMBUS_RETRY_TIMES)),
                ObjectReader.getInt(conf.get(Config.STORM_NIMBUS_RETRY_INTERVAL)),
                ObjectReader.getInt(conf.get(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING)),
                    retryForever);
            transport = connectionRetry.doConnectWithRetry(transportPlugin, socket, host, asUser);
        } catch (Exception ex) {
            // close the socket, which releases connection if it has created any.
            if (socket != null) {
                try {
                    socket.close();
                } catch (Exception e) {
                    //ignore
                }
            }
            throw new RuntimeException(ex);
        }
        protocol = null;
        if (transport != null) {
            protocol = new TBinaryProtocol(transport);
        }
    }

    @Override
    public synchronized void close() {
        if (transport != null) {
            transport.close();
            transport = null;
            protocol = null;
        }
    }
}
