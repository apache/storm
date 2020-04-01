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

import java.io.IOException;
import java.util.Map;
import org.apache.storm.security.auth.sasl.SaslTransportPlugin;
import org.apache.storm.thrift.TProcessor;
import org.apache.storm.thrift.server.TServer;
import org.apache.storm.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftServer {
    private static final Logger LOG = LoggerFactory.getLogger(ThriftServer.class);
    protected final TProcessor processor;
    private final Map<String, Object> conf; //storm configuration
    private final ThriftConnectionType type;
    private TServer server;
    private int port;
    private boolean areWorkerTokensSupported;
    private ITransportPlugin transportPlugin;

    public ThriftServer(Map<String, Object> conf, TProcessor processor, ThriftConnectionType type) {
        this.conf = conf;
        this.processor = processor;
        this.type = type;

        try {
            //locate our thrift transport plugin
            transportPlugin = ClientAuthUtils.getTransportPlugin(this.type, this.conf);
            //server
            server = transportPlugin.getServer(this.processor);
            port = transportPlugin.getPort();
            areWorkerTokensSupported = transportPlugin.areWorkerTokensSupported();
        } catch (IOException | TTransportException ex) {
            handleServerException(ex);
        }

    }

    public void stop() {
        server.stop();
        if (transportPlugin instanceof SaslTransportPlugin) {
            ((SaslTransportPlugin) transportPlugin).close();
        }
    }

    /**
     * Check whether serving.
     * @return true if ThriftServer is listening to requests?
     */
    public boolean isServing() {
        return server.isServing();
    }

    public void serve() {
        try {
            //start accepting requests
            server.serve();
        } catch (Exception ex) {
            handleServerException(ex);
        }
    }

    private void handleServerException(Exception ex) {
        LOG.error("ThriftServer is being stopped due to: " + ex, ex);
        if (server != null) {
            server.stop();
        }
        Runtime.getRuntime().halt(1); //shutdown server process since we could not handle Thrift requests any more
    }

    /**
     * Get port.
     * @return The port this server is/will be listening on
     */
    public int getPort() {
        return port;
    }

    /**
     * Check if worker tokens are supported by this thrift server.
     *
     * @return true if they are else false.
     */
    public boolean supportsWorkerTokens() {
        return areWorkerTokensSupported;
    }
}
