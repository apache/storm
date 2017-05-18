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
package org.apache.storm.security.auth;

import java.io.IOException;
import java.util.Map;

import javax.security.auth.login.Configuration;

import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftServer {
    private static final Logger LOG = LoggerFactory.getLogger(ThriftServer.class);
    private Map _topoConf; //storm configuration
    protected TProcessor _processor = null;
    private final ThriftConnectionType _type;
    private TServer _server;
    private Configuration _login_conf;
    private int _port;
    
    public ThriftServer(Map<String, Object> topoConf, TProcessor processor, ThriftConnectionType type) {
        _topoConf = topoConf;
        _processor = processor;
        _type = type;

        try {
            //retrieve authentication configuration 
            _login_conf = AuthUtils.GetConfiguration(_topoConf);
        } catch (Exception x) {
            LOG.error(x.getMessage(), x);
        }
        try {
            //locate our thrift transport plugin
            ITransportPlugin transportPlugin = AuthUtils.GetTransportPlugin(_type, _topoConf, _login_conf);
            //server
            _server = transportPlugin.getServer(_processor);
            _port = transportPlugin.getPort();
        } catch (IOException | TTransportException ex) {
            handleServerException(ex);
        }

    }

    public void stop() {
        _server.stop();
    }

    /**
     * @return true if ThriftServer is listening to requests?
     */
    public boolean isServing() {
        return _server.isServing();
    }
    
    public void serve()  {
        try {
            //start accepting requests
            _server.serve();
        } catch (Exception ex) {
            handleServerException(ex);
        }
    }
    
    private void handleServerException(Exception ex) {
        LOG.error("ThriftServer is being stopped due to: " + ex, ex);
        if (_server != null) {
            _server.stop();
        }
        Runtime.getRuntime().halt(1); //shutdown server process since we could not handle Thrift requests any more
    }
    
    /**
     * @return The port this server is/will be listening on
     */
    public int getPort() {
        return _port;
    }
}
