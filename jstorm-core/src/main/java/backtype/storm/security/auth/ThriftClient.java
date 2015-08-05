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
package backtype.storm.security.auth;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.Configuration;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.cluster.Cluster;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

public class ThriftClient {
    private static final Logger LOG = LoggerFactory.getLogger(ThriftClient.class);
    private TTransport _transport;
    protected TProtocol _protocol;
    private String hostPort;
    private String host;
    private Integer port;
    
    private Map<Object, Object> conf;
    
    private Integer timeout;
    private ThriftConnectionType type;
    private String asUser;
    
    public ThriftClient(Map conf, ThriftConnectionType type) throws Exception {
        this(conf, type, null, null, null, null);
    }
    
    @SuppressWarnings("unchecked")
    public ThriftClient(Map conf, ThriftConnectionType type, Integer timeout) throws Exception {
        this(conf, type, null, null, timeout, null);
    }

    /**
     * This is only for be compatible for Storm
     * @param conf
     * @param type
     * @param host
     */
    public ThriftClient(Map conf, ThriftConnectionType type, String host) {
        this(conf, type, host, null, null, null);
    }

    public ThriftClient(Map conf, ThriftConnectionType type, String host, Integer port, Integer timeout){
        this(conf, type, host, port, timeout, null);
    }

    public ThriftClient(Map conf, ThriftConnectionType type, String host, Integer port, Integer timeout, String asUser) {
        //create a socket with server
        
        this.timeout = timeout;
        this.conf = conf;
        this.type = type;
        this.asUser = asUser;
        
        getMaster(conf, host, port);
        reconnect();
    }
    
    
    
    public static String getMasterByZk(Map conf) throws Exception {

        
        CuratorFramework zkobj = null;
        String masterHost = null;
        
        try {
            String root = String.valueOf(conf.get(Config.STORM_ZOOKEEPER_ROOT));
            String zkMasterDir = root + Cluster.MASTER_SUBTREE;
            
            zkobj = Utils.newCurator(conf, 
                            (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS), 
                            conf.get(Config.STORM_ZOOKEEPER_PORT), 
                            zkMasterDir);
            zkobj.start();
            if (zkobj.checkExists().forPath("/") == null) {
                throw new RuntimeException("No alive nimbus ");
            }
            
            masterHost = new String(zkobj.getData().forPath("/"));
            
            LOG.info("masterHost:" + masterHost);
            return masterHost;
        } finally {
            if (zkobj != null) {
                zkobj.close();
                zkobj = null;
            }
        }
    }
    
    public void getMaster(Map conf, String host, Integer port){
        if (StringUtils.isBlank(host) == false) {
            this.host = host;
            if (port == null) {
                port = type.getPort(conf);
            }
            this.port = port;
            this.hostPort = host + ":" + port;
        }else {
            try {
                hostPort = ThriftClient.getMasterByZk(conf);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                throw new RuntimeException("Failed to get master from ZK.", e);
            }
            String[] host_port = hostPort.split(":");
            if (host_port.length != 2) {
                throw new InvalidParameterException("Host format error: " + hostPort);
            }
            this.host = host_port[0];
            this.port = Integer.parseInt(host_port[1]);
        }
        
        // create a socket with server
        if (this.host == null) {
            throw new IllegalArgumentException("host is not set");
        }
        if (this.port == null || this.port <= 0) {
            throw new IllegalArgumentException("invalid port: " + port);
        }
    }
    
    public synchronized TTransport transport() {
        return _transport;
    }
    
    public synchronized void reconnect() {
        close();    
        try {
            TSocket socket = new TSocket(host, port);
            if(timeout!=null) {
                socket.setTimeout(timeout);
            }else {
                //@@@ Todo
                // set the socket default Timeout as xxxx
            }

            //locate login configuration 
            Configuration login_conf = AuthUtils.GetConfiguration(conf);

            //construct a transport plugin
            ITransportPlugin transportPlugin = AuthUtils.GetTransportPlugin(type, conf, login_conf);

            final TTransport underlyingTransport = socket;

            //TODO get this from type instead of hardcoding to Nimbus.
            //establish client-server transport via plugin
            //do retries if the connect fails
            TBackoffConnect connectionRetry 
                = new TBackoffConnect(
                                      Utils.getInt(conf.get(Config.STORM_NIMBUS_RETRY_TIMES)),
                                      Utils.getInt(conf.get(Config.STORM_NIMBUS_RETRY_INTERVAL)),
                                      Utils.getInt(conf.get(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING)));
            _transport = connectionRetry.doConnectWithRetry(transportPlugin, underlyingTransport, host, asUser);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        _protocol = null;
        if (_transport != null) {
            _protocol = new  TBinaryProtocol(_transport);
        }
    }

    public synchronized void close() {
        if (_transport != null) {
            _transport.close();
            _transport = null;
            _protocol = null;
        }
    }

}
