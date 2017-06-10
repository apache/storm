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
import java.security.Principal;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.HashSet;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.security.auth.login.Configuration;
import javax.security.auth.Subject;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple transport for Thrift plugin.
 * 
 * This plugin is designed to be backward compatible with existing Storm code.
 */
public class SimpleTransportPlugin implements ITransportPlugin {
    protected ThriftConnectionType type;
    protected Map<String, Object> topoConf;
    protected Configuration login_conf;
    private static final Logger LOG = LoggerFactory.getLogger(SimpleTransportPlugin.class);
    private int port;

    @Override
    public void prepare(ThriftConnectionType type, Map<String, Object> topoConf, Configuration login_conf) {
        this.type = type;
        this.topoConf = topoConf;
        this.login_conf = login_conf;
    }

    @Override
    public TServer getServer(TProcessor processor) throws IOException, TTransportException {
        int configuredPort = type.getPort(topoConf);
        TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(configuredPort);
        this.port = serverTransport.getPort();
        int numWorkerThreads = type.getNumThreads(topoConf);
        int maxBufferSize = type.getMaxBufferSize(topoConf);
        Integer queueSize = type.getQueueSize(topoConf);

        THsHaServer.Args server_args = new THsHaServer.Args(serverTransport).
                processor(new SimpleWrapProcessor(processor)).
                maxWorkerThreads(numWorkerThreads).
                protocolFactory(new TBinaryProtocol.Factory(false, true, maxBufferSize, -1));

        server_args.maxReadBufferBytes = maxBufferSize;

        if (queueSize != null) {
            server_args.executorService(new ThreadPoolExecutor(numWorkerThreads, numWorkerThreads, 
                                   60, TimeUnit.SECONDS, new ArrayBlockingQueue(queueSize)));
        }

        //construct THsHaServer
        return new THsHaServer(server_args);
    }

    /**
     * Connect to the specified server via framed transport 
     * @param transport The underlying Thrift transport.
     * @param serverHost unused.
     * @param asUser unused.
     */
    @Override
    public TTransport connect(TTransport transport, String serverHost, String asUser) throws TTransportException {
        int maxBufferSize = type.getMaxBufferSize(topoConf);
        //create a framed transport
        TTransport conn = new TFramedTransport(transport, maxBufferSize);

        //connect
        conn.open();
        LOG.debug("Simple client transport has been established");

        return conn;
    }

    /**
     * @return the subject that will be used for all connections
     */  
    protected Subject getDefaultSubject() {
        return null;
    }

    @Override
    public int getPort() {
        return port;
    }

    /**                                                                                                                                                                             
     * Processor that populate simple transport info into ReqContext, and then invoke a service handler                                                                              
     */
    private class SimpleWrapProcessor implements TProcessor {
        final TProcessor wrapped;

        SimpleWrapProcessor(TProcessor wrapped) {
            this.wrapped = wrapped;
        }

        public boolean process(final TProtocol inProt, final TProtocol outProt) throws TException {
            //populating request context 
            ReqContext req_context = ReqContext.context();

            TTransport trans = inProt.getTransport();
            if (trans instanceof TMemoryInputTransport) {
                try {
                    req_context.setRemoteAddress(InetAddress.getLocalHost());
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }                                
            } else if (trans instanceof TSocket) {
                TSocket tsocket = (TSocket)trans;
                //remote address
                Socket socket = tsocket.getSocket();
                req_context.setRemoteAddress(socket.getInetAddress());                
            } 

            //anonymous user
            Subject s = getDefaultSubject();
            if (s == null) {
              final String user = (String)topoConf.get("debug.simple.transport.user");
              if (user != null) {
                HashSet<Principal> principals = new HashSet<>();
                principals.add(new Principal() {
                  public String getName() { return user; }
                  public String toString() { return user; }
                });
                s = new Subject(true, principals, new HashSet<>(), new HashSet<>());
              }
            }
            req_context.setSubject(s);

            //invoke service handler
            return wrapped.process(inProt, outProt);
        }
    } 
}
