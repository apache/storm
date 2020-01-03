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
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.Principal;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.TProcessor;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.protocol.TProtocol;
import org.apache.storm.thrift.server.THsHaServer;
import org.apache.storm.thrift.server.TServer;
import org.apache.storm.thrift.transport.TFramedTransport;
import org.apache.storm.thrift.transport.TMemoryInputTransport;
import org.apache.storm.thrift.transport.TNonblockingServerSocket;
import org.apache.storm.thrift.transport.TSocket;
import org.apache.storm.thrift.transport.TTransport;
import org.apache.storm.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple transport for Thrift plugin.
 *
 * <p>This plugin is designed to be backward compatible with existing Storm code.
 */
public class SimpleTransportPlugin implements ITransportPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleTransportPlugin.class);
    protected ThriftConnectionType type;
    protected Map<String, Object> topoConf;
    private int port;

    @Override
    public void prepare(ThriftConnectionType type, Map<String, Object> topoConf) {
        this.type = type;
        this.topoConf = topoConf;
    }

    @Override
    public TServer getServer(TProcessor processor) throws IOException, TTransportException {
        int configuredPort = type.getPort(topoConf);
        TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(configuredPort);
        this.port = serverTransport.getPort();
        int numWorkerThreads = type.getNumThreads(topoConf);
        int maxBufferSize = type.getMaxBufferSize(topoConf);
        Integer queueSize = type.getQueueSize(topoConf);

        THsHaServer.Args serverArgs = new THsHaServer.Args(serverTransport)
                .processor(new SimpleWrapProcessor(processor))
                .maxWorkerThreads(numWorkerThreads)
                .protocolFactory(new TBinaryProtocol.Factory(false,
                        true,
                        maxBufferSize,
                        -1));

        serverArgs.maxReadBufferBytes = maxBufferSize;

        if (queueSize != null) {
            serverArgs.executorService(new ThreadPoolExecutor(numWorkerThreads, numWorkerThreads,
                                                               60, TimeUnit.SECONDS, new ArrayBlockingQueue(queueSize)));
        }

        //construct THsHaServer
        return new THsHaServer(serverArgs);
    }

    /**
     * Connect to the specified server via framed transport.
     *
     * @param transport  The underlying Thrift transport
     * @param serverHost unused
     * @param asUser     unused
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
     * Get default subject.
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
     * Processor that populate simple transport info into ReqContext, and then invoke a service handler.
     */
    private class SimpleWrapProcessor implements TProcessor {
        final TProcessor wrapped;

        SimpleWrapProcessor(TProcessor wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public void process(final TProtocol inProt, final TProtocol outProt) throws TException {
            //populating request context 
            ReqContext reqContext = ReqContext.context();

            TTransport trans = inProt.getTransport();
            if (trans instanceof TMemoryInputTransport) {
                try {
                    reqContext.setRemoteAddress(InetAddress.getLocalHost());
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            } else if (trans instanceof TSocket) {
                TSocket tsocket = (TSocket) trans;
                //remote address
                Socket socket = tsocket.getSocket();
                reqContext.setRemoteAddress(socket.getInetAddress());
            }

            //anonymous user
            Subject s = getDefaultSubject();
            if (s == null) {
                final String user = (String) topoConf.get("debug.simple.transport.user");
                if (user != null) {
                    HashSet<Principal> principals = new HashSet<>();
                    principals.add(new Principal() {
                        @Override
                        public String getName() {
                            return user;
                        }

                        @Override
                        public String toString() {
                            return user;
                        }
                    });
                    s = new Subject(true, principals, new HashSet<>(), new HashSet<>());
                }
            }
            reqContext.setSubject(s);

            //invoke service handler
            wrapped.process(inProt, outProt);
        }
    }
}
