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

package org.apache.storm.security.auth.tls;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.security.Principal;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSocket;
import javax.security.auth.Subject;
import javax.security.cert.X509Certificate;
import org.apache.storm.security.auth.ITransportPlugin;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.SingleUserPrincipal;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.TProcessor;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.protocol.TProtocol;
import org.apache.storm.thrift.server.TServer;
import org.apache.storm.thrift.server.TThreadPoolServer;
import org.apache.storm.thrift.transport.TSSLTransportFactory;
import org.apache.storm.thrift.transport.TServerSocket;
import org.apache.storm.thrift.transport.TSocket;
import org.apache.storm.thrift.transport.TTransport;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.ExtendedThreadPoolExecutor;
import org.apache.storm.utils.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TlsTransportPlugin implements ITransportPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(TlsTransportPlugin.class);
    protected ThriftConnectionType type;
    protected Map<String, Object> conf;
    private int port;
    private static TServerSocket serverTransport;
    private static TThreadPoolServer tThreadPoolServer;

    @Override
    public void prepare(ThriftConnectionType type, Map<String, Object> conf) {
        this.type = type;
        this.conf = conf;
    }

    @Override
    public TServer getServer(TProcessor processor) throws IOException, TTransportException {

        if (!type.isTlsEnabled()) {
            throw new UnsupportedEncodingException("Non-TLS connection is not supported");
        }

        int configuredPort = type.getPort(conf);
        Integer socketTimeout = type.getSocketTimeOut(conf);

        TSSLTransportFactory.TSSLTransportParameters params = new TSSLTransportFactory.TSSLTransportParameters();
        if (type.getServerKeyStorePath(conf) != null && type.getServerKeyStorePassword(conf) != null) {
            params.setKeyStore(type.getServerKeyStorePath(conf), type.getServerKeyStorePassword(conf), null,
                    SecurityUtils.inferKeyStoreTypeFromPath(type.getServerKeyStorePath(conf)));
        } else {
            throw new IllegalArgumentException("The server keystore is not configured properly");
        }

        if (type.isClientAuthRequired(conf)) {
            if (type.getServerTrustStorePath(conf) != null && type.getServerTrustStorePassword(conf) != null) {
                params.setTrustStore(type.getServerTrustStorePath(conf), type.getServerTrustStorePassword(conf), null,
                        SecurityUtils.inferKeyStoreTypeFromPath(type.getServerTrustStorePath(conf)));
                params.requireClientAuth(true);
            } else {
                throw new IllegalArgumentException("The server truststore is not configured properly");
            }
        }

        int clientTimeout = (socketTimeout == null ? 0 : socketTimeout);

        TServerSocket serverTransport = null;
        try {
            serverTransport = ReloadableTsslTransportFactory.getServerSocket(configuredPort, clientTimeout,
                    InetAddress.getLocalHost(), type, conf);
        } catch (Exception e) {
            throw new IOException(e);
        }

        ServerSocket socket = serverTransport.getServerSocket();
        socket.setReuseAddress(true);
        this.port = socket.getLocalPort();
        
        int numWorkerThreads = type.getNumThreads(conf);
        Integer queueSize = type.getQueueSize(conf);

        TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport)
                .processor(new TTlsWrapProcessor(processor))
                .minWorkerThreads(numWorkerThreads)
                .maxWorkerThreads(numWorkerThreads)
                .protocolFactory(new TBinaryProtocol.Factory(false, true));

        BlockingQueue<Runnable> workQueue = new SynchronousQueue<>();
        if (queueSize != null) {
            workQueue = new ArrayBlockingQueue<>(queueSize);
        }
        ThreadPoolExecutor executorService = new ExtendedThreadPoolExecutor(numWorkerThreads, numWorkerThreads,
                60, TimeUnit.SECONDS, workQueue);
        serverArgs.executorService(executorService);
        tThreadPoolServer = new TThreadPoolServer(serverArgs);
        return tThreadPoolServer;
    }

    @Override
    public TTransport connect(TTransport transport, String serverHost, String asUser) throws IOException, TTransportException {
        return transport;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public boolean areWorkerTokensSupported() {
        return false;
    }

    private static class TTlsWrapProcessor implements TProcessor {
        final TProcessor wrapped;

        TTlsWrapProcessor(TProcessor wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public void process(final TProtocol inProt, final TProtocol outProt) throws TException {

            TTransport trans = inProt.getTransport();
            TSocket tsocket = (TSocket) trans;
            SSLSocket socket = (SSLSocket) tsocket.getSocket();

            String principalName = "CN=ANONYMOUS";
            try {
                for (X509Certificate cert: socket.getSession().getPeerCertificateChain()) {
                    Principal principal = cert.getSubjectDN();
                    principalName = principal.getName();
                    break;
                }
            } catch (SSLPeerUnverifiedException e) {
                LOG.debug("Client cert is not verified. Set principalName={}.", principalName, e);
            }
            LOG.debug("principalName : {} ", principalName);
            ReqContext reqContext = ReqContext.context();

            //remote address
            reqContext.setRemoteAddress(socket.getInetAddress());

            //remote subject
            Subject remoteUser = new Subject();
            remoteUser.getPrincipals().add(new SingleUserPrincipal(principalName));
            reqContext.setSubject(remoteUser);

            wrapped.process(inProt, outProt);
        }
    }
}
