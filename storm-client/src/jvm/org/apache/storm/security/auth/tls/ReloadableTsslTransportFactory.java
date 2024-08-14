/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 *  file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the
 * Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
 * on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 *  governing permissions
 * and limitations under the License.
 */

package org.apache.storm.security.auth.tls;

import java.net.InetAddress;
import java.security.SecureRandom;
import java.util.Map;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.storm.thrift.transport.TSSLTransportFactory;
import org.apache.storm.thrift.transport.TServerSocket;
import org.apache.storm.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReloadableTsslTransportFactory extends TSSLTransportFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ReloadableTsslTransportFactory.class);

    public static TServerSocket getServerSocket(int port, int clientTimeout, InetAddress ifAddress,
                                                ThriftConnectionType type, Map<String, Object> conf) throws Exception {
        SSLContext ctx = createSslContext(type, conf);
        return createServerSocket(ctx.getServerSocketFactory(), port, clientTimeout, type.isClientAuthRequired(conf),
                ifAddress, type);
    }

    private static SSLContext createSslContext(ThriftConnectionType type, Map<String, Object> conf) throws Exception {
        X509TrustManager trustManager = new ReloadableX509TrustManager(type.getServerTrustStorePath(conf),
                type.getServerTrustStorePassword(conf));
        X509KeyManager keyManager = new ReloadableX509KeyManager(type.getServerKeyStorePath(conf),
                type.getServerKeyStorePassword(conf));
        SSLContext ctx = SSLContext.getInstance("TLSv1.2");

        ctx.init(new KeyManager[]{keyManager}, new TrustManager[]{trustManager}, new SecureRandom());
        return ctx;
    }

    private static TServerSocket createServerSocket(SSLServerSocketFactory factory, int port, int timeout,
                                                    boolean clientAuth, InetAddress ifAddress,
                                                    ThriftConnectionType type) throws TTransportException {
        try {
            SSLServerSocket serverSocket = (SSLServerSocket) factory.createServerSocket(port, 100, ifAddress);
            serverSocket.setEnabledProtocols(new String[]{"TLSv1.2"});

            serverSocket.setSoTimeout(timeout);
            serverSocket.setNeedClientAuth(clientAuth);
            serverSocket.setWantClientAuth(clientAuth);
            return new TServerSocket(new TServerSocket.ServerSocketTransportArgs().serverSocket(serverSocket).clientTimeout(timeout));
        } catch (Exception e) {
            throw new TTransportException("Could not bind to port " + port, e);
        }
    }
}
