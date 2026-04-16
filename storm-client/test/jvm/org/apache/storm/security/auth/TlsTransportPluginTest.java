/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.security.auth;


import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import org.apache.storm.Config;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.security.auth.tls.ReloadableTsslTransportFactory;
import org.apache.storm.security.auth.tls.TlsTransportPlugin;
import org.apache.storm.thrift.transport.TServerSocket;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.mock;

class TlsTransportPluginTest {
    static private final Map<String, Object> conf = new HashMap<>();
    Nimbus.Iface handler;
    private TlsTransportPlugin tlsTransportPlugin;
    private final ThriftConnectionType type = ThriftConnectionType.NIMBUS_TLS;
    private final String testDataPath = System.getProperty("test.data.dir", "test/resources/ssl/");

    @BeforeEach
    void setUp() {
        tlsTransportPlugin = new TlsTransportPlugin();
        conf.clear();
        conf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, TlsTransportPlugin.class.getName());
        handler = mock(Nimbus.Iface.class);
    }
    @Test
    void testNonTlsConnection() {
        assertThrows(IllegalArgumentException.class, () -> {
            tlsTransportPlugin.prepare(type, conf);
            tlsTransportPlugin.getServer(new Nimbus.Processor<>(handler));
        });
    }

    @Test
    void testValidTlsSetup() {
        conf.put(Config.NIMBUS_THRIFT_TLS_PORT, "1111");
        conf.put(Config.STORM_THRIFT_TLS_SOCKET_TIMEOUT_MS, 60);
        conf.put(Config.NIMBUS_THRIFT_TLS_SERVER_KEYSTORE_PATH, testDataPath + "testKeyStore.jks");
        conf.put(Config.NIMBUS_THRIFT_TLS_SERVER_KEYSTORE_PASSWORD, "testpass");
        conf.put(Config.NIMBUS_THRIFT_TLS_SERVER_TRUSTSTORE_PATH, testDataPath + "testTrustStore.jks");
        conf.put(Config.NIMBUS_THRIFT_TLS_SERVER_TRUSTSTORE_PASSWORD, "testpass");
        conf.put(Config.NIMBUS_THRIFT_TLS_THREADS, 1);
        conf.put(Config.NIMBUS_QUEUE_SIZE, 1);

        try {
            tlsTransportPlugin.prepare(type, conf);
            tlsTransportPlugin.getServer(new Nimbus.Processor<>(handler));
            assertEquals(1111, tlsTransportPlugin.getPort());
        } catch (Exception e) {
            fail("TLS setup failed: " + e.getMessage());
        }
    }

    /**
     * Regression test for VULN-14. With {@code nimbus.thrift.tls.client.auth.required=true} the
     * factory must leave the SSLServerSocket in true "need client auth" mode, so that JSSE fails
     * the handshake closed when the client presents no certificate. The {@code CN=ANONYMOUS}
     * fallback branch in {@link TlsTransportPlugin} must therefore be unreachable.
     */
    @Test
    void testClientAuthRequiredRejectsUnauthenticatedClient() throws Exception {
        conf.put(Config.NIMBUS_THRIFT_TLS_PORT, 0);
        conf.put(Config.STORM_THRIFT_TLS_SOCKET_TIMEOUT_MS, 5000);
        conf.put(Config.NIMBUS_THRIFT_TLS_SERVER_KEYSTORE_PATH, testDataPath + "testKeyStore.jks");
        conf.put(Config.NIMBUS_THRIFT_TLS_SERVER_KEYSTORE_PASSWORD, "testpass");
        conf.put(Config.NIMBUS_THRIFT_TLS_SERVER_TRUSTSTORE_PATH, testDataPath + "testTrustStore.jks");
        conf.put(Config.NIMBUS_THRIFT_TLS_SERVER_TRUSTSTORE_PASSWORD, "testpass");
        conf.put(Config.NIMBUS_THRIFT_TLS_CLIENT_AUTH_REQUIRED, true);

        TServerSocket serverTransport = ReloadableTsslTransportFactory.getServerSocket(
                0, 5000, InetAddress.getLoopbackAddress(), type, conf);
        SSLServerSocket serverSocket = (SSLServerSocket) serverTransport.getServerSocket();
        try {
            assertTrue(serverSocket.getNeedClientAuth(),
                    "Factory must leave needClientAuth=true so JSSE fails the handshake closed");

            int port = serverSocket.getLocalPort();
            AtomicReference<Throwable> serverError = new AtomicReference<>();
            Thread acceptor = new Thread(() -> {
                try (SSLSocket accepted = (SSLSocket) serverSocket.accept()) {
                    accepted.setSoTimeout(5000);
                    accepted.startHandshake();
                } catch (Throwable t) {
                    serverError.set(t);
                }
            }, "tls-test-acceptor");
            acceptor.setDaemon(true);
            acceptor.start();

            // Trust-only client context: no client keystore, so the client cannot present a cert.
            KeyStore ts = KeyStore.getInstance("JKS");
            try (InputStream in = Files.newInputStream(Paths.get(testDataPath + "testTrustStore.jks"))) {
                ts.load(in, "testpass".toCharArray());
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);
            SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
            sslContext.init(null, tmf.getTrustManagers(), new SecureRandom());

            try (SSLSocket client = (SSLSocket) sslContext.getSocketFactory()
                    .createSocket(InetAddress.getLoopbackAddress(), port)) {
                client.setSoTimeout(5000);
                // Depending on timing, JSSE surfaces the server-side need-client-auth rejection
                // as SSLException or as SocketException ("Connection reset") — both prove the
                // handshake failed closed.
                assertThrows(IOException.class, client::startHandshake);
            }

            acceptor.join(5000);
            assertTrue(serverError.get() instanceof IOException,
                    "Server-side handshake must fail; got: " + serverError.get());
        } finally {
            serverTransport.close();
        }
    }
}