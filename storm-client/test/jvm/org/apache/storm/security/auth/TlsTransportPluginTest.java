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


import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.security.auth.tls.TlsTransportPlugin;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
}