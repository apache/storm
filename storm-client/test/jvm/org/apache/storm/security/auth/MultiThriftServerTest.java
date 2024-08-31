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

import java.util.*;
import org.apache.storm.*;
import org.apache.storm.generated.Nimbus;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class MultiThriftServerTest {

    static private final Map<String, Object> conf = new HashMap<>();
    static ThriftServer serverNonTls;
    static ThriftServer serverTls;
    static private MultiThriftServer<ThriftServer> multiThriftServer;

    @BeforeAll
    public static void setUp() {
        conf.clear();
        multiThriftServer = new MultiThriftServer<>("TestServer");
        Nimbus.Iface handler;
        handler = mock(Nimbus.Iface.class);
        conf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, TestingITransportPlugin.class.getName());
        serverNonTls = new ThriftServer(conf,
                new Nimbus.Processor<>(handler),
                ThriftConnectionType.NIMBUS);
        serverTls = new ThriftServer(conf,
                new Nimbus.Processor<>(handler),
                ThriftConnectionType.NIMBUS_TLS);
    }
    @Test
    public void testAddThriftServer() {
        multiThriftServer.add(serverNonTls);
        assertEquals(serverNonTls, multiThriftServer.get(ThriftConnectionType.NIMBUS));
    }
    @Test
    public void testAddThriftServerTls() {
        multiThriftServer.add(serverTls);
        assertEquals(serverTls, multiThriftServer.get(ThriftConnectionType.NIMBUS_TLS));
    }
    @Test
    public void testAddThriftServerBoth() {
        multiThriftServer.add(serverTls);
        multiThriftServer.add(serverNonTls);
        assertEquals(serverNonTls, multiThriftServer.get(ThriftConnectionType.NIMBUS));
        assertEquals(serverTls, multiThriftServer.get(ThriftConnectionType.NIMBUS_TLS));
    }
}
