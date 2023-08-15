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

import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ThriftClientTest {

    private final int NIMBUS_TIMEOUT = 3 * 1000;
    private Map<String, Object> conf;

    @BeforeEach
    public void setup() {
        conf = Utils.readDefaultConfig();
        conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 0);
    }

    @Test
    public void testConstructorThrowsIfPortNegative() {
        assertThrows(IllegalArgumentException.class,
            () -> new ThriftClient(conf, ThriftConnectionType.DRPC, "bogushost", -1, NIMBUS_TIMEOUT));
    }

    @Test
    public void testConstructorThrowsIfPortZero() {
        assertThrows(IllegalArgumentException.class,
            () -> new ThriftClient(conf, ThriftConnectionType.DRPC, "bogushost", 0, NIMBUS_TIMEOUT));
    }

    @Test
    public void testConstructorThrowsIfHostNull() {
        assertThrows(IllegalArgumentException.class,
            () -> new ThriftClient(conf, ThriftConnectionType.DRPC, null, 4242, NIMBUS_TIMEOUT));
    }

    @Test
    public void testConstructorThrowsIfHostEmpty() {
        Exception e = assertThrows(RuntimeException.class,
            () -> new ThriftClient(conf, ThriftConnectionType.DRPC, "", 4242, NIMBUS_TIMEOUT));
        // Now the cause of the thrown exception must be TTransportException
        assertTrue(e.getCause().getCause() instanceof TTransportException, e.getCause().getMessage());
    }
}
