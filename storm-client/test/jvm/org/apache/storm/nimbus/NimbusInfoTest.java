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

package org.apache.storm.nimbus;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NimbusInfoTest {

    @Test
    void parseOnePort() {
        NimbusInfo nimbusInfo = NimbusInfo.parse("nimbus.com:6627");

        assertEquals(6627, nimbusInfo.getPort());
        assertEquals("nimbus.com", nimbusInfo.getHost());
        assertEquals(0, nimbusInfo.getTlsPort());
    }

    @Test
    void parseTwoPort() {
        NimbusInfo nimbusInfo = NimbusInfo.parse("nimbus.com:6627:6628");

        assertEquals(6627, nimbusInfo.getPort());
        assertEquals("nimbus.com", nimbusInfo.getHost());
        assertEquals(6628, nimbusInfo.getTlsPort());
    }

    @Test
    void parseInvalidTlsPort() {

        Exception exception = assertThrows(RuntimeException.class, () -> {
            NimbusInfo.parse("nimbus.com:6627:abc");
        });

        String expectedMessage = "nimbusInfo should have format of host:port:tlsPort or host:port";
        String actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));

        exception = assertThrows(RuntimeException.class, () -> {
            NimbusInfo.parse("nimbus.com:6627:");
        });
        actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));
    }
}