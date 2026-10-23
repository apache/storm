/*
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

package org.apache.storm.daemon.drpc.webapp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletRequest;

import java.net.InetAddress;

import org.apache.storm.security.auth.ReqContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class ReqContextFilterTest {

    @AfterEach
    public void resetContext() {
        ReqContext.context().setRemoteAddress(null);
    }

    private static InetAddress remoteAddressFor(String remoteAddr) {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn(remoteAddr);
        new ReqContextFilter(null).populateContext(request);
        return ReqContext.context().remoteAddress();
    }

    @Test
    public void testIpAddressLiteralsAreUsed() {
        assertEquals(InetAddress.ofLiteral("192.0.2.1"), remoteAddressFor("192.0.2.1"));
        assertEquals(InetAddress.ofLiteral("::1"), remoteAddressFor("::1"));
    }

    @Test
    public void testHostNamesAreNotResolved() {
        //A host name is not looked up, the remote address is unknown instead
        assertNull(remoteAddressFor("localhost"));
        assertNull(remoteAddressFor(""));
        assertNull(remoteAddressFor(null));
    }
}
