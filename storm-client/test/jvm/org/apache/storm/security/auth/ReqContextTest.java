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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Principal;
import java.util.HashSet;
import java.util.Set;
import javax.security.auth.Subject;
import org.apache.storm.shade.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ReqContextTest {

    private ReqContext rc;

    @BeforeEach
    public void setup() {
        rc = ReqContext.context();
    }

    @Test
    public void testSubject() {
        Subject expected = new Subject();
        assertFalse(expected.isReadOnly());
        rc.setSubject(expected);
        assertEquals(expected, rc.subject());

        expected.setReadOnly();
        rc.setSubject(expected);
        assertEquals(expected, rc.subject());
    }

    @Test
    public void testRemoteAddress() throws UnknownHostException {
        InetAddress expected = InetAddress.getByAddress("ABCD".getBytes());
        rc.setRemoteAddress(expected);
        assertEquals(expected, rc.remoteAddress());
    }

    /**
     * If subject has no principals, request context should return null principal
     */
    @Test
    public void testPrincipalReturnsNullWhenNoSubject() {
        rc.setSubject(new Subject());
        assertNull(rc.principal());
    }

    @Test
    public void testPrincipal() {
        final String principalName = "Test Principal";
        Principal testPrincipal = () -> principalName;
        Set<Principal> principals = ImmutableSet.of(testPrincipal);
        Subject subject = new Subject(false, principals, new HashSet<>(), new HashSet<>());
        rc.setSubject(subject);
        assertNotNull(rc.principal());
        assertEquals(principalName, rc.principal().getName());
        rc.setSubject(null);
    }
}
