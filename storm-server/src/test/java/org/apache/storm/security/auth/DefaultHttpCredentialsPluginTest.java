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

import java.security.Principal;
import java.util.HashMap;
import java.util.HashSet;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import org.apache.storm.shade.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultHttpCredentialsPluginTest {

    @Test
    public void test_getUserName() {
        DefaultHttpCredentialsPlugin handler = new DefaultHttpCredentialsPlugin();
        handler.prepare(new HashMap<>());

        assertNull(handler.getUserName(null), "Should return null when request is null");

        assertNull(handler.getUserName(Mockito.mock(HttpServletRequest.class)), "Should return null when user principal is null");

        HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
        Mockito.when(mockRequest.getUserPrincipal()).thenReturn(new SingleUserPrincipal(""));
        assertNull(handler.getUserName(mockRequest), "Should return null when user is blank");

        String expName = "Alice";
        mockRequest = Mockito.mock(HttpServletRequest.class);
        Mockito.when(mockRequest.getUserPrincipal()).thenReturn(new SingleUserPrincipal(expName));
        assertEquals(expName, handler.getUserName(mockRequest), "Should return correct user from requests principal");

        try {
            String doAsUserName = "Bob";
            mockRequest = Mockito.mock(HttpServletRequest.class);
            Mockito.when(mockRequest.getUserPrincipal()).thenReturn(new SingleUserPrincipal(expName));
            Mockito.when(mockRequest.getHeader("doAsUser")).thenReturn(doAsUserName);
            ReqContext context = handler.populateContext(ReqContext.context(), mockRequest);

            assertTrue(context.isImpersonating());
            assertEquals(expName, context.realPrincipal().getName());
            assertEquals(doAsUserName, context.principal().getName());
        } finally {
            ReqContext.reset();
        }
    }

    @Test
    public void test_populate_req_context_on_null_user() {
        try {
            DefaultHttpCredentialsPlugin handler = new DefaultHttpCredentialsPlugin();
            handler.prepare(new HashMap<>());
            Subject subject =
                new Subject(false, ImmutableSet.<Principal>of(new SingleUserPrincipal("test")), new HashSet<>(), new HashSet<>());
            ReqContext context = new ReqContext(subject);


            assertEquals(0, handler
                .populateContext(context, Mockito.mock(HttpServletRequest.class))
                .subject()
                .getPrincipals()
                .size()

            );
        } finally {
            ReqContext.reset();
        }

    }
}
