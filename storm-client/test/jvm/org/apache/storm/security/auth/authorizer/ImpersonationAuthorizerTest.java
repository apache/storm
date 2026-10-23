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

package org.apache.storm.security.auth.authorizer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.security.auth.Subject;
import org.apache.storm.Config;
import org.apache.storm.security.auth.DefaultPrincipalToLocal;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.SingleUserPrincipal;
import org.junit.jupiter.api.Test;

public class ImpersonationAuthorizerTest {

    private static ImpersonationAuthorizer mkAuthorizer(String impersonatingUser, List<String> hosts) {
        Map<String, List<String>> entry = new HashMap<>();
        entry.put("groups", Collections.singletonList("*"));
        entry.put("hosts", hosts);
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.NIMBUS_IMPERSONATION_ACL, Collections.singletonMap(impersonatingUser, entry));
        conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, DefaultPrincipalToLocal.class.getName());
        ImpersonationAuthorizer authorizer = new ImpersonationAuthorizer();
        authorizer.prepare(conf);
        return authorizer;
    }

    private static ReqContext mkContext(String realUser, String user, InetAddress remoteAddress) {
        Subject subject = new Subject();
        subject.getPrincipals().add(new SingleUserPrincipal(user));
        ReqContext context = new ReqContext(subject);
        context.setRealPrincipal(new SingleUserPrincipal(realUser));
        context.setRemoteAddress(remoteAddress);
        return context;
    }

    @Test
    public void testRealPrincipalWithoutNameIsDenied() {
        ImpersonationAuthorizer authorizer = mkAuthorizer("bob", Collections.singletonList("*"));
        ReqContext context = mkContext(null, "alice", InetAddress.getLoopbackAddress());
        assertTrue(context.isImpersonating());
        assertFalse(authorizer.permit(context, "execute", null));
    }

    @Test
    public void testRealPrincipalWithoutNameIsDeniedWithNullAclEntry() {
        ImpersonationAuthorizer authorizer = mkAuthorizer(null, Collections.singletonList("*"));
        ReqContext context = mkContext(null, "alice", InetAddress.getLoopbackAddress());
        assertFalse(authorizer.permit(context, "execute", null));
    }

    @Test
    public void testRealPrincipalWithEmptyNameIsDenied() {
        ImpersonationAuthorizer authorizer = mkAuthorizer("", Collections.singletonList("*"));
        ReqContext context = mkContext("", "alice", InetAddress.getLoopbackAddress());
        assertTrue(context.isImpersonating());
        assertFalse(authorizer.permit(context, "execute", null));
    }

    @Test
    public void testHostListedByAddressSkipsNameLookup() {
        ImpersonationAuthorizer authorizer = mkAuthorizer("bob", Collections.singletonList("192.0.2.10"));
        InetAddress remoteAddress = mock(InetAddress.class);
        when(remoteAddress.getHostAddress()).thenReturn("192.0.2.10");
        assertTrue(authorizer.permit(mkContext("bob", "alice", remoteAddress), "execute", null));
        verify(remoteAddress, never()).getCanonicalHostName();
        verify(remoteAddress, never()).getHostName();
    }

    @Test
    public void testHostListedByName() {
        ImpersonationAuthorizer authorizer = mkAuthorizer("bob", Collections.singletonList("client.example.com"));
        InetAddress remoteAddress = mock(InetAddress.class);
        when(remoteAddress.getHostAddress()).thenReturn("192.0.2.10");
        when(remoteAddress.getCanonicalHostName()).thenReturn("client.example.com");
        assertTrue(authorizer.permit(mkContext("bob", "alice", remoteAddress), "execute", null));
    }

    @Test
    public void testUnlistedHostDenied() {
        ImpersonationAuthorizer authorizer = mkAuthorizer("bob", Collections.singletonList("192.0.2.10"));
        InetAddress remoteAddress = mock(InetAddress.class);
        when(remoteAddress.getHostAddress()).thenReturn("192.0.2.11");
        when(remoteAddress.getCanonicalHostName()).thenReturn("other.example.com");
        when(remoteAddress.getHostName()).thenReturn("other.example.com");
        assertFalse(authorizer.permit(mkContext("bob", "alice", remoteAddress), "execute", null));
    }
}
