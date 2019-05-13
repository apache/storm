/**
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

package org.apache.storm.security.auth.workertoken;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.PrivateWorkerKey;
import org.apache.storm.generated.WorkerToken;
import org.apache.storm.generated.WorkerTokenInfo;
import org.apache.storm.generated.WorkerTokenServiceType;
import org.apache.storm.security.auth.ClientAuthUtils;
import org.apache.storm.utils.Time;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class WorkerTokenTest {
    public static final long ONE_DAY_MILLIS = TimeUnit.HOURS.toMillis(24);

    @Test
    public void testBasicGenerateAndAuthorize() {
        final AtomicReference<PrivateWorkerKey> privateKey = new AtomicReference<>();
        final String topoId = "topo-1";
        final String userName = "user";
        final WorkerTokenServiceType type = WorkerTokenServiceType.NIMBUS;
        final long versionNumber = 0L;
        //Simulate time starts out at 0, so we are going to just leave it here.
        try (Time.SimulatedTime sim = new Time.SimulatedTime()) {
            IStormClusterState mockState = mock(IStormClusterState.class);
            Map<String, Object> conf = new HashMap<>();
            WorkerTokenManager wtm = new WorkerTokenManager(conf, mockState);

            when(mockState.getNextPrivateWorkerKeyVersion(type, topoId)).thenReturn(versionNumber);
            doAnswer((invocation) -> {
                //Save the private worker key away so we can test it too.
                privateKey.set(invocation.getArgument(3));
                return null;
            }).when(mockState).addPrivateWorkerKey(eq(type), eq(topoId), eq(versionNumber), any(PrivateWorkerKey.class));
            //Answer when we ask for a private key...
            when(mockState.getPrivateWorkerKey(type, topoId, versionNumber)).thenAnswer((invocation) -> privateKey.get());

            WorkerToken wt = wtm.createOrUpdateTokenFor(type, userName, topoId);
            verify(mockState).addPrivateWorkerKey(eq(type), eq(topoId), eq(versionNumber), any(PrivateWorkerKey.class));
            assertTrue(wt.is_set_serviceType());
            assertEquals(type, wt.get_serviceType());
            assertTrue(wt.is_set_info());
            assertTrue(wt.is_set_signature());

            PrivateWorkerKey pwk = privateKey.get();
            assertNotNull(pwk);
            assertTrue(pwk.is_set_expirationTimeMillis());
            assertEquals(ONE_DAY_MILLIS, pwk.get_expirationTimeMillis());

            WorkerTokenInfo info = ClientAuthUtils.getWorkerTokenInfo(wt);
            assertTrue(info.is_set_topologyId());
            assertTrue(info.is_set_userName());
            assertTrue(info.is_set_expirationTimeMillis());
            assertTrue(info.is_set_secretVersion());
            assertEquals(topoId, info.get_topologyId());
            assertEquals(userName, info.get_userName());
            assertEquals(ONE_DAY_MILLIS, info.get_expirationTimeMillis());
            assertEquals(versionNumber, info.get_secretVersion());

            try (WorkerTokenAuthorizer wta = new WorkerTokenAuthorizer(type, mockState)) {
                //Verify the signature...
                byte[] signature = wta.getSignedPasswordFor(wt.get_info(), info);
                assertArrayEquals(wt.get_signature(), signature);
            }
        }
    }

    @Test
    public void testExpiration() {
        final AtomicReference<PrivateWorkerKey> privateKey = new AtomicReference<>();
        final String topoId = "topo-1";
        final String userName = "user";
        final WorkerTokenServiceType type = WorkerTokenServiceType.NIMBUS;
        final long versionNumber = 5L;
        //Simulate time starts out at 0, so we are going to just leave it here.
        try (Time.SimulatedTime sim = new Time.SimulatedTime()) {
            IStormClusterState mockState = mock(IStormClusterState.class);
            Map<String, Object> conf = new HashMap<>();
            WorkerTokenManager wtm = new WorkerTokenManager(conf, mockState);

            when(mockState.getNextPrivateWorkerKeyVersion(type, topoId)).thenReturn(versionNumber);
            doAnswer((invocation) -> {
                //Save the private worker key away so we can test it too.
                privateKey.set(invocation.getArgument(3));
                return null;
            }).when(mockState).addPrivateWorkerKey(eq(type), eq(topoId), eq(versionNumber), any(PrivateWorkerKey.class));
            //Answer when we ask for a private key...
            when(mockState.getPrivateWorkerKey(type, topoId, versionNumber)).thenAnswer((invocation) -> privateKey.get());

            WorkerToken wt = wtm.createOrUpdateTokenFor(type, userName, topoId);
            verify(mockState).addPrivateWorkerKey(eq(type), eq(topoId), eq(versionNumber), any(PrivateWorkerKey.class));
            assertTrue(wt.is_set_serviceType());
            assertEquals(type, wt.get_serviceType());
            assertTrue(wt.is_set_info());
            assertTrue(wt.is_set_signature());

            PrivateWorkerKey pwk = privateKey.get();
            assertNotNull(pwk);
            assertTrue(pwk.is_set_expirationTimeMillis());
            assertEquals(ONE_DAY_MILLIS, pwk.get_expirationTimeMillis());

            WorkerTokenInfo info = ClientAuthUtils.getWorkerTokenInfo(wt);
            assertTrue(info.is_set_topologyId());
            assertTrue(info.is_set_userName());
            assertTrue(info.is_set_expirationTimeMillis());
            assertTrue(info.is_set_secretVersion());
            assertEquals(topoId, info.get_topologyId());
            assertEquals(userName, info.get_userName());
            assertEquals(ONE_DAY_MILLIS, info.get_expirationTimeMillis());
            assertEquals(versionNumber, info.get_secretVersion());

            //Expire the token
            Time.advanceTime(ONE_DAY_MILLIS + 1);

            try (WorkerTokenAuthorizer wta = new WorkerTokenAuthorizer(type, mockState)) {
                try {
                    //Verify the signature...
                    wta.getSignedPasswordFor(wt.get_info(), info);
                    fail("Expected an expired token to not be signed!!!");
                } catch (IllegalArgumentException ia) {
                    //What we want...
                }
            }

            //Verify if WorkerTokenManager recognizes the expired WorkerToken.
            Map<String, String> creds = new HashMap<>();
            ClientAuthUtils.setWorkerToken(creds, wt);
            assertTrue("Expired WorkerToken should be eligible for renewal", wtm.shouldRenewWorkerToken(creds, type));
        }
    }
}
