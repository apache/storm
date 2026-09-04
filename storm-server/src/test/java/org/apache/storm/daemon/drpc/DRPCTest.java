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

package org.apache.storm.daemon.drpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.auth.Subject;
import org.apache.storm.Config;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExceptionType;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.DefaultPrincipalToLocal;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.SingleUserPrincipal;
import org.apache.storm.security.auth.authorizer.DRPCSimpleACLAuthorizer.AclFunctionEntry;
import org.apache.storm.security.auth.authorizer.DRPCSimpleACLAuthorizer;
import org.apache.storm.security.auth.authorizer.DenyAuthorizer;
import org.apache.storm.utils.Time;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

public class DRPCTest {
    private static final ExecutorService exec = Executors.newCachedThreadPool();

    private static void assertThrows(ThrowStuff t, Class<? extends Exception> expected) {
        try {
            t.run();
            fail("Expected " + t + " to throw " + expected + " didn't throw at all...");
        } catch (Exception e) {
            assertTrue(expected.isInstance(e), "Expected " + t + " to throw " + expected + " but threw " + e);
        }
    }

    @AfterAll
    public static void close() {
        exec.shutdownNow();
    }

    public static DRPCRequest getNextAvailableRequest(DRPC server, String func) {
        AtomicReference<DRPCRequest> result = new AtomicReference<>();
        Awaitility.await("DRPC request on " + func)
            .atMost(5, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.MILLISECONDS)
            .until(() -> {
                DRPCRequest req = server.fetchRequest(func);
                if (req != null && req.get_request_id() != null && !req.get_request_id().isEmpty()) {
                    result.set(req);
                    return true;
                }
                return false;
            });
        return result.get();
    }

    @Test
    public void testGoodBlocking() throws Exception {
        try (DRPC server = new DRPC(new StormMetricsRegistry(), null, 100)) {
            Future<String> found = exec.submit(() -> server.executeBlocking("testing", "test"));
            DRPCRequest request = getNextAvailableRequest(server, "testing");
            assertNotNull(request);
            assertEquals("test", request.get_func_args());
            assertNotNull(request.get_request_id());
            server.returnResult(request.get_request_id(), "tested");
            String result = found.get(10, TimeUnit.MILLISECONDS);
            assertEquals("tested", result);
        }
    }

    @Test
    public void testFailedBlocking() throws Exception {
        try (DRPC server = new DRPC(new StormMetricsRegistry(), null, 100)) {
            Future<String> found = exec.submit(() -> server.executeBlocking("testing", "test"));
            DRPCRequest request = getNextAvailableRequest(server, "testing");
            assertNotNull(request);
            assertEquals("test", request.get_func_args());
            assertNotNull(request.get_request_id());
            server.failRequest(request.get_request_id(), null);
            try {
                found.get(100, TimeUnit.MILLISECONDS);
                fail("exec did not throw an exception");
            } catch (ExecutionException e) {
                Throwable t = e.getCause();
                assertTrue(t instanceof DRPCExecutionException);
                //Don't know a better way to validate that it failed.
                assertEquals(DRPCExceptionType.FAILED_REQUEST, ((DRPCExecutionException) t).get_type());
            }
        }
    }

    @Test
    public void testDequeueAfterTimeout() throws Exception {
        long timeout = 1000;
        try (DRPC server = new DRPC(new StormMetricsRegistry(), null, timeout)) {
            long start = Time.currentTimeMillis();
            try {
                server.executeBlocking("testing", "test");
                fail("Should have timed out....");
            } catch (DRPCExecutionException e) {
                long spent = Time.currentTimeMillis() - start;
                assertTrue(spent < timeout * 2);
                assertTrue(spent >= timeout);
                assertEquals(DRPCExceptionType.SERVER_TIMEOUT, e.get_type());
            }
            DRPCRequest request = server.fetchRequest("testing");
            assertNotNull(request);
            assertEquals("", request.get_request_id());
            assertEquals("", request.get_func_args());
        }
    }

    @Test
    public void testQueuesAreRemovedWhenEmpty() throws Exception {
        try (DRPC server = new DRPC(new StormMetricsRegistry(), null, 1000)) {
            //Fetching for a function nothing was ever submitted for must not leave state behind
            DRPCRequest nothing = server.fetchRequest("never-registered");
            assertNotNull(nothing);
            assertEquals("", nothing.get_request_id());
            assertEquals(0, server.getNumTrackedFunctions());

            //A registered function is still served repeatedly, and is not left behind once idle
            for (int i = 0; i < 3; i++) {
                Future<String> found = exec.submit(() -> server.executeBlocking("testing", "test"));
                DRPCRequest request = getNextAvailableRequest(server, "testing");
                assertNotNull(request);
                server.returnResult(request.get_request_id(), "tested");
                assertEquals("tested", found.get(10, TimeUnit.MILLISECONDS));
            }
            assertEquals(0, server.getNumTrackedFunctions());

            //Nor is a function whose only request timed out.  The timer thread fails the request
            // before it drops the queue, so the caller can return first; wait for the drop instead
            // of racing it, with a hard timeout so a real leak still fails the test.
            try {
                server.executeBlocking("timing-out", "test");
                fail("Should have timed out....");
            } catch (DRPCExecutionException e) {
                assertEquals(DRPCExceptionType.SERVER_TIMEOUT, e.get_type());
            }
            Awaitility.await("DRPC queue for timing-out to be dropped")
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.MILLISECONDS)
                .until(() -> server.getNumTrackedFunctions() == 0);
        }
    }

    @Test
    public void testConcurrentExecuteAndFetchLosesNoRequests() throws Exception {
        //A bounded pool of 16 threads is what keeps this cheap: executeBlocking() parks its caller,
        // so an unbounded pool would need one live thread per outstanding request.  The request
        // count costs no threads at all, and is what gives the stress test its power.  Measured
        // against a fetchRequest() whose poll/remove escapes the per-function compute lock, the
        // lost request was caught 1 run in 25 at 200 requests, 3 in 10 at 2000 and 9 in 10 at 5000,
        // while a correct server still serves all 5000 in about a second with every core busy.
        final int numRequests = 5000;
        final int numThreads = 16;
        final long deadlineMs = 30_000;
        //A timeout far beyond the test deadline, so the cleanup timer never reaps a live request.
        try (DRPC server = new DRPC(new StormMetricsRegistry(), null, 300_000)) {
            ExecutorService submitters = Executors.newFixedThreadPool(numThreads);
            try {
                List<Future<String>> futures = new ArrayList<>(numRequests);
                for (int i = 0; i < numRequests; i++) {
                    final String args = "test-" + i;
                    futures.add(submitters.submit(() -> server.executeBlocking("testing", args)));
                }

                Set<String> servedIds = new HashSet<>();
                long deadline = Time.currentTimeMillis() + deadlineMs;
                int emptyFetches = 0;
                while (servedIds.size() < numRequests) {
                    if (Time.currentTimeMillis() > deadline) {
                        fail("Only served " + servedIds.size() + " of " + numRequests
                             + " requests within " + deadlineMs + "ms, a request was lost");
                    }
                    DRPCRequest req = server.fetchRequest("testing");
                    assertNotNull(req);
                    String id = req.get_request_id();
                    if (id.isEmpty()) {
                        //Nothing to serve right now.  Spin at first, so fetches keep interleaving
                        // tightly with the submitting threads, and only back off if this goes on
                        // for a long time (a regression, which the deadline above then fails).
                        if (++emptyFetches > 10_000) {
                            TimeUnit.MILLISECONDS.sleep(1);
                        } else {
                            Thread.onSpinWait();
                        }
                        continue;
                    }
                    emptyFetches = 0;
                    assertTrue(servedIds.add(id), "Request " + id + " was fetched more than once");
                    server.returnResult(id, "tested-" + id);
                }

                Set<String> results = new HashSet<>();
                for (Future<String> f : futures) {
                    long left = deadline - Time.currentTimeMillis();
                    assertTrue(left > 0, "Ran out of time waiting for the blocked callers");
                    assertTrue(results.add(f.get(left, TimeUnit.MILLISECONDS)), "Duplicate result returned");
                }
                assertEquals(numRequests, results.size());
                for (String id : servedIds) {
                    assertTrue(results.contains("tested-" + id), "No caller got the result for " + id);
                }
                //Nothing is waiting any more, so no per-function queue may be left behind
                assertEquals(0, server.getNumTrackedFunctions());
            } finally {
                submitters.shutdownNow();
            }
        }
    }

    @Test
    public void testDeny() {
        try (DRPC server = new DRPC(new StormMetricsRegistry(), new DenyAuthorizer(), 100)) {
            assertThrows(() -> server.executeBlocking("testing", "test"), AuthorizationException.class);
            assertThrows(() -> server.fetchRequest("testing"), AuthorizationException.class);
        }
    }

    @Test
    public void testStrict() throws Exception {
        ReqContext jt = new ReqContext(new Subject());
        SingleUserPrincipal jumpTopo = new SingleUserPrincipal("jump_topo");
        jt.subject().getPrincipals().add(jumpTopo);

        ReqContext jc = new ReqContext(new Subject());
        SingleUserPrincipal jumpClient = new SingleUserPrincipal("jump_client");
        jc.subject().getPrincipals().add(jumpClient);

        ReqContext other = new ReqContext(new Subject());
        SingleUserPrincipal otherUser = new SingleUserPrincipal("other");
        other.subject().getPrincipals().add(otherUser);

        Map<String, AclFunctionEntry> acl = new HashMap<>();
        acl.put("jump", new AclFunctionEntry(Collections.singletonList(jumpClient.getName()), jumpTopo.getName()));
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.DRPC_AUTHORIZER_ACL_STRICT, true);
        conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, DefaultPrincipalToLocal.class.getName());
        DRPCSimpleACLAuthorizer auth = new DRPCSimpleACLAuthorizer() {
            @Override
            protected Map<String, AclFunctionEntry> readAclFromConfig() {
                return acl;
            }
        };
        auth.prepare(conf);
        //JUMP
        DRPC.checkAuthorization(jt, auth, "fetchRequest", "jump");
        assertThrows(() -> DRPC.checkAuthorization(jc, auth, "fetchRequest", "jump"), AuthorizationException.class);
        assertThrows(() -> DRPC.checkAuthorization(other, auth, "fetchRequest", "jump"), AuthorizationException.class);

        DRPC.checkAuthorization(jt, auth, "result", "jump");
        assertThrows(() -> DRPC.checkAuthorization(jc, auth, "result", "jump"), AuthorizationException.class);
        assertThrows(() -> DRPC.checkAuthorization(other, auth, "result", "jump"), AuthorizationException.class);

        assertThrows(() -> DRPC.checkAuthorization(jt, auth, "execute", "jump"), AuthorizationException.class);
        DRPC.checkAuthorization(jc, auth, "execute", "jump");
        assertThrows(() -> DRPC.checkAuthorization(other, auth, "execute", "jump"), AuthorizationException.class);

        //not_jump (closed in strict mode)
        assertThrows(() -> DRPC.checkAuthorization(jt, auth, "fetchRequest", "not_jump"), AuthorizationException.class);
        assertThrows(() -> DRPC.checkAuthorization(jc, auth, "fetchRequest", "not_jump"), AuthorizationException.class);
        assertThrows(() -> DRPC.checkAuthorization(other, auth, "fetchRequest", "not_jump"), AuthorizationException.class);

        assertThrows(() -> DRPC.checkAuthorization(jt, auth, "result", "not_jump"), AuthorizationException.class);
        assertThrows(() -> DRPC.checkAuthorization(jc, auth, "result", "not_jump"), AuthorizationException.class);
        assertThrows(() -> DRPC.checkAuthorization(other, auth, "result", "not_jump"), AuthorizationException.class);

        assertThrows(() -> DRPC.checkAuthorization(jt, auth, "execute", "not_jump"), AuthorizationException.class);
        assertThrows(() -> DRPC.checkAuthorization(jc, auth, "execute", "not_jump"), AuthorizationException.class);
        assertThrows(() -> DRPC.checkAuthorization(other, auth, "execute", "not_jump"), AuthorizationException.class);
    }

    @Test
    public void testNotStrict() throws Exception {
        ReqContext jt = new ReqContext(new Subject());
        SingleUserPrincipal jumpTopo = new SingleUserPrincipal("jump_topo");
        jt.subject().getPrincipals().add(jumpTopo);

        ReqContext jc = new ReqContext(new Subject());
        SingleUserPrincipal jumpClient = new SingleUserPrincipal("jump_client");
        jc.subject().getPrincipals().add(jumpClient);

        ReqContext other = new ReqContext(new Subject());
        SingleUserPrincipal otherUser = new SingleUserPrincipal("other");
        other.subject().getPrincipals().add(otherUser);

        Map<String, AclFunctionEntry> acl = new HashMap<>();
        acl.put("jump", new AclFunctionEntry(Collections.singletonList(jumpClient.getName()), jumpTopo.getName()));
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.DRPC_AUTHORIZER_ACL_STRICT, false);
        conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, DefaultPrincipalToLocal.class.getName());
        DRPCSimpleACLAuthorizer auth = new DRPCSimpleACLAuthorizer() {
            @Override
            protected Map<String, AclFunctionEntry> readAclFromConfig() {
                return acl;
            }
        };
        auth.prepare(conf);
        //JUMP
        DRPC.checkAuthorization(jt, auth, "fetchRequest", "jump");
        assertThrows(() -> DRPC.checkAuthorization(jc, auth, "fetchRequest", "jump"), AuthorizationException.class);
        assertThrows(() -> DRPC.checkAuthorization(other, auth, "fetchRequest", "jump"), AuthorizationException.class);

        DRPC.checkAuthorization(jt, auth, "result", "jump");
        assertThrows(() -> DRPC.checkAuthorization(jc, auth, "result", "jump"), AuthorizationException.class);
        assertThrows(() -> DRPC.checkAuthorization(other, auth, "result", "jump"), AuthorizationException.class);

        assertThrows(() -> DRPC.checkAuthorization(jt, auth, "execute", "jump"), AuthorizationException.class);
        DRPC.checkAuthorization(jc, auth, "execute", "jump");
        assertThrows(() -> DRPC.checkAuthorization(other, auth, "execute", "jump"), AuthorizationException.class);

        //not_jump (open in not strict mode)
        DRPC.checkAuthorization(jt, auth, "fetchRequest", "not_jump");
        DRPC.checkAuthorization(jc, auth, "fetchRequest", "not_jump");
        DRPC.checkAuthorization(other, auth, "fetchRequest", "not_jump");

        DRPC.checkAuthorization(jt, auth, "result", "not_jump");
        DRPC.checkAuthorization(jc, auth, "result", "not_jump");
        DRPC.checkAuthorization(other, auth, "result", "not_jump");

        DRPC.checkAuthorization(jt, auth, "execute", "not_jump");
        DRPC.checkAuthorization(jc, auth, "execute", "not_jump");
        DRPC.checkAuthorization(other, auth, "execute", "not_jump");
    }

    public interface ThrowStuff {
        void run() throws Exception;
    }
}
