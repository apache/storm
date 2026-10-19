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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.AfterAll;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.drpc.DRPCInvocationsClient;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.DefaultHttpCredentialsPlugin;
import org.apache.storm.security.auth.DefaultPrincipalToLocal;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.SimpleTransportPlugin;
import org.apache.storm.security.auth.SingleUserPrincipal;
import org.apache.storm.security.auth.authorizer.DRPCSimpleACLAuthorizer;
import org.apache.storm.security.auth.authorizer.ImpersonationAuthorizer;
import org.apache.storm.utils.DRPCClient;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DRPCServerTest {
    private static final Logger LOG = LoggerFactory.getLogger(DRPCServerTest.class);
    private static final ExecutorService exec = Executors.newCachedThreadPool();
    
    @AfterAll
    public static void close() {
        exec.shutdownNow();
    }
    
    private static DRPCRequest getNextAvailableRequest(DRPCInvocationsClient invoke, String func) throws Exception {
        DRPCRequest request = null;
        long timedout = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < timedout) {
            request = invoke.getClient().fetchRequest(func);
            if (request != null && request.get_request_id() != null && !request.get_request_id().isEmpty()) {
                return request;
            }
            Thread.sleep(1);
        }
        fail("Test timed out waiting for a request on " + func);
        return request;
    }
    
    private Map<String, Object> getConf(int drpcPort, int invocationsPort, Integer httpPort) {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.DRPC_PORT, drpcPort);
        conf.put(Config.DRPC_INVOCATIONS_PORT, invocationsPort);
        conf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, SimpleTransportPlugin.class.getName());
        conf.put(Config.DRPC_WORKER_THREADS, 5);
        conf.put(Config.DRPC_INVOCATIONS_THREADS, 5);
        conf.put(Config.DRPC_MAX_BUFFER_SIZE, 1048576);
        conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 2);
        conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 10);
        conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 100);
        if (httpPort != null) {
            conf.put(DaemonConfig.DRPC_HTTP_PORT, httpPort);
        }
        return conf;
    }
    
    @Test
    public void testGoodThrift() throws Exception {
        Map<String, Object> conf = getConf(0, 0, null);
        try (DRPCServer server = new DRPCServer(conf, new StormMetricsRegistry())) {
            server.start();
            try (DRPCClient client = new DRPCClient(conf, "localhost", server.getDrpcPort());
                DRPCInvocationsClient invoke = new DRPCInvocationsClient(conf, "localhost", server.getDrpcInvokePort())) {
                final Future<String> found = exec.submit(() -> client.getClient().execute("testing", "test"));
                DRPCRequest request = getNextAvailableRequest(invoke, "testing");
                assertNotNull(request);
                assertEquals("test", request.get_func_args());
                assertNotNull(request.get_request_id());
                invoke.result(request.get_request_id(), "tested");
                String result = found.get(1000, TimeUnit.MILLISECONDS);
                assertEquals("tested", result);
            }
        }
    }
    
    @Test
    public void testFailedThrift() throws Exception {
        Map<String, Object> conf = getConf(0, 0, null);
        try (DRPCServer server = new DRPCServer(conf, new StormMetricsRegistry())) {
            server.start();
            try (DRPCClient client = new DRPCClient(conf, "localhost", server.getDrpcPort());
                    DRPCInvocationsClient invoke = new DRPCInvocationsClient(conf, "localhost", server.getDrpcInvokePort())) {
                Future<String> found = exec.submit(() -> client.getClient().execute("testing", "test"));
                DRPCRequest request = getNextAvailableRequest(invoke, "testing");
                assertNotNull(request);
                assertEquals("test", request.get_func_args());
                assertNotNull(request.get_request_id());
                invoke.failRequest(request.get_request_id());
                try {
                    found.get(1000, TimeUnit.MILLISECONDS);
                    fail("exec did not throw an exception");
                } catch (ExecutionException e) {
                    Throwable t = e.getCause();
                    assertEquals(t.getClass(), DRPCExecutionException.class);
                    //Don't know a better way to validate that it failed.
                    assertEquals("Request failed", ((DRPCExecutionException)t).get_msg());
                }
            }
        }
    }
    
    private static String doGet(int port, String func, String args) {
        try {
            URL url = new URL("http://localhost:" + port + "/drpc/" + func + "/" + args);
            InputStream in = url.openStream();
            byte[] buffer = new byte[1024];
            int read = in.read(buffer);
            return new String(buffer, 0, read);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @Test
    public void testGoodHttpGet() throws Exception {
        LOG.info("STARTING HTTP GET TEST...");
        Map<String, Object> conf = getConf(0, 0, 0);
        try (DRPCServer server = new DRPCServer(conf, new StormMetricsRegistry())) {
            server.start();
            //TODO need a better way to do this
            Thread.sleep(2000);
            try (DRPCInvocationsClient invoke = new DRPCInvocationsClient(conf, "localhost", server.getDrpcInvokePort())) {
                final Future<String> found = exec.submit(() -> doGet(server.getHttpServerPort(), "testing", "test"));
                DRPCRequest request = getNextAvailableRequest(invoke, "testing");
                assertNotNull(request);
                assertEquals("test", request.get_func_args());
                assertNotNull(request.get_request_id());
                invoke.result(request.get_request_id(), "tested");
                String result = found.get(1000, TimeUnit.MILLISECONDS);
                assertEquals("tested", result);
            }
        }
    }
    
    @Test
    public void testFailedHttpGet() throws Exception {
        LOG.info("STARTING HTTP GET (FAIL) TEST...");
        Map<String, Object> conf = getConf(0, 0, 0);
        try (DRPCServer server = new DRPCServer(conf, new StormMetricsRegistry())) {
            server.start();
            //TODO need a better way to do this
            Thread.sleep(2000);
            try (DRPCInvocationsClient invoke = new DRPCInvocationsClient(conf, "localhost", server.getDrpcInvokePort())) {
                Future<String> found = exec.submit(() -> doGet(server.getHttpServerPort(), "testing", "test"));
                DRPCRequest request = getNextAvailableRequest(invoke, "testing");
                assertNotNull(request);
                assertEquals("test", request.get_func_args());
                assertNotNull(request.get_request_id());
                invoke.getClient().failRequest(request.get_request_id());
                try {
                    found.get(1000, TimeUnit.MILLISECONDS);
                    fail("exec did not throw an exception");
                } catch (ExecutionException e) {
                    LOG.warn("Got Expected Exception", e);
                    //Getting the exact response code is a bit more complex.
                    //TODO should use a better client
                }
            }
        }
    }

    /**
     * Authenticates every HTTP request as the user given in the "user" init parameter.
     */
    public static class FixedUserFilter implements Filter {
        private String user;

        @Override
        public void init(FilterConfig config) {
            user = config.getInitParameter("user");
        }

        @Override
        public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
            chain.doFilter(new HttpServletRequestWrapper((HttpServletRequest) request) {
                @Override
                public Principal getUserPrincipal() {
                    return new SingleUserPrincipal(user);
                }

                @Override
                public String getRemoteUser() {
                    return user;
                }
            }, response);
        }

        @Override
        public void destroy() {
        }
    }

    /**
     * Only "alice" may execute the function "sensitive"; any client may fetch and answer requests.
     */
    public static class SensitiveFunctionAclAuthorizer extends DRPCSimpleACLAuthorizer {
        @Override
        protected Map<String, AclFunctionEntry> readAclFromConfig() {
            Map<String, AclFunctionEntry> acl = new HashMap<>();
            acl.put("sensitive", new AclFunctionEntry(Collections.singletonList("alice"), "sensitive_topo"));
            return acl;
        }

        @Override
        protected boolean permitInvocationRequest(ReqContext context, String operation, Map<String, Object> params) {
            return true;
        }
    }

    private Map<String, Object> getImpersonationConf(String httpUser) {
        return getImpersonationConf(httpUser, new HashMap<>());
    }

    private Map<String, Object> getImpersonationConf(String httpUser, Map<String, Map<String, List<String>>> impersonationAcl) {
        Map<String, Object> conf = getConf(0, 0, 0);
        conf.put(DaemonConfig.DRPC_HTTP_FILTER, FixedUserFilter.class.getName());
        conf.put(DaemonConfig.DRPC_HTTP_FILTER_PARAMS, Collections.singletonMap("user", httpUser));
        conf.put(DaemonConfig.DRPC_HTTP_CREDS_PLUGIN, DefaultHttpCredentialsPlugin.class.getName());
        conf.put(DaemonConfig.DRPC_AUTHORIZER, SensitiveFunctionAclAuthorizer.class.getName());
        conf.put(Config.DRPC_AUTHORIZER_ACL_STRICT, true);
        conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, DefaultPrincipalToLocal.class.getName());
        conf.put(DaemonConfig.NIMBUS_IMPERSONATION_AUTHORIZER, ImpersonationAuthorizer.class.getName());
        conf.put(Config.NIMBUS_IMPERSONATION_ACL, impersonationAcl);
        conf.put(DaemonConfig.DRPC_REQUEST_TIMEOUT_SECS, 30);
        return conf;
    }

    private static Map<String, Map<String, List<String>>> impersonationAclFor(String user, List<String> hosts) {
        Map<String, List<String>> entry = new HashMap<>();
        entry.put("groups", Collections.singletonList("*"));
        entry.put("hosts", hosts);
        return Collections.singletonMap(user, entry);
    }

    private static HttpResult doGetWithStatus(int port, String pathAndQuery) {
        try {
            URL url = new URL("http://localhost:" + port + pathAndQuery);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            try {
                conn.setReadTimeout(10_000);
                int status = conn.getResponseCode();
                String body = "";
                try (InputStream in = status < 400 ? conn.getInputStream() : conn.getErrorStream()) {
                    if (in != null) {
                        body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                    }
                }
                return new HttpResult(status, body);
            } finally {
                conn.disconnect();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final class HttpResult {
        private final int status;
        private final String body;

        private HttpResult(int status, String body) {
            this.status = status;
            this.body = body;
        }
    }

    /**
     * Sends the GET request and answers any request for "sensitive" that reaches the DRPC queue.
     */
    private HttpResult runSensitiveRequest(Map<String, Object> conf, String query) throws Exception {
        try (DRPCServer server = new DRPCServer(conf, new StormMetricsRegistry())) {
            server.start();
            //TODO need a better way to do this
            Thread.sleep(2000);
            try (DRPCInvocationsClient invoke = new DRPCInvocationsClient(conf, "localhost", server.getDrpcInvokePort())) {
                Future<HttpResult> found = exec.submit(
                    () -> doGetWithStatus(server.getHttpServerPort(), "/drpc/sensitive/args" + query));
                long timeout = System.currentTimeMillis() + 5_000;
                while (!found.isDone() && System.currentTimeMillis() < timeout) {
                    DRPCRequest request = invoke.getClient().fetchRequest("sensitive");
                    if (request != null && request.get_request_id() != null && !request.get_request_id().isEmpty()) {
                        invoke.result(request.get_request_id(), "sensitive-result");
                        break;
                    }
                    Thread.sleep(1);
                }
                try {
                    return found.get(5, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    found.cancel(true);
                    fail("Test timed out waiting for the response to the request on sensitive");
                    return null;
                }
            }
        }
    }

    @Test
    public void testHttpImpersonationDeniedByImpersonationAuthorizer() throws Exception {
        HttpResult result = runSensitiveRequest(getImpersonationConf("bob"), "?doAsUser=alice");
        assertEquals(401, result.status);
        assertNotEquals("sensitive-result", result.body);
    }

    @Test
    public void testHttpImpersonationFromListedHostPermitted() throws Exception {
        Map<String, Map<String, List<String>>> acl =
            impersonationAclFor("bob", Arrays.asList("localhost", "127.0.0.1", "0:0:0:0:0:0:0:1"));
        HttpResult result = runSensitiveRequest(getImpersonationConf("bob", acl), "?doAsUser=alice");
        assertEquals(200, result.status);
        assertEquals("sensitive-result", result.body);
    }

    @Test
    public void testHttpImpersonationFromUnlistedHostDenied() throws Exception {
        Map<String, Map<String, List<String>>> acl = impersonationAclFor("bob", Collections.singletonList("192.0.2.1"));
        HttpResult result = runSensitiveRequest(getImpersonationConf("bob", acl), "?doAsUser=alice");
        assertEquals(401, result.status);
        assertNotEquals("sensitive-result", result.body);
    }

    @Test
    public void testHttpAllowedUserWithoutImpersonation() throws Exception {
        HttpResult result = runSensitiveRequest(getImpersonationConf("alice"), "");
        assertEquals(200, result.status);
        assertEquals("sensitive-result", result.body);
    }

    @Test
    public void testHttpOtherUserWithoutImpersonationDenied() throws Exception {
        HttpResult result = runSensitiveRequest(getImpersonationConf("bob"), "");
        assertEquals(401, result.status);
        assertNotEquals("sensitive-result", result.body);
    }
}
