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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.drpc.DRPCInvocationsClient;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.SimpleTransportPlugin;
import org.apache.storm.utils.DRPCClient;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DRPCServerTest {
    private static final Logger LOG = LoggerFactory.getLogger(DRPCServerTest.class);
    private static final ExecutorService exec = Executors.newCachedThreadPool();
    
    @AfterClass
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
}
