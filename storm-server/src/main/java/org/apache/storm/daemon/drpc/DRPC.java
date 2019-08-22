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

import com.codahale.metrics.Meter;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExceptionType;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.logging.ThriftAccessLogger;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.authorizer.DRPCAuthorizerBase;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.WrappedAuthorizationException;
import org.apache.storm.utils.WrappedDRPCExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class DRPC implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DRPC.class);
    private static final DRPCRequest NOTHING_REQUEST = new DRPCRequest("", "");
    private static final DRPCExecutionException TIMED_OUT = new WrappedDRPCExecutionException("Timed Out");
    private static final DRPCExecutionException SHUT_DOWN = new WrappedDRPCExecutionException("Server Shutting Down");
    private static final DRPCExecutionException DEFAULT_FAILED = new WrappedDRPCExecutionException("Request failed");
    
    private final Meter meterServerTimedOut;
    private final Meter meterExecuteCalls;
    private final Meter meterResultCalls;
    private final Meter meterFailRequestCalls;
    private final Meter meterFetchRequestCalls;

    static {
        TIMED_OUT.set_type(DRPCExceptionType.SERVER_TIMEOUT);
        SHUT_DOWN.set_type(DRPCExceptionType.SERVER_SHUTDOWN);
        DEFAULT_FAILED.set_type(DRPCExceptionType.FAILED_REQUEST);
    }

    //Waiting to be fetched
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<OutstandingRequest>> queues =
            new ConcurrentHashMap<>();
    //Waiting to be returned
    private final ConcurrentHashMap<String, OutstandingRequest> requests =
            new ConcurrentHashMap<>();
    private final Timer timer = new Timer("DRPC-CLEANUP-TIMER", true);
    private final AtomicLong ctr = new AtomicLong(0);
    private final IAuthorizer auth;

    public DRPC(StormMetricsRegistry metricsRegistry, Map<String, Object> conf) {
        this(metricsRegistry, mkAuthorizationHandler((String) conf.get(DaemonConfig.DRPC_AUTHORIZER), conf),
             ObjectReader.getInt(conf.get(DaemonConfig.DRPC_REQUEST_TIMEOUT_SECS), 600) * 1000);
    }

    public DRPC(StormMetricsRegistry metricsRegistry, IAuthorizer auth, long timeoutMs) {
        this.auth = auth;
        this.meterServerTimedOut = metricsRegistry.registerMeter("drpc:num-server-timedout-requests");
        this.meterExecuteCalls = metricsRegistry.registerMeter("drpc:num-execute-calls");
        this.meterResultCalls = metricsRegistry.registerMeter("drpc:num-result-calls");
        this.meterFailRequestCalls = metricsRegistry.registerMeter("drpc:num-failRequest-calls");
        this.meterFetchRequestCalls = metricsRegistry.registerMeter("drpc:num-fetchRequest-calls");
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                cleanupAll(timeoutMs, TIMED_OUT);
            }
        }, timeoutMs / 2, timeoutMs / 2);
    }

    private static IAuthorizer mkAuthorizationHandler(String klassname, Map<String, Object> conf) {
        try {
            return StormCommon.mkAuthorizationHandler(klassname, conf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void logAccess(String operation, String function) {
        logAccess(ReqContext.context(), operation, function);
    }

    private static void logAccess(ReqContext reqContext, String operation, String function) {
        ThriftAccessLogger.logAccessFunction(reqContext.requestID(), reqContext.remoteAddress(), reqContext.principal(), operation,
                                             function);
    }

    @VisibleForTesting
    static void checkAuthorization(ReqContext reqContext, IAuthorizer auth, String operation, String function)
        throws AuthorizationException {
        checkAuthorization(reqContext, auth, operation, function, true);
    }

    private static void checkAuthorization(ReqContext reqContext, IAuthorizer auth, String operation, String function, boolean log)
        throws AuthorizationException {
        if (reqContext != null && log) {
            logAccess(reqContext, operation, function);
        }
        if (auth != null) {
            Map<String, Object> map = new HashMap<>();
            map.put(DRPCAuthorizerBase.FUNCTION_NAME, function);
            if (!auth.permit(reqContext, operation, map)) {
                Principal principal = reqContext.principal();
                String user = (principal != null) ? principal.getName() : "unknown";
                throw new WrappedAuthorizationException("DRPC request '" + operation + "' for '" + user + "' user is not authorized");
            }
        }
    }

    private void checkAuthorization(String operation, String function) throws AuthorizationException {
        checkAuthorization(ReqContext.context(), auth, operation, function);
    }

    private void checkAuthorizationNoLog(String operation, String function) throws AuthorizationException {
        checkAuthorization(ReqContext.context(), auth, operation, function, false);
    }

    private void cleanup(String id) {
        OutstandingRequest req = requests.remove(id);
        if (req != null && !req.wasFetched()) {
            queues.get(req.getFunction()).remove(req);
        }
    }

    private void cleanupAll(long timeoutMs, DRPCExecutionException exp) {
        for (Entry<String, OutstandingRequest> e : requests.entrySet()) {
            OutstandingRequest req = e.getValue();
            if (req.isTimedOut(timeoutMs)) {
                req.fail(exp);
                cleanup(e.getKey());
                meterServerTimedOut.mark();
            }
        }
    }

    private String nextId() {
        return String.valueOf(ctr.incrementAndGet());
    }

    private ConcurrentLinkedQueue<OutstandingRequest> getQueue(String function) {
        if (function == null) {
            throw new IllegalArgumentException("The function for a request cannot be null");
        }
        ConcurrentLinkedQueue<OutstandingRequest> queue = queues.get(function);
        if (queue == null) {
            queues.putIfAbsent(function, new ConcurrentLinkedQueue<>());
            queue = queues.get(function);
        }
        return queue;
    }

    public void returnResult(String id, String result) throws AuthorizationException {
        meterResultCalls.mark();
        LOG.debug("Got a result {} {}", id, result);
        OutstandingRequest req = requests.get(id);
        if (req != null) {
            checkAuthorization("result", req.getFunction());
            req.returnResult(result);
        }
    }

    public DRPCRequest fetchRequest(String functionName) throws AuthorizationException {
        meterFetchRequestCalls.mark();
        checkAuthorizationNoLog("fetchRequest", functionName);
        ConcurrentLinkedQueue<OutstandingRequest> q = getQueue(functionName);
        OutstandingRequest req = q.poll();
        if (req != null) {
            //Only log accesses that fetched something
            logAccess("fetchRequest", functionName);
            req.fetched();
            DRPCRequest ret = req.getRequest();
            return ret;
        }
        return NOTHING_REQUEST;
    }

    public void failRequest(String id, DRPCExecutionException e) throws AuthorizationException {
        meterFailRequestCalls.mark();
        LOG.debug("Got a fail {}", id);
        OutstandingRequest req = requests.get(id);
        if (req != null) {
            checkAuthorization("failRequest", req.getFunction());
            if (e == null) {
                e = DEFAULT_FAILED;
            }
            req.fail(e);
        }
    }

    public <T extends OutstandingRequest> T execute(String functionName, String funcArgs, RequestFactory<T> factory) throws
        AuthorizationException {
        meterExecuteCalls.mark();
        checkAuthorization("execute", functionName);
        String id = nextId();
        LOG.debug("Execute {} {}", functionName, funcArgs);
        T req = factory.mkRequest(functionName, new DRPCRequest(funcArgs, id));
        requests.put(id, req);
        ConcurrentLinkedQueue<OutstandingRequest> q = getQueue(functionName);
        q.add(req);
        return req;
    }

    public String executeBlocking(String functionName, String funcArgs) throws DRPCExecutionException, AuthorizationException {
        BlockingOutstandingRequest req = execute(functionName, funcArgs, BlockingOutstandingRequest.FACTORY);
        try {
            LOG.debug("Waiting for result {} {}", functionName, funcArgs);
            return req.getResult();
        } catch (DRPCExecutionException e) {
            throw e;
        } finally {
            cleanup(req.getRequest().get_request_id());
        }
    }

    @Override
    public void close() {
        timer.cancel();
        cleanupAll(0, SHUT_DOWN);
    }
}
