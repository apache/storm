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
package org.apache.storm.daemon.drpc;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.Config;
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
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;

public class DRPC implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DRPC.class);
    private static final DRPCRequest NOTHING_REQUEST = new DRPCRequest("","");
    private static final DRPCExecutionException TIMED_OUT = new DRPCExecutionException("Timed Out");
    private static final DRPCExecutionException SHUT_DOWN = new DRPCExecutionException("Server Shutting Down");
    private static final DRPCExecutionException DEFAULT_FAILED = new DRPCExecutionException("Request failed");
    static {
        TIMED_OUT.set_type(DRPCExceptionType.SERVER_TIMEOUT);
        SHUT_DOWN.set_type(DRPCExceptionType.SERVER_SHUTDOWN);
        DEFAULT_FAILED.set_type(DRPCExceptionType.FAILED_REQUEST);
    }
    private static final Meter meterServerTimedOut = StormMetricsRegistry.registerMeter("drpc:num-server-timedout-requests");
    private static final Meter meterExecuteCalls = StormMetricsRegistry.registerMeter("drpc:num-execute-calls");
    private static final Meter meterResultCalls = StormMetricsRegistry.registerMeter("drpc:num-result-calls");
    private static final Meter meterFailRequestCalls = StormMetricsRegistry.registerMeter("drpc:num-failRequest-calls");
    private static final Meter meterFetchRequestCalls = StormMetricsRegistry.registerMeter("drpc:num-fetchRequest-calls");
    
    private static IAuthorizer mkAuthorizationHandler(String klassname, Map<String, Object> conf) {
        try {
            return StormCommon.mkAuthorizationHandler(klassname, conf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @VisibleForTesting
    static void checkAuthorization(ReqContext reqContext, IAuthorizer auth, String operation, String function) throws AuthorizationException {
        if (reqContext != null) {
            ThriftAccessLogger.logAccessFunction(reqContext.requestID(), reqContext.remoteAddress(), reqContext.principal(), operation, function);
        }
        if (auth != null) {
            Map<String, String> map = new HashMap<>();
            map.put(DRPCAuthorizerBase.FUNCTION_NAME, function);
            if (!auth.permit(reqContext, operation, map)) {
                Principal principal = reqContext.principal();
                String user = (principal != null) ? principal.getName() : "unknown";
                throw new AuthorizationException("DRPC request '" + operation + "' for '" + user + "' user is not authorized");
            }
        }
    }
    
    //Waiting to be fetched
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<OutstandingRequest>> _queues = 
            new ConcurrentHashMap<>();
    //Waiting to be returned
    private final ConcurrentHashMap<String, OutstandingRequest> _requests = 
            new ConcurrentHashMap<>();
    private final Timer _timer = new Timer();
    private final AtomicLong _ctr = new AtomicLong(0);
    private final IAuthorizer _auth;
    
    public DRPC(Map<String, Object> conf) {
        this(mkAuthorizationHandler((String)conf.get(Config.DRPC_AUTHORIZER), conf),
                Utils.getInt(conf.get(Config.DRPC_REQUEST_TIMEOUT_SECS), 600) * 1000);
    }
    
    public DRPC(IAuthorizer auth, long timeoutMs) {
        _auth = auth;
        _timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                cleanupAll(timeoutMs, TIMED_OUT);
            }
        }, timeoutMs/2, timeoutMs/2);
    }
    
    
    private void checkAuthorization(String operation, String function) throws AuthorizationException {
        checkAuthorization(ReqContext.context(), _auth, operation, function);
    }
    
    private void cleanup(String id) {
        OutstandingRequest req = _requests.remove(id);
        if (req != null && !req.wasFetched()) {
            _queues.get(req.getFunction()).remove(req);
        }
    }

    private void cleanupAll(long timeoutMs, DRPCExecutionException exp) {
        for (Entry<String, OutstandingRequest> e : _requests.entrySet()) {
            OutstandingRequest req = e.getValue();
            if (req.isTimedOut(timeoutMs)) {
                req.fail(exp);
                cleanup(e.getKey());
                meterServerTimedOut.mark();
            }
        }
    }
    
    private String nextId() {
        return String.valueOf(_ctr.incrementAndGet());
    }

    private ConcurrentLinkedQueue<OutstandingRequest> getQueue(String function) {
        ConcurrentLinkedQueue<OutstandingRequest> queue = _queues.get(function);
        if (queue == null) {
            _queues.putIfAbsent(function, new ConcurrentLinkedQueue<>());
            queue = _queues.get(function);
        }
        return queue;
    }

    public void returnResult(String id, String result) throws AuthorizationException {
        meterResultCalls.mark();
        LOG.debug("Got a result {} {}", id, result);
        OutstandingRequest req = _requests.get(id);
        if (req != null) {
            checkAuthorization("result", req.getFunction());
            req.returnResult(result);
        }
    }

    public DRPCRequest fetchRequest(String functionName) throws AuthorizationException {
        meterFetchRequestCalls.mark();
        checkAuthorization("fetchRequest", functionName);
        ConcurrentLinkedQueue<OutstandingRequest> q = getQueue(functionName);
        OutstandingRequest req = q.poll();
        if (req != null) {
            req.fetched();
            DRPCRequest ret = req.getRequest();
            return ret;
        }
        return NOTHING_REQUEST;
    }

    public void failRequest(String id, DRPCExecutionException e) throws AuthorizationException {
        meterFailRequestCalls.mark();
        LOG.debug("Got a fail {}", id);
        OutstandingRequest req = _requests.get(id);
        if (req != null) {
            checkAuthorization("failRequest", req.getFunction());
            if (e == null) {
                e = DEFAULT_FAILED;
            }
            req.fail(e);
        }
    }

    public <T extends OutstandingRequest> T execute(String functionName, String funcArgs, RequestFactory<T> factory) throws AuthorizationException {
        meterExecuteCalls.mark();
        checkAuthorization("execute", functionName);
        String id = nextId();
        LOG.debug("Execute {} {}", functionName, funcArgs);
        T req = factory.mkRequest(functionName, new DRPCRequest(funcArgs, id));
        _requests.put(id, req);
        ConcurrentLinkedQueue<OutstandingRequest> q = getQueue(functionName);
        q.add(req);
        return req;
    }
    
    public String executeBlocking(String functionName, String funcArgs) throws DRPCExecutionException, AuthorizationException {
        BlockingOutstandingRequest req = execute(functionName, funcArgs, BlockingOutstandingRequest.FACTORY);
        try {
            LOG.debug("Waiting for result {} {}",functionName, funcArgs);
            return req.getResult();
        } catch (DRPCExecutionException e) {
            throw e;
        } finally {
            cleanup(req.getRequest().get_request_id());
        }
    }

    @Override
    public void close() {
        _timer.cancel();
        cleanupAll(0, SHUT_DOWN);
    }
}
