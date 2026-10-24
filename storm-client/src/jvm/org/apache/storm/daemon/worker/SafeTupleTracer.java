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

package org.apache.storm.daemon.worker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tracing.TupleTracer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps the worker's tracer so that its failures are handled as the {@link TupleTracer} contract describes. Each method logs
 * its failures at most once per {@link #LOG_INTERVAL_NANOS}, with the number it did not log since its previous warning.
 */
class SafeTupleTracer implements TupleTracer {
    static final long LOG_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
    private static final Logger LOG = LoggerFactory.getLogger(SafeTupleTracer.class);

    private final TupleTracer delegate;
    private final String tracerName;
    private final FailureLog spoutEmitFailures = new FailureLog("spoutEmit");
    private final FailureLog boltEmitFailures = new FailureLog("boltEmit");
    private final FailureLog startExecuteFailures = new FailureLog("startExecute");
    private final FailureLog spoutOutcomeFailures = new FailureLog("spoutOutcome");
    private final FailureLog boltFailFailures = new FailureLog("boltFail");
    private final FailureLog encodeFailures = new FailureLog("encode");
    private final FailureLog decodeFailures = new FailureLog("decode");
    private final FailureLog contextFailures = new FailureLog("ExecuteScope.context");
    private final FailureLog closeFailures = new FailureLog("ExecuteScope.close");

    SafeTupleTracer(TupleTracer delegate) {
        this.delegate = delegate;
        this.tracerName = delegate.getClass().getName();
    }

    @Override
    public void prepare(Map<String, Object> topoConf, WorkerTopologyContext context) {
        delegate.prepare(topoConf, context);
    }

    @Override
    public Object spoutEmit(int taskId, String streamId) {
        try {
            return delegate.spoutEmit(taskId, streamId);
        } catch (RuntimeException | LinkageError e) {
            spoutEmitFailures.log(e);
            return null;
        }
    }

    @Override
    public Object boltEmit(int taskId, String streamId, List<Object> anchorContexts) {
        try {
            return delegate.boltEmit(taskId, streamId, anchorContexts);
        } catch (RuntimeException | LinkageError e) {
            boltEmitFailures.log(e);
            return null;
        }
    }

    @Override
    public ExecuteScope startExecute(int taskId, Tuple tuple, Object context) {
        ExecuteScope scope;
        try {
            scope = delegate.startExecute(taskId, tuple, context);
        } catch (RuntimeException | LinkageError e) {
            startExecuteFailures.log(e);
            return null;
        }
        return scope == null ? null : new SafeScope(scope, context);
    }

    @Override
    public void spoutOutcome(int taskId, Object context, Outcome outcome, long latencyMs) {
        try {
            delegate.spoutOutcome(taskId, context, outcome, latencyMs);
        } catch (RuntimeException | LinkageError e) {
            spoutOutcomeFailures.log(e);
        }
    }

    @Override
    public void boltFail(int taskId, Object context) {
        try {
            delegate.boltFail(taskId, context);
        } catch (RuntimeException | LinkageError e) {
            boltFailFailures.log(e);
        }
    }

    @Override
    public byte[] encode(Object context) {
        try {
            return delegate.encode(context);
        } catch (RuntimeException | LinkageError e) {
            encodeFailures.log(e);
            return null;
        }
    }

    @Override
    public Object decode(byte[] bytes) {
        try {
            return delegate.decode(bytes);
        } catch (RuntimeException | LinkageError e) {
            decodeFailures.log(e);
            return null;
        }
    }

    private class SafeScope implements ExecuteScope {
        private final ExecuteScope scope;
        private final Object received;

        SafeScope(ExecuteScope scope, Object received) {
            this.scope = scope;
            this.received = received;
        }

        @Override
        public Object context() {
            try {
                return scope.context();
            } catch (RuntimeException | LinkageError e) {
                contextFailures.log(e);
                return received;
            }
        }

        @Override
        public void close() {
            try {
                scope.close();
            } catch (RuntimeException | LinkageError e) {
                closeFailures.log(e);
            }
        }
    }

    private class FailureLog {
        private final String method;
        private final AtomicLong lastLogNanos = new AtomicLong(Time.nanoTime() - LOG_INTERVAL_NANOS);
        private final LongAdder unlogged = new LongAdder();

        FailureLog(String method) {
            this.method = method;
        }

        void log(Throwable failure) {
            long now = Time.nanoTime();
            long last = lastLogNanos.get();
            if (now - last < LOG_INTERVAL_NANOS || !lastLogNanos.compareAndSet(last, now)) {
                unlogged.increment();
                return;
            }
            long skipped = unlogged.sumThenReset();
            if (skipped == 0) {
                LOG.warn("Ignoring an exception from {}() of tracer {}", method, tracerName, failure);
            } else {
                LOG.warn("Ignoring an exception from {}() of tracer {}; {} more were not logged since the previous warning",
                    method, tracerName, skipped, failure);
            }
        }
    }
}
