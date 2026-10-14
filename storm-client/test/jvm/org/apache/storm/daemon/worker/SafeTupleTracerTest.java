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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tracing.TupleTracer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SafeTupleTracerTest {

    /** A runtime exception, and a linkage error such as a tracer built against another version of a library throws. */
    private static final List<Function<String, Throwable>> CONTAINED_FAILURES =
        List.of(IllegalStateException::new, NoClassDefFoundError::new);

    @Test
    public void testFailuresAreContained() {
        for (Function<String, Throwable> failure : CONTAINED_FAILURES) {
            SafeTupleTracer tracer = new SafeTupleTracer(new FailingTracer(failure));

            assertNull(tracer.spoutEmit(1, "stream"));
            assertNull(tracer.boltEmit(1, "stream", List.of("anchor")));
            assertNull(tracer.startExecute(1, null, "received"));
            assertNull(tracer.encode("context"));
            assertNull(tracer.decode(new byte[]{1}));
            assertDoesNotThrow(() -> tracer.spoutOutcome(1, "context", TupleTracer.Outcome.ACK, 5));
            assertDoesNotThrow(() -> tracer.boltFail(1, "context"));

            FailingScopeTracer scopeTracer = new FailingScopeTracer(failure);
            TupleTracer.ExecuteScope scope = new SafeTupleTracer(scopeTracer).startExecute(1, null, "received");
            assertEquals("received", scope.context());
            assertDoesNotThrow(scope::close);
            assertEquals(1, scopeTracer.closes.get());
        }
    }

    @Test
    public void testPrepareFailuresAndOtherErrorsEscape() {
        assertThrows(IllegalStateException.class,
            () -> new SafeTupleTracer(new FailingTracer(IllegalStateException::new)).prepare(Map.of(), null));
        assertThrows(OutOfMemoryError.class,
            () -> new SafeTupleTracer(new FailingTracer(OutOfMemoryError::new)).spoutEmit(1, "stream"));
    }

    @Test
    public void testEachMethodLogsAtMostOncePerInterval() {
        String tracerName = FailingScopeTracer.class.getName();
        List<LogEvent> warnings = warningsDuring(() -> {
            try (SimulatedTime ignored = new SimulatedTime()) {
                SafeTupleTracer tracer = new SafeTupleTracer(new FailingScopeTracer(IllegalStateException::new));
                tracer.encode("context");
                tracer.encode("context");
                tracer.decode(new byte[]{1});
                TupleTracer.ExecuteScope scope = tracer.startExecute(1, null, "received");
                scope.context();
                scope.close();
                Time.advanceTimeNanos(SafeTupleTracer.LOG_INTERVAL_NANOS - 1);
                tracer.encode("context");
                Time.advanceTimeNanos(1);
                tracer.encode("context");
            }
        });

        assertEquals(List.of(
                "Ignoring an exception from encode() of tracer " + tracerName,
                "Ignoring an exception from decode() of tracer " + tracerName,
                "Ignoring an exception from ExecuteScope.context() of tracer " + tracerName,
                "Ignoring an exception from ExecuteScope.close() of tracer " + tracerName,
                "Ignoring an exception from encode() of tracer " + tracerName
                    + "; 2 more were not logged since the previous warning"),
            messages(warnings));
        assertEquals(List.of("encode", "decode", "context", "close", "encode"),
            warnings.stream().map(w -> w.getThrown().getMessage()).collect(Collectors.toList()));
    }

    private static List<LogEvent> warningsDuring(Runnable action) {
        List<LogEvent> warnings = new CopyOnWriteArrayList<>();
        String loggerName = SafeTupleTracer.class.getName();
        LoggerContext logContext = LoggerContext.getContext(false);
        LoggerConfig loggerConfig = new LoggerConfig(loggerName, Level.WARN, false);
        Appender appender = new AbstractAppender("warnings", null, null, true, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                warnings.add(event.toImmutable());
            }
        };
        appender.start();
        loggerConfig.addAppender(appender, null, null);
        logContext.getConfiguration().addLogger(loggerName, loggerConfig);
        logContext.updateLoggers();
        try {
            action.run();
        } finally {
            logContext.getConfiguration().removeLogger(loggerName);
            logContext.updateLoggers();
        }
        return warnings;
    }

    private static List<String> messages(List<LogEvent> events) {
        return events.stream().map(e -> e.getMessage().getFormattedMessage()).collect(Collectors.toList());
    }

    private static class FailingTracer implements TupleTracer {
        private final Function<String, Throwable> failure;

        FailingTracer(Function<String, Throwable> failure) {
            this.failure = failure;
        }

        /** Throws the failure for {@code method} if it is an error, otherwise returns it for the caller to throw. */
        RuntimeException fail(String method) {
            Throwable thrown = failure.apply(method);
            if (thrown instanceof Error error) {
                throw error;
            }
            return (RuntimeException) thrown;
        }

        @Override
        public void prepare(Map<String, Object> topoConf, WorkerTopologyContext context) {
            throw fail("prepare");
        }

        @Override
        public Object spoutEmit(int taskId, String streamId) {
            throw fail("spoutEmit");
        }

        @Override
        public Object boltEmit(int taskId, String streamId, List<Object> anchorContexts) {
            throw fail("boltEmit");
        }

        @Override
        public ExecuteScope startExecute(int taskId, Tuple tuple, Object context) {
            throw fail("startExecute");
        }

        @Override
        public void spoutOutcome(int taskId, Object context, Outcome outcome, long latencyMs) {
            throw fail("spoutOutcome");
        }

        @Override
        public void boltFail(int taskId, Object context) {
            throw fail("boltFail");
        }

        @Override
        public byte[] encode(Object context) {
            throw fail("encode");
        }

        @Override
        public Object decode(byte[] bytes) {
            throw fail("decode");
        }
    }

    private static class FailingScopeTracer extends FailingTracer {
        final AtomicInteger closes = new AtomicInteger();

        FailingScopeTracer(Function<String, Throwable> failure) {
            super(failure);
        }

        @Override
        public ExecuteScope startExecute(int taskId, Tuple tuple, Object context) {
            return new ExecuteScope() {
                @Override
                public Object context() {
                    throw fail("context");
                }

                @Override
                public void close() {
                    closes.incrementAndGet();
                    throw fail("close");
                }
            };
        }
    }
}
