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

package org.apache.storm.opentelemetry;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.storm.Config;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tracing.TupleTracer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Traces tuple trees with OpenTelemetry. To use it, set {@link Config#TOPOLOGY_TRACING_TRACER} to this class name. Spans go to
 * the SDK registered as the global OpenTelemetry instance, usually by the OpenTelemetry Java agent, version 2.23.0 or later. Until
 * one is registered, tuples are not traced and one warning is logged.
 *
 * <p>Each spout emit starts a trace with a root span named "component emit", started and ended at once. Each bolt
 * {@code execute()} of a traced tuple runs in a child span named "component execute", current while {@code execute()} runs. A bolt
 * emit carries the context of its anchors: the anchor's context if they share one span. Otherwise it carries the context of a span
 * named "component emit", started and ended at once: a child of the first anchor's span linked to the others if all anchors belong
 * to one trace, else a new root span linked to each.
 *
 * <p>The spout records the ack, fail or timeout of each traced tuple tree as a span whose duration, also set as the
 * {@code storm.tuple.latency_ms} attribute, is the time from the emit to the outcome. A bolt records each {@code fail()} as a span
 * started and ended at once. Contexts with the sampled flag unset are propagated too.
 */
public class OpenTelemetryTupleTracer implements TupleTracer {

    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryTupleTracer.class);
    private static final String INSTRUMENTATION_SCOPE = "org.apache.storm";
    private static final long SDK_LOOKUP_INTERVAL_MS = 1000;
    private static final long UNTRACED_EMIT_LOG_INTERVAL_MS = 60_000;

    private static final AttributeKey<String> TOPOLOGY_NAME_KEY = AttributeKey.stringKey("storm.topology.name");
    private static final AttributeKey<String> TOPOLOGY_ID_KEY = AttributeKey.stringKey("storm.topology.id");
    private static final AttributeKey<String> COMPONENT_ID_KEY = AttributeKey.stringKey("storm.component.id");
    private static final AttributeKey<Long> TASK_ID_KEY = AttributeKey.longKey("storm.task.id");
    private static final AttributeKey<String> SOURCE_COMPONENT_ID_KEY = AttributeKey.stringKey("storm.source.component.id");
    private static final AttributeKey<String> SOURCE_STREAM_ID_KEY = AttributeKey.stringKey("storm.source.stream.id");
    private static final AttributeKey<String> WORKER_HOST_KEY = AttributeKey.stringKey("storm.worker.host");
    private static final AttributeKey<Long> WORKER_PORT_KEY = AttributeKey.longKey("storm.worker.port");
    private static final AttributeKey<Long> TUPLE_LATENCY_KEY = AttributeKey.longKey("storm.tuple.latency_ms");

    private WorkerTopologyContext context;
    private Attributes workerAttributes;
    /** Indexed by task id; null for the tasks of other workers and for system tasks with a negative id. */
    private TaskSpans[] taskSpans;
    private volatile Tracer tracer;
    private volatile long nextSdkLookupMs;
    private final AtomicBoolean warnedNoSdk = new AtomicBoolean();

    @Override
    public void prepare(Map<String, Object> topoConf, WorkerTopologyContext context) {
        this.context = context;
        AttributesBuilder attributes = Attributes.builder()
            .put(TOPOLOGY_NAME_KEY, (String) topoConf.get(Config.TOPOLOGY_NAME))
            .put(TOPOLOGY_ID_KEY, context.getStormId())
            .put(WORKER_PORT_KEY, context.getThisWorkerPort().longValue());
        try {
            attributes.put(WORKER_HOST_KEY, Utils.hostname());
        } catch (UnknownHostException e) {
            LOG.warn("Execute spans get no {} attribute: the host name is unknown", WORKER_HOST_KEY, e);
        }
        this.workerAttributes = attributes.build();
        int maxTaskId = context.getThisWorkerTasks().stream().mapToInt(Integer::intValue).max().orElse(-1);
        TaskSpans[] byTask = new TaskSpans[maxTaskId + 1];
        for (int taskId : context.getThisWorkerTasks()) {
            if (taskId >= 0) {
                byTask[taskId] = newTaskSpans(taskId);
            }
        }
        this.taskSpans = byTask;
    }

    @Override
    public Object spoutEmit(int taskId, String streamId) {
        Tracer current = tracer();
        return current == null ? null : emitContext(current.spanBuilder(spans(taskId).emitName()).setNoParent());
    }

    @Override
    public Object boltEmit(int taskId, String streamId, List<Object> anchorContexts) {
        if (anchorContexts.isEmpty()) {
            logUntracedEmitUnderSpan(taskId, streamId);
            return null;
        }
        Context first = (Context) anchorContexts.get(0);
        if (anchorContexts.size() == 1) {
            return first;
        }
        SpanContext firstSpan = Span.fromContext(first).getSpanContext();
        Set<SpanContext> otherSpans = new LinkedHashSet<>();
        for (Object anchorContext : anchorContexts) {
            otherSpans.add(Span.fromContext((Context) anchorContext).getSpanContext());
        }
        otherSpans.remove(firstSpan);
        if (otherSpans.isEmpty()) {
            return first;
        }
        Tracer current = tracer();
        if (current == null) {
            return null;
        }
        // the SDK keeps up to 128 links by default
        SpanBuilder builder = current.spanBuilder(spans(taskId).emitName());
        if (otherSpans.stream().allMatch(span -> span.getTraceId().equals(firstSpan.getTraceId()))) {
            builder.setParent(first);
        } else {
            builder.setNoParent().addLink(firstSpan);
        }
        otherSpans.forEach(builder::addLink);
        return emitContext(builder);
    }

    @Override
    public ExecuteScope startExecute(int taskId, Tuple tuple, Object context) {
        Tracer current = tracer();
        if (current == null) {
            return null;
        }
        Context received = (Context) context;
        TaskSpans spans = spans(taskId);
        Span span = current.spanBuilder(spans.executeName()).setParent(received).startSpan();
        if (span.isRecording()) {
            span.setAllAttributes(spans.executeAttributes());
            span.setAttribute(SOURCE_COMPONENT_ID_KEY, tuple.getSourceComponent());
            span.setAttribute(SOURCE_STREAM_ID_KEY, tuple.getSourceStreamId());
        }
        // the tuple keeps the span ids only: it may outlive the span
        Context executeContext = received.with(Span.wrap(span.getSpanContext()));
        return new ExecuteSpan(span, span.makeCurrent(), executeContext);
    }

    @Override
    public void spoutOutcome(int taskId, Object context, Outcome outcome, long latencyMs) {
        Tracer current = tracer();
        if (current == null) {
            return;
        }
        TaskSpans spans = spans(taskId);
        String spanName = switch (outcome) {
            case ACK -> spans.ackName();
            case FAIL -> spans.failName();
            case TIMEOUT -> spans.timeoutName();
        };
        Instant end = Instant.now();
        Span span = current.spanBuilder(spanName)
            .setParent((Context) context)
            .setStartTimestamp(end.minusMillis(latencyMs))
            .setAttribute(TUPLE_LATENCY_KEY, latencyMs)
            .startSpan();
        if (outcome != Outcome.ACK) {
            span.setStatus(StatusCode.ERROR);
        }
        span.end(end);
    }

    @Override
    public void boltFail(int taskId, Object context) {
        Tracer current = tracer();
        if (current == null) {
            return;
        }
        current.spanBuilder(spans(taskId).failName()).setParent((Context) context).startSpan()
            .setStatus(StatusCode.ERROR)
            .end();
    }

    @Override
    public byte[] encode(Object context) {
        return TraceContextCodec.encode((Context) context);
    }

    @Override
    public Object decode(byte[] bytes) {
        return TraceContextCodec.decode(bytes);
    }

    /**
     * Returns the tracer, or null until an SDK is registered as the global instance. Checks at most once a second, with isSet()
     * rather than get() so that an SDK registered later is still used.
     */
    private Tracer tracer() {
        Tracer current = tracer;
        if (current != null) {
            return current;
        }
        long now = Time.currentTimeMillis();
        if (now < nextSdkLookupMs) {
            return null;
        }
        nextSdkLookupMs = now + SDK_LOOKUP_INTERVAL_MS;
        if (GlobalOpenTelemetry.isSet()) {
            current = GlobalOpenTelemetry.get().getTracer(INSTRUMENTATION_SCOPE);
            tracer = current;
            return current;
        }
        if (warnedNoSdk.compareAndSet(false, true)) {
            LOG.warn("{} is configured but no OpenTelemetry SDK is registered as the global instance, so tuples are not traced"
                + " until one is. With the OpenTelemetry Java agent, this needs version 2.23.0 or later.", getClass().getName());
        }
        return null;
    }

    /**
     * Starts and immediately ends the emit span, and returns its context, or null when the span is not valid. The context keeps the
     * span ids only: pending tuples hold it until their tree completes.
     */
    private static Context emitContext(SpanBuilder builder) {
        Span span = builder.startSpan();
        span.end();
        SpanContext ids = span.getSpanContext();
        return ids.isValid() ? Context.root().with(Span.wrap(ids)) : null;
    }

    private void logUntracedEmitUnderSpan(int taskId, String streamId) {
        if (!LOG.isDebugEnabled() || !Span.current().getSpanContext().isValid()) {
            return;
        }
        TaskSpans spans = spans(taskId);
        AtomicLong lastLogMs = spans.lastUntracedEmitLogMs();
        long last = lastLogMs.get();
        long now = Time.currentTimeMillis();
        if (now - last < UNTRACED_EMIT_LOG_INTERVAL_MS || !lastLogMs.compareAndSet(last, now)) {
            return;
        }
        LOG.debug("{} emitted on stream {} without a traced anchor while a span was current; the emitted tuple carries no"
            + " trace context", spans.component(), streamId);
    }

    private TaskSpans spans(int taskId) {
        TaskSpans[] byTask = taskSpans;
        TaskSpans spans = taskId >= 0 && taskId < byTask.length ? byTask[taskId] : null;
        return spans != null ? spans : newTaskSpans(taskId);
    }

    private TaskSpans newTaskSpans(int taskId) {
        String component = context.getComponentId(taskId);
        Attributes executeAttributes = workerAttributes.toBuilder()
            .put(COMPONENT_ID_KEY, component)
            .put(TASK_ID_KEY, (long) taskId)
            .build();
        return new TaskSpans(component, component + " emit", component + " execute", component + " ack", component + " fail",
            component + " timeout", executeAttributes, new AtomicLong());
    }

    /** Span names, execute span attributes and the time of the last untraced emit log line of one task. */
    private record TaskSpans(String component, String emitName, String executeName, String ackName, String failName,
                             String timeoutName, Attributes executeAttributes, AtomicLong lastUntracedEmitLogMs) {
    }

    private record ExecuteSpan(Span span, Scope scope, Context context) implements ExecuteScope {
        @Override
        public void close() {
            scope.close();
            span.end();
        }
    }
}
