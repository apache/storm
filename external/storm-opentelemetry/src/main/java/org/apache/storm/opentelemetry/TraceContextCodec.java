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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.TraceStateBuilder;
import io.opentelemetry.context.Context;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.StringJoiner;

/**
 * Binary form of a W3C trace context: a version byte, the 16-byte trace id, the 8-byte span id, the trace flags byte, then the
 * tracestate in its W3C header form, if not empty.
 */
final class TraceContextCodec {

    /** Bump when the layout changes; readers return no context for other versions. */
    static final int VERSION = 1;
    /** The tracestate length W3C asks vendors to propagate at least. Longer ones are not sent. */
    static final int MAX_TRACE_STATE_LENGTH = 512;

    private static final int TRACE_ID_BYTES = 16;
    private static final int SPAN_ID_BYTES = 8;
    private static final int TRACE_STATE_OFFSET = 1 + TRACE_ID_BYTES + SPAN_ID_BYTES + 1;

    private TraceContextCodec() {
    }

    /**
     * Returns the bytes of the span context in {@code context}, or null when it holds no valid span context. Unsampled span
     * contexts are encoded too.
     */
    static byte[] encode(Context context) {
        SpanContext span = Span.fromContext(context).getSpanContext();
        if (!span.isValid()) {
            return null;
        }
        byte[] traceState = traceState(span.getTraceState());
        byte[] bytes = new byte[TRACE_STATE_OFFSET + traceState.length];
        bytes[0] = VERSION;
        System.arraycopy(span.getTraceIdBytes(), 0, bytes, 1, TRACE_ID_BYTES);
        System.arraycopy(span.getSpanIdBytes(), 0, bytes, 1 + TRACE_ID_BYTES, SPAN_ID_BYTES);
        bytes[TRACE_STATE_OFFSET - 1] = span.getTraceFlags().asByte();
        System.arraycopy(traceState, 0, bytes, TRACE_STATE_OFFSET, traceState.length);
        return bytes;
    }

    /**
     * Returns a context with the remote span context in {@code bytes}, or null when they are too short, of another version, carry
     * invalid ids or a tracestate over {@link #MAX_TRACE_STATE_LENGTH}. Invalid tracestate entries are dropped.
     */
    static Context decode(byte[] bytes) {
        int traceStateLength = bytes.length - TRACE_STATE_OFFSET;
        if (traceStateLength < 0 || traceStateLength > MAX_TRACE_STATE_LENGTH || bytes[0] != VERSION) {
            return null;
        }
        String traceId = TraceId.fromBytes(Arrays.copyOfRange(bytes, 1, 1 + TRACE_ID_BYTES));
        String spanId = SpanId.fromBytes(Arrays.copyOfRange(bytes, 1 + TRACE_ID_BYTES, TRACE_STATE_OFFSET - 1));
        TraceFlags flags = TraceFlags.fromByte(bytes[TRACE_STATE_OFFSET - 1]);
        TraceState traceState = traceStateLength == 0
            ? TraceState.getDefault()
            : parseTraceState(new String(bytes, TRACE_STATE_OFFSET, traceStateLength, StandardCharsets.US_ASCII));
        SpanContext span = SpanContext.createFromRemoteParent(traceId, spanId, flags, traceState);
        return span.isValid() ? Context.root().with(Span.wrap(span)) : null;
    }

    private static byte[] traceState(TraceState traceState) {
        if (traceState.isEmpty()) {
            return new byte[0];
        }
        StringJoiner entries = new StringJoiner(",");
        traceState.forEach((key, value) -> entries.add(key + '=' + value));
        byte[] bytes = entries.toString().getBytes(StandardCharsets.US_ASCII);
        return bytes.length > MAX_TRACE_STATE_LENGTH ? new byte[0] : bytes;
    }

    private static TraceState parseTraceState(String header) {
        String[] entries = header.split(",");
        TraceStateBuilder builder = TraceState.builder();
        // put() inserts in front of existing entries: add in reverse to keep the order
        for (int i = entries.length - 1; i >= 0; i--) {
            int separator = entries[i].indexOf('=');
            if (separator > 0) {
                builder.put(entries[i].substring(0, separator), entries[i].substring(separator + 1));
            }
        }
        return builder.build();
    }
}
