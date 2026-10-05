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
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TraceContextCodecTest {

    private static final String TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
    private static final String SPAN_ID = "b7ad6b7169203331";

    @Test
    public void testRoundTrip() {
        TraceState twoEntries = TraceState.builder().put("vendor", "v1").put("ot", "th:8;rv:0123456789abcd").build();
        // 0x03 = sampled plus the W3C random-trace-id bit; 0x00 = not sampled, which must propagate too
        List<SpanContext> sent = List.of(
            spanContext((byte) 0x03, TraceState.getDefault()),
            spanContext((byte) 0x03, twoEntries),
            spanContext((byte) 0x00, TraceState.getDefault()));

        for (SpanContext span : sent) {
            SpanContext received = Span.fromContext(TraceContextCodec.decode(TraceContextCodec.encode(context(span))))
                .getSpanContext();
            assertEquals(span.getTraceId(), received.getTraceId());
            assertEquals(span.getSpanId(), received.getSpanId());
            assertEquals(span.getTraceFlags(), received.getTraceFlags());
            assertEquals(span.getTraceState(), received.getTraceState());
            assertTrue(received.isRemote());
        }
    }

    @Test
    public void testContextWithoutValidSpanIsNotEncoded() {
        assertNull(TraceContextCodec.encode(Context.root()));
    }

    @Test
    public void testTraceStateOverTheLimitIsNotSent() {
        // a value holds at most 256 characters, so three entries are needed to pass 512
        TraceState large = TraceState.builder().put("a", "x".repeat(200)).put("b", "x".repeat(200)).put("c", "x".repeat(200)).build();
        assertEquals(3, large.size());

        SpanContext received = Span.fromContext(
            TraceContextCodec.decode(TraceContextCodec.encode(context(spanContext((byte) 0x01, large))))).getSpanContext();
        assertEquals(TRACE_ID, received.getTraceId());
        assertTrue(received.getTraceState().isEmpty());
    }

    @Test
    public void testTraceStateOverTheLimitReadsAsAbsent() {
        byte[] ids = TraceContextCodec.encode(context(spanContext((byte) 0x01, TraceState.getDefault())));
        String entry = "x".repeat(200);
        byte[] state = ("a=" + entry + ",b=" + entry + ",c=" + entry).getBytes(StandardCharsets.US_ASCII);
        byte[] bytes = Arrays.copyOf(ids, ids.length + state.length);
        System.arraycopy(state, 0, bytes, ids.length, state.length);

        assertNull(TraceContextCodec.decode(bytes));
    }

    @Test
    public void testShortPayloadReadsAsAbsent() {
        byte[] bytes = TraceContextCodec.encode(context(spanContext((byte) 0x01, TraceState.getDefault())));

        for (int length = 0; length < bytes.length; length++) {
            assertNull(TraceContextCodec.decode(Arrays.copyOf(bytes, length)), "length " + length);
        }
    }

    @Test
    public void testUnknownVersionReadsAsAbsent() {
        byte[] bytes = TraceContextCodec.encode(context(spanContext((byte) 0x01, TraceState.getDefault())));
        bytes[0] = 2;

        assertNull(TraceContextCodec.decode(bytes));
    }

    @Test
    public void testInvalidIdsReadAsAbsent() {
        byte[] bytes = TraceContextCodec.encode(context(spanContext((byte) 0x01, TraceState.getDefault())));
        Arrays.fill(bytes, 1, 17, (byte) 0); // all-zero trace id

        assertNull(TraceContextCodec.decode(bytes));
    }

    private static SpanContext spanContext(byte flags, TraceState traceState) {
        return SpanContext.create(TRACE_ID, SPAN_ID, TraceFlags.fromByte(flags), traceState);
    }

    private static Context context(SpanContext span) {
        return Context.root().with(Span.wrap(span));
    }
}
