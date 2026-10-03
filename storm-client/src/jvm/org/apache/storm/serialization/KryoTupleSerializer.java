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

package org.apache.storm.serialization;

import com.esotericsoftware.kryo.io.Output;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.storm.Config;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;

public class KryoTupleSerializer implements ITupleSerializer {
    private static final int DEFAULT_COMPRESSION_THRESHOLD = 1460;
    private static final Integer DEFAULT_ZSTD_COMPRESSION_LEVEL = 3;
    /** Below 0x80, the bit HAS_TRACE_STATE takes; bump it when the layout changes. */
    static final int TRACE_CONTEXT_VERSION = 1;
    /** Header bit: a W3C tracestate follows the trace flags. */
    static final int HAS_TRACE_STATE = 0x80;

    private final KryoValuesSerializer kryo;
    private final SerializationFactory.IdDictionary ids;
    private final Output kryoOut;
    private final boolean isCompressionEnabled;
    private final int compressionThreshold;
    private final int zstdCompressionLevel;

    public KryoTupleSerializer(final Map<String, Object> conf, final GeneralTopologyContext context) {
        kryo = new KryoValuesSerializer(conf);
        kryoOut = new Output(2000, 2000000000);
        ids = new SerializationFactory.IdDictionary(context.getRawTopology());
        isCompressionEnabled = ObjectReader.getBoolean(conf.get(Config.TOPOLOGY_TUPLE_COMPRESSION_ENABLE), false);
        compressionThreshold = ObjectReader.getInt(conf.get(Config.TOPOLOGY_TUPLE_COMPRESSION_THRESHOLD), DEFAULT_COMPRESSION_THRESHOLD);
        zstdCompressionLevel = ObjectReader.getInt(conf.get(Config.STORM_COMPRESSION_ZSTD_LEVEL), DEFAULT_ZSTD_COMPRESSION_LEVEL);
    }

    @Override
    public byte[] serialize(Tuple tuple) {
        try {

            kryoOut.reset();
            kryoOut.writeInt(tuple.getSourceTask(), true);
            kryoOut.writeInt(ids.getStreamId(tuple.getSourceComponent(), tuple.getSourceStreamId()), true);
            tuple.getMessageId().serialize(kryoOut);
            kryo.serializeInto(tuple.getValues(), kryoOut);
            if (tuple instanceof TupleImpl impl) {
                writeTraceContext(kryoOut, impl.getTraceContext());
            }

            byte[] rawBytes = kryoOut.getBuffer();
            int dataLength = kryoOut.position();

            if (this.isCompressionEnabled && dataLength > this.compressionThreshold) {
                return Utils.ZstdUtils.compress(rawBytes, 0, dataLength, this.zstdCompressionLevel);
            } else {
                return Arrays.copyOf(rawBytes, dataLength);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Appends the trace context after the values: header byte (version, tracestate bit), 16-byte
     * trace id, 8-byte span id, trace flags byte, then the tracestate if not empty. Readers that
     * stop after the values ignore these bytes.
     */
    private static void writeTraceContext(Output out, Context traceContext) {
        if (traceContext == null) {
            return;
        }
        SpanContext span = Span.fromContext(traceContext).getSpanContext();
        // isValid() ignores the sampled flag: unsampled contexts propagate too
        if (!span.isValid()) {
            return;
        }
        TraceState traceState = span.getTraceState();
        out.writeByte(TRACE_CONTEXT_VERSION | (traceState.isEmpty() ? 0 : HAS_TRACE_STATE));
        out.writeBytes(span.getTraceIdBytes());
        out.writeBytes(span.getSpanIdBytes());
        out.writeByte(span.getTraceFlags().asByte());
        if (!traceState.isEmpty()) {
            StringJoiner entries = new StringJoiner(",");
            traceState.forEach((key, value) -> entries.add(key + '=' + value));
            out.writeString(entries.toString());
        }
    }
}
