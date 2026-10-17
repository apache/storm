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
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.tracing.TupleTracer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;

public class KryoTupleSerializer implements ITupleSerializer {
    private static final int DEFAULT_COMPRESSION_THRESHOLD = 1460;
    private static final Integer DEFAULT_ZSTD_COMPRESSION_LEVEL = 3;
    /** Tag of the entry that carries the trace context, see {@link #writeTraceContext}. */
    static final int TRACE_CONTEXT_TAG = 1;

    private final KryoValuesSerializer kryo;
    private final SerializationFactory.IdDictionary ids;
    private final Output kryoOut;
    private final boolean isCompressionEnabled;
    private final int compressionThreshold;
    private final int zstdCompressionLevel;
    private final TupleTracer tracer;

    public KryoTupleSerializer(final Map<String, Object> conf, final GeneralTopologyContext context) {
        this(conf, context, null);
    }

    /**
     * Creates a serializer that writes the trace context of each tuple, as encoded by {@code tracer}; a null tracer writes none.
     */
    public KryoTupleSerializer(final Map<String, Object> conf, final GeneralTopologyContext context, final TupleTracer tracer) {
        kryo = new KryoValuesSerializer(conf);
        kryoOut = new Output(2000, 2000000000);
        ids = new SerializationFactory.IdDictionary(context.getRawTopology());
        isCompressionEnabled = ObjectReader.getBoolean(conf.get(Config.TOPOLOGY_TUPLE_COMPRESSION_ENABLE), false);
        compressionThreshold = ObjectReader.getInt(conf.get(Config.TOPOLOGY_TUPLE_COMPRESSION_THRESHOLD), DEFAULT_COMPRESSION_THRESHOLD);
        zstdCompressionLevel = ObjectReader.getInt(conf.get(Config.STORM_COMPRESSION_ZSTD_LEVEL), DEFAULT_ZSTD_COMPRESSION_LEVEL);
        this.tracer = tracer;
    }

    @Override
    public byte[] serialize(Tuple tuple) {
        try {

            kryoOut.reset();
            kryoOut.writeInt(tuple.getSourceTask(), true);
            kryoOut.writeInt(ids.getStreamId(tuple.getSourceComponent(), tuple.getSourceStreamId()), true);
            tuple.getMessageId().serialize(kryoOut);
            kryo.serializeInto(tuple.getValues(), kryoOut);
            Object traceContext = tracer != null && tuple instanceof TupleImpl impl ? impl.getTraceContext() : null;
            if (traceContext != null) {
                writeTraceContext(traceContext);
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
     * Appends an entry after the values: a tag byte, the payload length as a varint, then the payload. Readers skip entries with
     * an unknown tag, and readers that stop after the values ignore all of them.
     */
    private void writeTraceContext(Object traceContext) {
        byte[] payload = tracer.encode(traceContext);
        if (payload == null) {
            return;
        }
        kryoOut.writeByte(TRACE_CONTEXT_TAG);
        kryoOut.writeVarInt(payload.length, true);
        kryoOut.writeBytes(payload);
    }
}
