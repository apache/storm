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

import com.esotericsoftware.kryo.io.Input;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.tracing.TupleTracer;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KryoTupleDeserializer implements ITupleDeserializer {
    private static final Integer DEFAULT_MAX_DECOMPRESSED_BYTES = 10 * 1024 * 1024; // 10MBytes
    public static final Logger LOG = LoggerFactory.getLogger(KryoTupleDeserializer.class);
    public static final String FAILED_TO_DESERIALIZE_TUPLE = "Failed to deserialize tuple";
    private final GeneralTopologyContext context;
    private final KryoValuesDeserializer kryo;
    private final SerializationFactory.IdDictionary ids;
    private final Input kryoInput;
    private final int maxZstdDecompressedBytes;
    private final boolean anyTupleCompressionEnabled;
    private final TupleTracer tracer;

    public KryoTupleDeserializer(final Map<String, Object> conf,
            final GeneralTopologyContext context) {
        this(conf, context, null);
    }

    /**
     * Creates a deserializer that reads the trace context of each tuple with {@code tracer}; a null tracer skips it.
     */
    public KryoTupleDeserializer(final Map<String, Object> conf, final GeneralTopologyContext context, final TupleTracer tracer) {
        kryo = new KryoValuesDeserializer(conf);
        this.context = context;
        ids = new SerializationFactory.IdDictionary(context.getRawTopology());
        kryoInput = new Input(1);
        maxZstdDecompressedBytes = ObjectReader.getInt(conf
                .get(Config.TOPOLOGY_TUPLE_COMPRESSION_MAX_DECOMPRESSED_BYTES),
                DEFAULT_MAX_DECOMPRESSED_BYTES);
        anyTupleCompressionEnabled = isTupleCompressionEnabled(conf, context);
        this.tracer = tracer;
    }

    @Override
    public TupleImpl deserialize(byte[] ser) {
        // check zstd header if at least one component is compressing tuples.
        if (anyTupleCompressionEnabled && Utils.ZstdUtils.isZstd(ser)) {
            try {
                byte[] decompressed = Utils.ZstdUtils.decompress(ser,
                        this.maxZstdDecompressedBytes);
                return deserializeTuple(decompressed);
            } catch (RuntimeException e) {
                if (e.getMessage() != null && e.getMessage()
                        .contains(FAILED_TO_DESERIALIZE_TUPLE)) {
                    // isZstd() false positive: a raw Kryo tuple's first 4 bytes matched
                    // ZSTD_MAGIC_HEADER by chance.
                    // This is astronomically unlikely in practice. Because ZSTD_MAGIC_HEADER
                    // (0xFD2FB528) is little-endian
                    // on the wire, the first byte checked is 0x28. A Kryo writeInt(taskId, true) of
                    // 40 yields exactly 0x28.
                    // The collision is prevented not by the taskId range, but by the second field
                    // (streamId),
                    // which would rigidly have to equal 6069 to match the remaining magic bytes.
                    // Branch retained for correctness in case of an accidental collision.
                    LOG.debug("isZstd() false positive: raw Kryo tuple matched ZSTD_MAGIC_HEADER "
                            + "(0xFD2FB528).");
                    return deserializeTuple(ser);
                } else {
                    throw e;
                }
            }
        }
        return deserializeTuple(ser);
    }

    private TupleImpl deserializeTuple(byte[] data) {
        try {
            kryoInput.setBuffer(data, 0, data.length);
            int taskId = kryoInput.readInt(true);
            int streamId = kryoInput.readInt(true);
            String componentName = context.getComponentId(taskId);
            if (componentName == null) {
                throw new TupleDeserializationException("Received a tuple from unknown task "
                        + taskId);
            }
            String streamName = ids.getStreamName(componentName, streamId);
            if (streamName == null) {
                throw new TupleDeserializationException("Component " + componentName
                        + " has no stream with id " + streamId);
            }
            MessageId id = MessageId.deserialize(kryoInput);
            List<Object> values = kryo.deserializeFrom(kryoInput);
            TupleImpl tuple = new TupleImpl(context, values, componentName, taskId, streamName, id);
            if (tracer != null) {
                tuple.setTraceContext(readTraceContext());
            }
            return tuple;
        } catch (IOException e) {
            throw new RuntimeException(FAILED_TO_DESERIALIZE_TUPLE, e);
        }
    }

    /**
     * Reads the entries after the values, see {@link KryoTupleSerializer}. Returns null when no trace context entry is present or
     * it cannot be read, so that a bad entry never drops the tuple.
     */
    private Object readTraceContext() {
        try {
            while (kryoInput.position() < kryoInput.limit()) {
                int tag = kryoInput.readByte();
                int length = kryoInput.readVarInt(true);
                if (length < 0 || length > kryoInput.limit() - kryoInput.position()) {
                    return null;
                }
                if (tag == KryoTupleSerializer.TRACE_CONTEXT_TAG) {
                    return tracer.decode(kryoInput.readBytes(length));
                }
                kryoInput.skip(length);
            }
        } catch (RuntimeException malformed) {
            LOG.debug("Ignoring an unreadable trace context on a received tuple", malformed);
        }
        return null;
    }

    private static boolean isTupleCompressionEnabled(final Map<String, Object> conf,
            final GeneralTopologyContext context) {
        if (ObjectReader.getBoolean(conf.get(Config.TOPOLOGY_TUPLE_COMPRESSION_ENABLE), false)) {
            return true;
        }
        for (String componentId : context.getComponentIds()) {
            ComponentCommon common = context.getComponentCommon(componentId);
            Map<String, Object> componentConf = Utils.parseJson(common.get_json_conf());
            if (ObjectReader.getBoolean(componentConf.get(Config.TOPOLOGY_TUPLE_COMPRESSION_ENABLE),
                    false)) {
                return true;
            }
        }
        return false;
    }
}
