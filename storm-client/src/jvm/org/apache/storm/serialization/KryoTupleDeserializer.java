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
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;

public class KryoTupleDeserializer implements ITupleDeserializer {
    private static final Integer DEFAULT_MAX_DECOMPRESSED_BYTES = 10 * 1024 * 1024; // 10MBytes
    public static final String FAILED_TO_DESERIALIZE_TUPLE = "Failed to deserialize tuple";
    private final GeneralTopologyContext context;
    private final KryoValuesDeserializer kryo;
    private final SerializationFactory.IdDictionary ids;
    private final Input kryoInput;
    private final int maxZstdDecompressedBytes;

    public KryoTupleDeserializer(final Map<String, Object> conf, final GeneralTopologyContext context) {
        kryo = new KryoValuesDeserializer(conf);
        this.context = context;
        ids = new SerializationFactory.IdDictionary(context.getRawTopology());
        kryoInput = new Input(1);
        maxZstdDecompressedBytes = ObjectReader.getInt(conf.get(Config.TOPOLOGY_TUPLE_COMPRESSION_MAX_DECOMPRESSED_BYTES),
                DEFAULT_MAX_DECOMPRESSED_BYTES);
    }

    @Override
    public TupleImpl deserialize(byte[] ser) {
        if (Utils.ZstdUtils.isZstd(ser)) {
            try {
                byte[] decompressed = Utils.ZstdUtils.decompress(ser, this.maxZstdDecompressedBytes);
                return deserializeTuple(decompressed);
            } catch (RuntimeException e) {
                if (e.getMessage() != null && e.getMessage().contains(FAILED_TO_DESERIALIZE_TUPLE)) {
                    // isZstd() false positive: a raw Kryo tuple's first 4 bytes matched ZSTD_MAGIC_HEADER by chance.
                    // This is astronomically unlikely in practice. Because ZSTD_MAGIC_HEADER (0xFD2FB528) is little-endian
                    // on the wire, the first byte checked is 0x28. A Kryo writeInt(taskId, true) of 40 yields exactly 0x28.
                    // The collision is prevented not by the taskId range, but by the second field (streamId),
                    // which would rigidly have to equal 6069 to match the remaining magic bytes.
                    // Branch retained for correctness in case of an accidental collision.
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
            String streamName = ids.getStreamName(componentName, streamId);
            MessageId id = MessageId.deserialize(kryoInput);
            List<Object> values = kryo.deserializeFrom(kryoInput);
            return new TupleImpl(context, values, componentName, taskId, streamName, id);
        } catch (IOException e) {
            throw new RuntimeException(FAILED_TO_DESERIALIZE_TUPLE, e);
        }
    }
}
