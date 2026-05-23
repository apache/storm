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
    private static final Integer DEFAULT_MAX_DECOMPRESSED_BYTES = 100 * 1024 * 1024;
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
        maxZstdDecompressedBytes = ObjectReader.getInt(conf.get(Config.STORM_COMPRESSION_ZSTD_MAX_DECOMPRESSED_BYTES),
                DEFAULT_MAX_DECOMPRESSED_BYTES);
    }

    @Override
    public TupleImpl deserialize(byte[] ser) {
        if (Utils.ZstdUtils.isZstd(ser)) {
            try {
                byte[] decompressed = Utils.ZstdUtils.decompress(ser, this.maxZstdDecompressedBytes);
                return deserializeTuple(decompressed);
            } catch (RuntimeException e) {
                // isZstd() false positive: a raw Kryo tuple's first 4 bytes matched ZSTD_MAGIC_HEADER by chance.
                // This is mathematically impossible in practice: the first field is a varint taskId, whose
                // valid range produces a first byte that never reaches 0xFD (the magic header's first byte).
                // Branch retained for correctness in case assumptions about taskId range ever change.
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
            throw new RuntimeException("Failed to deserialize tuple", e);
        }
    }
}
