/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.serialization;

import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.thrift.TBase;
import org.apache.storm.thrift.TDeserializer;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.TSerializer;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;

/**
 * Note, this assumes it's deserializing a zstd byte stream, and will err if it encounters any other serialization.
 */
public class ZstdThriftSerializationDelegate implements SerializationDelegate {

    private static final int DEFAULT_MAX_DECOMPRESSED_BYTES = 100 * 1024 * 1024;
    private static final int DEFAULT_ZSTD_COMPRESSION_LEVEL = 3;

    private int zstdCompressionLevel;
    private int maxDecompressedBytes;

    @Override
    public void prepare(Map<String, Object> topoConf) {
        this.zstdCompressionLevel = ObjectReader.getInt(topoConf.getOrDefault(Config.STORM_COMPRESSION_ZSTD_LEVEL,
                DEFAULT_ZSTD_COMPRESSION_LEVEL));
        this.maxDecompressedBytes = ObjectReader.getInt(topoConf.getOrDefault(Config.STORM_COMPRESSION_ZSTD_MAX_DECOMPRESSED_BYTES,
                DEFAULT_MAX_DECOMPRESSED_BYTES));
    }

    @Override
    public byte[] serialize(Object object) {
        if (!(object instanceof TBase)) {
            throw new IllegalArgumentException("Object must be an instance of TBase");
        }
        try {
            TSerializer serializer = new TSerializer();
            byte[] thriftData = serializer.serialize((TBase<?, ?>) object);
            return Utils.ZstdUtils.compress(thriftData, this.zstdCompressionLevel);
        } catch (TTransportException e) {
            throw new RuntimeException("Failed to initialize Thrift Serializer", e);
        } catch (TException e) {
            throw new RuntimeException("Failed to serialize Thrift object", e);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        if (!Utils.ZstdUtils.isZstd(bytes)) {
            throw new RuntimeException(
                    String.format("Cannot deserialize [%s]. Expected zstd compressed bytes, but received unknown format.",
                            clazz.getSimpleName())
            );
        }
        try {
            TDeserializer deserializer = new TDeserializer();
            byte[] decompressed = Utils.ZstdUtils.decompress(bytes, this.maxDecompressedBytes);
            TBase<?, ?> instance = clazz.asSubclass(TBase.class).getDeclaredConstructor().newInstance();
            deserializer.deserialize(instance, decompressed);
            return (T) instance;
        } catch (TTransportException e) {
            throw new RuntimeException("Failed to initialize Thrift Deserializer", e);
        } catch (ReflectiveOperationException | TException e) {
            throw new RuntimeException("Failed to deserialize bytes to " + clazz.getName(), e);
        }
    }

}
