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
import org.apache.storm.thrift.TBase;
import org.apache.storm.thrift.TDeserializer;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.TSerializer;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.Utils;

/**
 * Note, this assumes it's deserializing a gzip byte stream, and will err if it encounters any other serialization.
 */
public class ZstdThriftSerializationDelegate implements SerializationDelegate {

    // ThreadLocal with explicit exception handling for checked TTransportException
    private static final ThreadLocal<TSerializer> SERIALIZER = ThreadLocal.withInitial(() -> {
        try {
            return new TSerializer();
        } catch (TTransportException e) {
            throw new RuntimeException("Failed to initialize Thrift Serializer", e);
        }
    });

    private static final ThreadLocal<TDeserializer> DESERIALIZER = ThreadLocal.withInitial(() -> {
        try {
            return new TDeserializer();
        } catch (TTransportException e) {
            throw new RuntimeException("Failed to initialize Thrift Deserializer", e);
        }
    });

    @Override
    public void prepare(Map<String, Object> topoConf) {
        // No-op: Initialization happens lazily per thread
    }

    @Override
    public byte[] serialize(Object object) {
        if (!(object instanceof TBase)) {
            throw new IllegalArgumentException("Object must be an instance of TBase");
        }
        try {
            byte[] thriftData = SERIALIZER.get().serialize((TBase<?, ?>) object);
            return Utils.ZstdUtils.compress(thriftData);
        } catch (TException e) {
            throw new RuntimeException("Failed to serialize Thrift object", e);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        try {
            byte[] decompressed = Utils.ZstdUtils.decompress(bytes);
            TBase<?, ?> instance = (TBase<?, ?>) clazz.getDeclaredConstructor().newInstance();
            DESERIALIZER.get().deserialize(instance, decompressed);
            return (T) instance;
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize bytes to " + clazz.getName(), e);
        }
    }

}
