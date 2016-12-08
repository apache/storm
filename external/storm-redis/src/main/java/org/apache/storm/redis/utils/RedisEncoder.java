/**
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
package org.apache.storm.redis.utils;

import com.google.common.base.Optional;

import org.apache.commons.codec.binary.Base64;
import org.apache.storm.state.DefaultStateSerializer;
import org.apache.storm.state.Serializer;

/**
 * Helper class for encoding/decoding redis key values.
 */
public class RedisEncoder<K, V> {

    public static final Serializer<Optional<byte[]>> internalValueSerializer = new DefaultStateSerializer<>();

    public static final String TOMBSTONE = encode(internalValueSerializer.serialize(Optional.absent()));

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public RedisEncoder(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public Serializer<K> getKeySerializer() {
        return keySerializer;
    }

    public Serializer<V> getValueSerializer() {
        return valueSerializer;
    }

    public String encodeKey(K key) {
        return encode(keySerializer.serialize(key));
    }

    public String encodeValue(V value) {
        return encode(internalValueSerializer.serialize(
                    Optional.of(valueSerializer.serialize(value))));
    }

    public K decodeKey(String redisKey) {
        return keySerializer.deserialize(decode(redisKey));
    }

    public V decodeValue(String redisValue) {
        Optional<byte[]> internalValue = internalValueSerializer.deserialize(decode(redisValue));
        if (internalValue.isPresent()) {
            return valueSerializer.deserialize(internalValue.get());
        }
        return null;
    }

    private static String encode(byte[] bytes) {
        return Base64.encodeBase64String(bytes);
    }

    private static byte[] decode(String s) {
        return Base64.decodeBase64(s);
    }
}
