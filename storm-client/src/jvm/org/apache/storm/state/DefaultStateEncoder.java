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

package org.apache.storm.state;

import java.util.Optional;

/**
 * Default state encoder class for encoding/decoding key values. This class assumes encoded types of key and value are both binary (byte
 * array) due to keep backward compatibility.
 */
public class DefaultStateEncoder<K, V> implements StateEncoder<K, V, byte[], byte[]> {

    public static final Serializer<Optional<byte[]>> INTERNAL_VALUE_SERIALIZER = new DefaultStateSerializer<>();

    public static final byte[] TOMBSTONE = INTERNAL_VALUE_SERIALIZER.serialize(Optional.<byte[]>empty());

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public DefaultStateEncoder(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public Serializer<K> getKeySerializer() {
        return keySerializer;
    }

    public Serializer<V> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public byte[] encodeKey(K key) {
        return keySerializer.serialize(key);
    }

    @Override
    public byte[] encodeValue(V value) {
        return INTERNAL_VALUE_SERIALIZER.serialize(
            Optional.of(valueSerializer.serialize(value)));
    }

    @Override
    public K decodeKey(byte[] encodedKey) {
        return keySerializer.deserialize(encodedKey);
    }

    @Override
    public V decodeValue(byte[] encodedValue) {
        Optional<byte[]> internalValue = INTERNAL_VALUE_SERIALIZER.deserialize(encodedValue);
        if (internalValue.isPresent()) {
            return valueSerializer.deserialize(internalValue.get());
        }
        return null;
    }

    @Override
    public byte[] getTombstoneValue() {
        return TOMBSTONE;
    }
}
