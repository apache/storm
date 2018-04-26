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

package org.apache.storm.streams.operations.mappers;

import org.apache.storm.streams.Pair;
import org.apache.storm.streams.operations.PairFunction;
import org.apache.storm.tuple.Tuple;

/**
 * Extracts a typed key-value pair from a tuple.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class PairValueMapper<K, V> implements TupleValueMapper<Pair<K, V>>, PairFunction<Tuple, K, V> {
    private final int keyIndex;
    private final int valueIndex;

    /**
     * Constructs a new {@link PairValueMapper} that constructs a pair from a tuple based on the key and value index.
     *
     * @param keyIndex   the key index
     * @param valueIndex the value index
     */
    public PairValueMapper(int keyIndex, int valueIndex) {
        this.keyIndex = keyIndex;
        this.valueIndex = valueIndex;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Pair<K, V> apply(Tuple input) {
        return Pair.of((K) input.getValue(keyIndex), (V) input.getValue(valueIndex));
    }
}
