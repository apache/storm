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

package org.apache.storm.streams.processors;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.operations.Reducer;

public class ReduceByKeyProcessor<K, V> extends BaseProcessor<Pair<K, V>> implements BatchProcessor {
    private final Reducer<V> reducer;
    private final Map<K, V> state = new HashMap<>();

    public ReduceByKeyProcessor(Reducer<V> reducer) {
        this.reducer = reducer;
    }

    @Override
    public void execute(Pair<K, V> input) {
        K key = input.getFirst();
        V val = input.getSecond();
        V agg = state.get(key);
        final V res = (agg == null) ? val : reducer.apply(agg, val);
        state.put(key, res);
        mayBeForwardAggUpdate(() -> Pair.of(key, res));
    }

    @Override
    public void finish() {
        for (Map.Entry<K, V> entry : state.entrySet()) {
            context.forward(Pair.of(entry.getKey(), entry.getValue()));
        }
        state.clear();
    }

}
