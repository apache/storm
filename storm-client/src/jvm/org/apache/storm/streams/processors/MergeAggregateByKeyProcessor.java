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
import org.apache.storm.streams.operations.CombinerAggregator;

public class MergeAggregateByKeyProcessor<K, V, A, R> extends BaseProcessor<Pair<K, A>> implements BatchProcessor {
    protected final CombinerAggregator<V, A, R> aggregator;
    protected final Map<K, A> state = new HashMap<>();

    public MergeAggregateByKeyProcessor(CombinerAggregator<V, A, R> aggregator) {
        this.aggregator = aggregator;
    }

    @Override
    public void execute(Pair<K, A> input) {
        K key = input.getFirst();
        A val = input.getSecond();
        A accumulator = state.get(key);
        if (accumulator == null) {
            accumulator = aggregator.init();
        }
        state.put(key, aggregator.merge(accumulator, val));
        mayBeForwardAggUpdate(() -> Pair.of(key, aggregator.result(state.get(key))));
    }

    @Override
    public void finish() {
        for (Map.Entry<K, A> entry : state.entrySet()) {
            context.forward(Pair.of(entry.getKey(), aggregator.result(entry.getValue())));
        }
        state.clear();
    }

}
