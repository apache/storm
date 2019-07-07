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

public class AggregateByKeyProcessor<K, V, A, R> extends BaseProcessor<Pair<K, V>> implements BatchProcessor {
    private final CombinerAggregator<V, A, R> aggregator;
    private final boolean emitAggregate;
    private final Map<K, A> state = new HashMap<>();

    public AggregateByKeyProcessor(CombinerAggregator<V, A, R> aggregator) {
        this(aggregator, false);
    }

    public AggregateByKeyProcessor(CombinerAggregator<V, A, R> aggregator, boolean emitAggregate) {
        this.aggregator = aggregator;
        this.emitAggregate = emitAggregate;
    }

    @Override
    public void execute(Pair<K, V> input) {
        K key = input.getFirst();
        V val = input.getSecond();
        A accumulator = state.get(key);
        if (accumulator == null) {
            accumulator = aggregator.init();
        }
        state.put(key, aggregator.apply(accumulator, val));
        if (emitAggregate) {
            mayBeForwardAggUpdate(() -> Pair.of(key, state.get(key)));
        } else {
            mayBeForwardAggUpdate(() -> Pair.of(key, aggregator.result(state.get(key))));
        }
    }

    @Override
    public void finish() {
        for (Map.Entry<K, A> entry : state.entrySet()) {
            if (emitAggregate) {
                context.forward(Pair.of(entry.getKey(), entry.getValue()));
            } else {
                context.forward(Pair.of(entry.getKey(), aggregator.result(entry.getValue())));
            }

        }
        state.clear();
    }

    @Override
    public String toString() {
        return "AggregateByKeyProcessor{"
                + "aggregator=" + aggregator
                + ", emitAggregate=" + emitAggregate
                + ", state=" + state
                + "}";
    }
}
