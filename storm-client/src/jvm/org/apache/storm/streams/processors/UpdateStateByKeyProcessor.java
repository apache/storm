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

import org.apache.storm.state.KeyValueState;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.operations.StateUpdater;

public class UpdateStateByKeyProcessor<K, V, R> extends BaseProcessor<Pair<K, V>> implements StatefulProcessor<K, R> {
    private final StateUpdater<V, R> stateUpdater;
    private KeyValueState<K, R> keyValueState;

    public UpdateStateByKeyProcessor(StateUpdater<V, R> stateUpdater) {
        this.stateUpdater = stateUpdater;
    }

    @Override
    public void initState(KeyValueState<K, R> keyValueState) {
        this.keyValueState = keyValueState;
    }

    @Override
    protected void execute(Pair<K, V> input) {
        K key = input.getFirst();
        V val = input.getSecond();
        R agg = keyValueState.get(key);
        if (agg == null) {
            agg = stateUpdater.init();
        }
        R newAgg = stateUpdater.apply(agg, val);
        keyValueState.put(key, newAgg);
        context.forward(Pair.of(key, newAgg));
    }
}
