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

import org.apache.storm.streams.Pair;
import org.apache.storm.streams.operations.FlatMapFunction;

public class FlatMapValuesProcessor<K, V, R> extends BaseProcessor<Pair<K, V>> {
    private final FlatMapFunction<V, R> function;

    public FlatMapValuesProcessor(FlatMapFunction<V, R> function) {
        this.function = function;
    }

    @Override
    protected void execute(Pair<K, V> input) {
        for (R res : function.apply(input.getSecond())) {
            context.forward(Pair.of(input.getFirst(), res));
        }
    }
}
