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
package org.apache.storm.streams.processors;

import org.apache.storm.streams.operations.Aggregator;

public class AggregateProcessor<T, R> extends BaseProcessor<T> implements BatchProcessor {
    private final Aggregator<T, R> aggregator;
    private R state;

    public AggregateProcessor(Aggregator<T, R> aggregator) {
        this.aggregator = aggregator;
    }

    @Override
    public void execute(T input) {
        if (state == null) {
            state = aggregator.init();
        }
        R curAggregate = (state != null) ? state : aggregator.init();
        state = aggregator.apply(input, curAggregate);
        mayBeForwardAggUpdate(state);
    }

    @Override
    public void finish() {
        context.forward(state);
        state = null;
    }
}
