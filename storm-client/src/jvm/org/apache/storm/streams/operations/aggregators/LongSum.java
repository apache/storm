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

package org.apache.storm.streams.operations.aggregators;

import org.apache.storm.streams.operations.CombinerAggregator;

/**
 * Computes the long sum of the input values.
 */
public class LongSum implements CombinerAggregator<Number, Long, Long> {
    @Override
    public Long init() {
        return 0L;
    }

    @Override
    public Long apply(Long aggregate, Number value) {
        return value.longValue() + aggregate;
    }

    @Override
    public Long merge(Long accum1, Long accum2) {
        return accum1 + accum2;
    }

    @Override
    public Long result(Long accum) {
        return accum;
    }
}
