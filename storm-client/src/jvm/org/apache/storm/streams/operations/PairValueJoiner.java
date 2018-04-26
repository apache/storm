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

package org.apache.storm.streams.operations;

import org.apache.storm.streams.Pair;

/**
 * A {@link ValueJoiner} that joins two values to produce a {@link Pair} of the two values as the result.
 *
 * @param <V1> the type of the first value
 * @param <V2> the type of the second value
 */
public class PairValueJoiner<V1, V2> implements ValueJoiner<V1, V2, Pair<V1, V2>> {
    /**
     * Joins two values and produces a {@link Pair} of the values as the result.
     *
     * @param value1 the first value
     * @param value2 the second value
     * @return a pair of the first and second value
     */
    @Override
    public Pair<V1, V2> apply(V1 value1, V2 value2) {
        return Pair.of(value1, value2);
    }
}
