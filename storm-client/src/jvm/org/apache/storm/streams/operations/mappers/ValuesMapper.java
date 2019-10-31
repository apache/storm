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

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Constructs a {@link Values} from a {@link Tuple} based on indicies.
 */
public class ValuesMapper implements TupleValueMapper<Values> {
    private final int[] indices;

    /**
     * Constructs a new {@link ValuesMapper} that extracts value from a {@link Tuple} at specified indices.
     *
     * @param indices the indices
     */
    public ValuesMapper(int... indices) {
        this.indices = indices;
    }

    @Override
    public Values apply(Tuple input) {
        Values values = new Values();
        for (int i : indices) {
            values.add(input.getValue(i));
        }
        return values;
    }
}
