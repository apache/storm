/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.trident.operation.builtin;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * Abstract {@code CombinerAggregator} for comparing two values in a stream.
 *
 */
public abstract class AbstractComparisionAggreagator<T> implements CombinerAggregator<T> {

    @Override
    public T init(TridentTuple tuple) {
        return (T) tuple.getValue(0);
    }

    @Override
    public T combine(T value1, T value2) {
        if(value1 == null) return value2;
        if(value2 == null) return value1;
        
        return compare(value1, value2);
    }

    protected abstract T compare(T value1, T value2);

    @Override
    public T zero() {
        return null;
    }
}
