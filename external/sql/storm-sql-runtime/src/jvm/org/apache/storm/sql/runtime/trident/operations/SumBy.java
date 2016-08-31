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
package org.apache.storm.sql.runtime.trident.operations;

import clojure.lang.Numbers;
import org.apache.storm.sql.runtime.trident.NumberConverter;
import org.apache.storm.sql.runtime.trident.TridentUtils;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;


public class SumBy implements CombinerAggregator<Number> {

    private final String inputFieldName;
    private final Class<? extends Number> targetClazz;

    public SumBy(String inputFieldName, Class<? extends Number> targetClazz) {
        this.inputFieldName = inputFieldName;
        this.targetClazz = targetClazz;
    }

    @Override
    public Number init(TridentTuple tuple) {
        // handles null field
        Object value = TridentUtils.valueFromTuple(tuple, inputFieldName);
        if (value == null) {
            return zero();
        }

        return (Number) value;
    }

    @Override
    public Number combine(Number val1, Number val2) {
        // to preserve the type of val
        // FIXME: find the alternatives if we don't want to take the overhead
        return NumberConverter.convert(Numbers.add(val1, val2), targetClazz);
    }

    @Override
    public Number zero() {
        return NumberConverter.convert(0, targetClazz);
    }

}