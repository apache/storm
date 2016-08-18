/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.sql.runtime.trident.operations;

import clojure.lang.Numbers;
import org.apache.storm.sql.runtime.trident.NumberConverter;
import org.apache.storm.sql.runtime.trident.TridentUtils;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

public class MaxBy implements CombinerAggregator<Object> {

    private final String inputFieldName;
    private final Class<?> targetClazz;

    public MaxBy(String inputFieldName, Class<?> targetClazz) {
        this.inputFieldName = inputFieldName;
        this.targetClazz = targetClazz;
    }

    @Override
    public Object init(TridentTuple tuple) {
        return TridentUtils.valueFromTuple(tuple, inputFieldName);
    }

    @Override
    public Object combine(Object val1, Object val2) {
        if (val1 == null) {
            return val2;
        }

        if (Number.class.isAssignableFrom(targetClazz)) {
            return NumberConverter.convert((Number) Numbers.max(val1, val2), (Class<? extends Number>) targetClazz);
        }

        if (!val1.getClass().equals(val2.getClass())) {
            throw new IllegalArgumentException("The type of values are different! - class of val1: " +
                    val1.getClass().getCanonicalName() + " and class of val2: " +
                    val2.getClass().getCanonicalName());
        } else if (!(val1 instanceof Comparable)) {
            throw new IllegalArgumentException("Value is not Comparable - class of value: " +
                    val1.getClass().getCanonicalName());
        }

        return ((Comparable)val1).compareTo(val2) < 0 ? val2 : val1;
    }

    @Override
    public Object zero() {
        return null;
    }
}
