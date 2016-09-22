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
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class DivideForAverage extends BaseFunction {

    private final Class<? extends Number> targetClazz;

    public DivideForAverage(Class<? extends Number> targetClazz) {
        this.targetClazz = targetClazz;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        // This expects two input fields: first one is the result of SumBy and next one is the result of CountBy
        Number n1 = (Number)tuple.get(0);
        // the type of result for CountBy is Long
        Long n2 = (Long)tuple.get(1);

        collector.emit(new Values(NumberConverter.convert(Numbers.divide(n1, n2), targetClazz)));
    }
}