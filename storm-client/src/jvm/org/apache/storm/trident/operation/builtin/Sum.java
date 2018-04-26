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

package org.apache.storm.trident.operation.builtin;

import java.math.BigDecimal;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;


public class Sum implements CombinerAggregator<Number> {

    private static BigDecimal asBigDecimal(Number val) {
        BigDecimal ret;
        if (val instanceof BigDecimal) {
            ret = (BigDecimal) val;
        } else {
            ret = new BigDecimal(val.doubleValue());
        }
        return ret;
    }

    @Override
    public Number init(TridentTuple tuple) {
        return (Number) tuple.getValue(0);
    }

    @Override
    public Number combine(Number val1, Number val2) {
        if (val1 instanceof BigDecimal || val2 instanceof BigDecimal) {
            BigDecimal v1 = asBigDecimal(val1);
            BigDecimal v2 = asBigDecimal(val2);
            return (v1).add(v2);
        }
        if (val1 instanceof Double || val2 instanceof Double) {
            return val1.doubleValue() + val2.doubleValue();
        }
        return val1.longValue() + val2.longValue();
    }

    @Override
    public Number zero() {
        return 0;
    }
}
