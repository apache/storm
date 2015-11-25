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
package com.alibaba.jstorm.common.metric.old.operator.merger;

import com.alibaba.jstorm.common.metric.old.Histogram;

import java.util.Collection;

public class AvgMerger implements Merger<Histogram.HistorgramPair> {
    private static final long serialVersionUID = -3892281208959055221L;

    @Override
    public Histogram.HistorgramPair merge(Collection<Histogram.HistorgramPair> objs, Histogram.HistorgramPair unflushed, Object... others) {
        // TODO Auto-generated method stub
        double sum = 0.0d;
        long times = 0l;

        if (unflushed != null) {
            sum = sum + unflushed.getSum();
            times = times + unflushed.getTimes();
        }

        for (Histogram.HistorgramPair item : objs) {
            if (item == null) {
                continue;
            }
            sum = sum + item.getSum();
            times = times + item.getTimes();
        }

        return new Histogram.HistorgramPair(sum, times);
    }

}
