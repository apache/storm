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

package org.apache.storm.trident.operation.impl;

import java.util.Map;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class ReducerAggregatorImpl implements Aggregator<Result> {
    ReducerAggregator agg;

    public ReducerAggregatorImpl(ReducerAggregator agg) {
        this.agg = agg;
    }

    @Override
    public void prepare(Map<String, Object> conf, TridentOperationContext context) {

    }

    @Override
    public Result init(Object batchId, TridentCollector collector) {
        Result ret = new Result();
        ret.obj = agg.init();
        return ret;
    }

    @Override
    public void aggregate(Result val, TridentTuple tuple, TridentCollector collector) {
        val.obj = agg.reduce(val.obj, tuple);
    }

    @Override
    public void complete(Result val, TridentCollector collector) {
        collector.emit(new Values(val.obj));
    }

    @Override
    public void cleanup() {

    }
}
