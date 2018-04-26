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
import org.apache.storm.trident.operation.BaseOperation;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.operation.Operation;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

public class MapFunctionExecutor extends BaseOperation implements Function {
    private final MapFunction function;

    public MapFunctionExecutor(MapFunction function) {
        this.function = function;
    }

    @Override
    public void prepare(Map<String, Object> conf, TridentOperationContext context) {
        // If MapFunction is aware of prepare, let it handle preparation
        if (function instanceof Operation) {
            ((Operation) function).prepare(conf, context);
        } else {
            super.prepare(conf, context);
        }
    }

    @Override
    public void cleanup() {
        // If MapFunction is aware of cleanup, let it handle cleaning up
        if (function instanceof Operation) {
            ((Operation) function).cleanup();
        } else {
            super.cleanup();
        }
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        collector.emit(function.execute(tuple));
    }
}
