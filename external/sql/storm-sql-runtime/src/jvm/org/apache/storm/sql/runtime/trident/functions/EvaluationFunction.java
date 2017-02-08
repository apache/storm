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
package org.apache.storm.sql.runtime.trident.functions;

import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.Context;
import org.apache.calcite.interpreter.StormContext;
import org.apache.storm.sql.runtime.calcite.DebuggableExecutableExpression;
import org.apache.storm.sql.runtime.calcite.ExecutableExpression;
import org.apache.storm.trident.operation.OperationAwareMapFunction;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EvaluationFunction implements OperationAwareMapFunction {
    private static final Logger LOG = LoggerFactory.getLogger(EvaluationFunction.class);

    private final ExecutableExpression projectionInstance;
    private final Object[] outputValues;
    private final DataContext dataContext;

    public EvaluationFunction(ExecutableExpression projectionInstance, int outputCount, DataContext dataContext) {
        this.projectionInstance = projectionInstance;
        this.outputValues = new Object[outputCount];
        this.dataContext = dataContext;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        if (projectionInstance instanceof DebuggableExecutableExpression) {
            LOG.info("Expression code: {}", ((DebuggableExecutableExpression) projectionInstance).getDelegateCode());
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public Values execute(TridentTuple input) {
        Context calciteContext = new StormContext(dataContext);
        calciteContext.values = input.getValues().toArray();
        projectionInstance.execute(calciteContext, outputValues);
        return new Values(outputValues);
    }
}
