/*
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
 *
 */

package org.apache.storm.sql.runtime.streams.functions;

import java.util.Map;

import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.Context;
import org.apache.calcite.interpreter.StormContext;
import org.apache.storm.sql.runtime.calcite.DebuggableExecutableExpression;
import org.apache.storm.sql.runtime.calcite.ExecutableExpression;
import org.apache.storm.streams.operations.Function;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EvaluationFunction implements Function<Values, Values> {
    private static final Logger LOG = LoggerFactory.getLogger(EvaluationFunction.class);

    private final ExecutableExpression projectionInstance;
    private final Object[] outputValues;
    private final DataContext dataContext;

    /**
     * EvaluationFunction Constructor.
     * @param projectionInstance ExecutableExpression
     * @param outputCount output count
     * @param dataContext DataContext
     */
    public EvaluationFunction(ExecutableExpression projectionInstance, int outputCount, DataContext dataContext) {
        this.projectionInstance = projectionInstance;
        this.outputValues = new Object[outputCount];
        this.dataContext = dataContext;

        if (projectionInstance instanceof DebuggableExecutableExpression) {
            LOG.info("Expression code: {}", ((DebuggableExecutableExpression) projectionInstance).getDelegateCode());
        }
    }

    @Override
    public Values apply(Values input) {
        Context calciteContext = new StormContext(dataContext);
        calciteContext.values = input.toArray();
        projectionInstance.execute(calciteContext, outputValues);
        return new Values(outputValues);
    }
}
