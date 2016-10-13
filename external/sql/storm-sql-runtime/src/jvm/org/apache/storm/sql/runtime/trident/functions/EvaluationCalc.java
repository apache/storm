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
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.OperationAwareFlatMapFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ScriptEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;

public class EvaluationCalc implements OperationAwareFlatMapFunction {
    private static final Logger LOG = LoggerFactory.getLogger(EvaluationCalc.class);

    private transient ScriptEvaluator evaluator;

    private final String expression;
    private final Object[] outputValues;
    private final DataContext dataContext;

    public EvaluationCalc(String filterExpression, String projectionExpression, int outputCount, DataContext dataContext) {
        expression = buildCompleteExpression(filterExpression, projectionExpression);
        this.outputValues = new Object[outputCount];
        this.dataContext = dataContext;
    }

    private String buildCompleteExpression(String filterExpression, String projectionExpression) {
        StringBuilder sb = new StringBuilder();

        if (filterExpression != null && !filterExpression.isEmpty()) {
            sb.append(filterExpression);
            // TODO: Convert this with Linq4j?
            sb.append("if (outputValues[0] == null || !((Boolean) outputValues[0])) { return 0; }\n\n");
        }

        if (projectionExpression != null && !projectionExpression.isEmpty()) {
            sb.append(projectionExpression);
        }
        sb.append("\nreturn 1;");

        return sb.toString();
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        LOG.info("Expression: {}", expression);
        try {
            evaluator = new ScriptEvaluator(expression, int.class,
                    new String[] {"context", "outputValues"},
                    new Class[] { Context.class, Object[].class });
        } catch (CompileException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public Iterable<Values> execute(TridentTuple input) {
        try {
            Context calciteContext = new StormContext(dataContext);
            calciteContext.values = input.getValues().toArray();
            int keepFlag = (int) evaluator.evaluate(
                    new Object[]{calciteContext, outputValues});
            // script
            if (keepFlag == 1) {
                return Collections.singletonList(new Values(outputValues));
            } else {
                return Collections.emptyList();
            }
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }

    }
}
