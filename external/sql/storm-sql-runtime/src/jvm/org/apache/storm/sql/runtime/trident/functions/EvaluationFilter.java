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
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ScriptEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class EvaluationFilter extends BaseFilter {
    private static final Logger LOG = LoggerFactory.getLogger(EvaluationFilter.class);

    private transient ScriptEvaluator evaluator;

    private final String expression;
    private final DataContext dataContext;
    private final Object[] outputValues;

    public EvaluationFilter(String expression, DataContext dataContext) {
        this.expression = expression;
        this.dataContext = dataContext;
        this.outputValues = new Object[1];
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
    public boolean isKeep(TridentTuple tuple) {
        try {
            Context calciteContext = new StormContext(dataContext);
            calciteContext.values = tuple.getValues().toArray();
            evaluator.evaluate(new Object[] {calciteContext, outputValues});
            return (outputValues[0] != null && (boolean) outputValues[0]);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
