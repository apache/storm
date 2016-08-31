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

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ScriptEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

public class EvaluationFilter extends BaseFilter {
    private static final Logger LOG = LoggerFactory.getLogger(EvaluationFilter.class);

    private transient ScriptEvaluator evaluator;
    private final String expression;

    public EvaluationFilter(String expression) {
        this.expression = expression;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        LOG.info("Expression: {}", expression);
        try {
            evaluator = new ScriptEvaluator(expression, Boolean.class,
                    new String[] {"_data"}, new Class[] { List.class });
        } catch (CompileException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        try {
            return (Boolean) evaluator.evaluate(new Object[] {tuple.getValues()});
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
