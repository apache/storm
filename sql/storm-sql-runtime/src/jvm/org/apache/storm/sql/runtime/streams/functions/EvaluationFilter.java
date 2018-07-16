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

import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.Context;
import org.apache.calcite.interpreter.StormContext;
import org.apache.storm.sql.runtime.calcite.DebuggableExecutableExpression;
import org.apache.storm.sql.runtime.calcite.ExecutableExpression;
import org.apache.storm.streams.operations.Predicate;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EvaluationFilter implements Predicate<Values> {
    private static final Logger LOG = LoggerFactory.getLogger(EvaluationFilter.class);

    private final ExecutableExpression filterInstance;
    private final DataContext dataContext;
    private final Object[] outputValues;

    /**
     * EvaluationFilter Constructor.
     * @param filterInstance ExecutableExpression
     * @param dataContext DataContext
     */
    public EvaluationFilter(ExecutableExpression filterInstance, DataContext dataContext) {
        this.filterInstance = filterInstance;
        this.dataContext = dataContext;
        this.outputValues = new Object[1];

        if (filterInstance != null && filterInstance instanceof DebuggableExecutableExpression) {
            LOG.info("Expression code for filter: \n{}", ((DebuggableExecutableExpression) filterInstance).getDelegateCode());
        }
    }

    @Override
    public boolean test(Values input) {
        Context calciteContext = new StormContext(dataContext);
        calciteContext.values = input.toArray();
        filterInstance.execute(calciteContext, outputValues);
        return (outputValues[0] != null && (boolean) outputValues[0]);
    }
}
