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
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EvaluationFilter extends BaseFilter {
    private static final Logger LOG = LoggerFactory.getLogger(EvaluationFilter.class);

    private final ExecutableExpression filterInstance;
    private final DataContext dataContext;
    private final Object[] outputValues;

    public EvaluationFilter(ExecutableExpression filterInstance, DataContext dataContext) {
        this.filterInstance = filterInstance;
        this.dataContext = dataContext;
        this.outputValues = new Object[1];
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        if (filterInstance != null && filterInstance instanceof DebuggableExecutableExpression) {
            LOG.info("Expression code for filter: \n{}", ((DebuggableExecutableExpression) filterInstance).getDelegateCode());
        }
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        Context calciteContext = new StormContext(dataContext);
        calciteContext.values = tuple.getValues().toArray();
        filterInstance.execute(calciteContext, outputValues);
        return (outputValues[0] != null && (boolean) outputValues[0]);
    }
}
