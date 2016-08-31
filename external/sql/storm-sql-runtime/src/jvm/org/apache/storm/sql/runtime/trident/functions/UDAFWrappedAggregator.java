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

import org.apache.storm.sql.runtime.trident.NumberConverter;
import org.apache.storm.sql.runtime.trident.TridentUtils;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

class UDAFAggregateState {
    private Object accumulator;

    public UDAFAggregateState(Object accumulator) {
        this.accumulator = accumulator;
    }

    public Object getAccumulator() {
        return accumulator;
    }

    public void setAccumulator(Object accumulator) {
        this.accumulator = accumulator;
    }
}

public class UDAFWrappedAggregator extends BaseAggregator<UDAFAggregateState> {

    private final String declaringClassName;
    private final boolean isMethodsStatic;
    private final String inputFieldName;
    private final Class<?> targetClazz;

    private transient Method initMethod;
    private transient Method addMethod;
    private transient Method resultMethod;

    private transient Object aggrInstance;

    public UDAFWrappedAggregator(String declaringClassName, boolean isMethodsStatic, String inputFieldName, Class<?> targetClazz) {
        this.declaringClassName = declaringClassName;
        this.isMethodsStatic = isMethodsStatic;
        this.inputFieldName = inputFieldName;
        this.targetClazz = targetClazz;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        Class<?> declaringClass;
        try {
            declaringClass = Class.forName(declaringClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("UDAF class is not located to classpath: " + declaringClassName);
        }

        setupAggregateMethods(declaringClass);

        if (!isMethodsStatic) {
            try {
                aggrInstance = declaringClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException("Failed to instantiate an instance of the UDAF class");
            }
        }
    }

    private void setupAggregateMethods(Class<?> declaringClass) {
        initMethod = findMethod(declaringClass, "init");
        addMethod = findMethod(declaringClass, "add");
        resultMethod = findMethod(declaringClass, "result");

        if (initMethod == null || addMethod == null) {
            throw new RuntimeException("UDAF class must implement both 'init' and 'add' methods");
        }
    }

    @Override
    public UDAFAggregateState init(Object batchId, TridentCollector collector) {
        Object val = invokeMethod(initMethod);
        return new UDAFAggregateState(val);
    }

    @Override
    public void aggregate(UDAFAggregateState state, TridentTuple tuple, TridentCollector collector) {
        Object fieldValue = TridentUtils.valueFromTuple(tuple, inputFieldName);
        Object newValue = invokeMethod(addMethod, state.getAccumulator(), fieldValue);
        state.setAccumulator(newValue);
    }

    @Override
    public void complete(UDAFAggregateState state, TridentCollector collector) {
        Object emitValue = state.getAccumulator();
        if (resultMethod != null) {
            emitValue = invokeMethod(resultMethod, emitValue);
        }

        if (Number.class.isAssignableFrom(targetClazz)) {
            if (emitValue instanceof Number) {
                emitValue = NumberConverter.convert((Number) emitValue, (Class<? extends Number>) targetClazz);
            } else {
                throw new RuntimeException("Type of aggregated value should be Number");
            }
        } else if (!targetClazz.isAssignableFrom(emitValue.getClass())) {
            throw new RuntimeException("Type of aggregated value should be " + targetClazz.getCanonicalName());
        }

        collector.emit(new Values(emitValue));
    }

    private Method findMethod(Class<?> clazz, String methodName) {
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.getName().equals(methodName)) {
                return method;
            }
        }

        return null;
    }

    private Object invokeMethod(Method method, Object...params) {
        try {
            if (isMethodsStatic) {
                return method.invoke(null, params);
            } else {
                return method.invoke(aggrInstance, params);
            }
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

}