/**
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
 */
package com.alibaba.jstorm.common.metric.old;

import com.alibaba.jstorm.common.metric.old.operator.convert.DefaultConvertor;
import com.alibaba.jstorm.common.metric.old.operator.merger.SumMerger;
import com.alibaba.jstorm.common.metric.old.operator.updater.AddUpdater;
import com.alibaba.jstorm.common.metric.old.window.Metric;

/**
 * The class is similar to com.codahale.metrics.Counter
 * 
 * Sum all window's value
 * 
 * how to use Counter , please refer to Sampling Interface
 * 
 * @author zhongyan.feng
 * 
 * @param <T>
 */
public class Counter<T extends Number> extends Metric<T, T> {
    private static final long serialVersionUID = -1362345159511508074L;

    public Counter(T zero) {
        updater = new AddUpdater<T>();
        merger = new SumMerger<T>();
        convertor = new DefaultConvertor<T>();
        defaultValue = zero;

        init();
    }

    public static void main(String[] args) {

    }
}
