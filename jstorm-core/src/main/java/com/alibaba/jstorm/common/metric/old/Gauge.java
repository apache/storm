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

import com.alibaba.jstorm.common.metric.old.window.Metric;
import com.alibaba.jstorm.common.metric.old.window.StatBuckets;

import java.util.Map;
import java.util.TreeMap;

public class Gauge<T extends Number> extends Metric<Number, Number> {
    private static final long serialVersionUID = 1985614006717750790L;

    protected com.codahale.metrics.Gauge<T> gauge;

    public Gauge(com.codahale.metrics.Gauge<T> gauge) {
        this.gauge = gauge;

        init();
    }

    @Override
    public void init() {

    }

    @Override
    public void update(Number obj) {
        // TODO Auto-generated method stub
    }

    @Override
    public Map<Integer, Number> getSnapshot() {
        // TODO Auto-generated method stub
        Number value = gauge.getValue();

        Map<Integer, Number> ret = new TreeMap<Integer, Number>();
        for (Integer timeKey : windowSeconds) {
            ret.put(timeKey, value);
        }
        ret.put(StatBuckets.ALL_TIME_WINDOW, value);

        return ret;
    }

}
