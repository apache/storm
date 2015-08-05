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
package com.alibaba.jstorm.common.metric;

import com.alibaba.jstorm.common.metric.operator.convert.DefaultConvertor;
import com.alibaba.jstorm.common.metric.operator.merger.TpsMerger;
import com.alibaba.jstorm.common.metric.operator.updater.AddUpdater;
import com.alibaba.jstorm.common.metric.window.Metric;
import com.alibaba.jstorm.common.metric.window.RollingWindow;

/**
 * Meter is used to compute tps
 * 
 * Attention: 1.
 * 
 * @author zhongyan.feng
 * 
 */
public class Meter extends Metric<Double, Double> {
    private static final long serialVersionUID = -1362345159511508074L;

    public Meter() {
        defaultValue = 0.0d;
        updater = new AddUpdater<Double>();
        merger = new TpsMerger();
        convertor = new DefaultConvertor<Double>();

        init();
    }

    public void update() {
        update(Double.valueOf(1));
    }

}
