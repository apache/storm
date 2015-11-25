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
package com.alibaba.jstorm.common.metric.old.operator.merger;

import java.util.Collection;

import com.alibaba.jstorm.common.metric.old.operator.StartTime;

public class TpsMerger implements Merger<Double> {
    private static final long serialVersionUID = -4534840881635955942L;
    protected final long createTime;

    public TpsMerger() {
        createTime = System.currentTimeMillis();
    }

    public long getRunMillis(Object... args) {
        long startTime = createTime;

        if (args != null) {
            if (args[0] != null && args[0] instanceof StartTime) {
                StartTime rollingWindow = (StartTime) args[0];

                startTime = rollingWindow.getStartTime();
            }
        }

        return (System.currentTimeMillis() - startTime);
    }

    @Override
    public Double merge(Collection<Double> objs, Double unflushed, Object... others) {
        // TODO Auto-generated method stub
        double sum = 0.0d;
        if (unflushed != null) {
            sum += unflushed;
        }

        for (Double item : objs) {
            if (item != null) {
                sum += item;
            }
        }

        Double ret = (sum * 1000) / getRunMillis(others);
        return ret;
    }

}
