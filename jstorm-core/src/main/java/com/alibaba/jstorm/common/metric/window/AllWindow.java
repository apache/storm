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
package com.alibaba.jstorm.common.metric.window;

import java.util.ArrayList;

import com.alibaba.jstorm.common.metric.operator.Sampling;
import com.alibaba.jstorm.common.metric.operator.StartTime;
import com.alibaba.jstorm.common.metric.operator.merger.Merger;
import com.alibaba.jstorm.common.metric.operator.updater.Updater;

public class AllWindow<V> implements Sampling<V>, StartTime {

    private static final long serialVersionUID = -8523514907315740812L;

    protected V unflushed;
    protected V defaultValue;

    protected Updater<V> updater;
    protected Merger<V> merger;
    protected long startTime;

    AllWindow(V defaultValue, Updater<V> updater, Merger<V> merger) {

        this.updater = updater;
        this.merger = merger;

        this.defaultValue = defaultValue;
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public void update(Number obj) {
        // TODO Auto-generated method stub
        synchronized (this) {
            unflushed = updater.update(obj, unflushed);
        }
    }
    
    public void updateBatch(V batch) {
        synchronized (this) {
            unflushed = updater.updateBatch(batch, unflushed);
        }
    }

    @Override
    public V getSnapshot() {
        // TODO Auto-generated method stub
        V ret = merger.merge(new ArrayList<V>(), unflushed, this);
        if (ret == null) {
            return defaultValue;
        } else {
            return ret;
        }
    }

    @Override
    public long getStartTime() {
        // TODO Auto-generated method stub
        return startTime;
    }

}
