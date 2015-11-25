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
package com.alibaba.jstorm.common.metric.old.operator.updater;

import com.alibaba.jstorm.common.metric.old.Histogram;

public class AvgUpdater implements Updater<Histogram.HistorgramPair> {
    private static final long serialVersionUID = 2562836921724586449L;

    @Override
    public Histogram.HistorgramPair update(Number object, Histogram.HistorgramPair cache, Object... others) {
        // TODO Auto-generated method stub
        if (object == null) {
            return cache;
        }
        if (cache == null) {
            cache = new Histogram.HistorgramPair();
        }

        cache.addValue(object.doubleValue());
        cache.addTimes(1l);

        return cache;
    }

    @Override
    public Histogram.HistorgramPair updateBatch(Histogram.HistorgramPair object, Histogram.HistorgramPair cache, Object... objects) {
        // TODO Auto-generated method stub
        if (object == null) {
            return cache;
        }
        if (cache == null) {
            cache = new Histogram.HistorgramPair();
        }

        cache.addValue(object.getSum());
        cache.addTimes(object.getTimes());

        return cache;
    }

}
