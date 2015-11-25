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

import com.alibaba.jstorm.common.metric.old.operator.convert.AtomicLongToLong;
import com.alibaba.jstorm.common.metric.old.operator.merger.LongSumMerger;
import com.alibaba.jstorm.common.metric.old.operator.updater.LongAddUpdater;
import com.alibaba.jstorm.common.metric.old.window.Metric;

import java.util.concurrent.atomic.AtomicLong;

public class LongCounter extends Metric<Long, AtomicLong> {
    private static final long serialVersionUID = -1362345159511508074L;

    public LongCounter() {
        super.defaultValue = new AtomicLong(0);
        super.updater = new LongAddUpdater();
        super.merger = new LongSumMerger();
        super.convertor = new AtomicLongToLong();

        init();
    }

}
