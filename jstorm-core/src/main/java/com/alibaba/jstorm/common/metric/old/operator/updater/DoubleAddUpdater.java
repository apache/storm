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

import com.google.common.util.concurrent.AtomicDouble;

public class DoubleAddUpdater implements Updater<AtomicDouble> {
    private static final long serialVersionUID = -1293565961076552462L;

    @Override
    public AtomicDouble update(Number object, AtomicDouble cache, Object... others) {
        // TODO Auto-generated method stub
        if (cache == null) {
            cache = new AtomicDouble(0.0);
        }
        if (object != null) {
            cache.addAndGet(object.doubleValue());
        }
        return cache;
    }

    @Override
    public AtomicDouble updateBatch(AtomicDouble object, AtomicDouble cache, Object... objects) {
        // TODO Auto-generated method stub
        return update(object, cache, objects);
    }

}
