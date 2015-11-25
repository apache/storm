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
import java.util.concurrent.atomic.AtomicLong;

public class LongSumMerger implements Merger<AtomicLong> {
    private static final long serialVersionUID = -3500779273677666691L;

    @Override
    public AtomicLong merge(Collection<AtomicLong> objs, AtomicLong unflushed, Object... others) {
        AtomicLong ret = new AtomicLong(0);
        if (unflushed != null) {
            ret.addAndGet(unflushed.get());
        }

        for (AtomicLong item : objs) {
            if (item == null) {
                continue;
            }
            ret.addAndGet(item.get());
        }
        return ret;
    }

}
