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

import com.alibaba.jstorm.utils.JStormUtils;

public class AddUpdater<T extends Number> implements Updater<T> {
    private static final long serialVersionUID = -7955740095421752763L;

    @SuppressWarnings("unchecked")
    @Override
    public T update(Number object, T cache, Object... others) {
        // TODO Auto-generated method stub
        return (T) JStormUtils.add(cache, object);
    }

    @Override
    public T updateBatch(T object, T cache, Object... objects) {
        // TODO Auto-generated method stub
        return (T) JStormUtils.add(cache, object);
    }

}
