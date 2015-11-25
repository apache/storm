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
package com.alibaba.jstorm.cache;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import com.alibaba.jstorm.client.ConfigExtension;

public interface JStormCache extends Serializable {
    public static final String TAG_TIMEOUT_LIST = ConfigExtension.CACHE_TIMEOUT_LIST;

    void init(Map<Object, Object> conf) throws Exception;

    void cleanup();

    Object get(String key);

    void getBatch(Map<String, Object> map);

    void remove(String key);

    void removeBatch(Collection<String> keys);

    void put(String key, Object value, int timeoutSecond);

    void put(String key, Object value);

    void putBatch(Map<String, Object> map);

    void putBatch(Map<String, Object> map, int timeoutSeconds);
}
