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

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeCacheMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class TimeoutMemCache implements JStormCache {
    private static final long serialVersionUID = 705938812240167583L;
    private static Logger LOG = LoggerFactory.getLogger(TimeoutMemCache.class);

    protected int defaultTimeout;
    protected final TreeMap<Integer, TimeCacheMap<String, Object>> cacheWindows = new TreeMap<Integer, TimeCacheMap<String, Object>>();

    public TimeoutMemCache() {
    }

    protected void registerCacheWindow(int timeoutSecond) {
        synchronized (this) {
            if (cacheWindows.get(timeoutSecond) == null) {
                TimeCacheMap<String, Object> cacheWindow = new TimeCacheMap<String, Object>(timeoutSecond);
                cacheWindows.put(timeoutSecond, cacheWindow);

                LOG.info("Successfully register CacheWindow: " + timeoutSecond);
            } else {
                LOG.info("CacheWindow: " + timeoutSecond + " has been registered");
            }
        }
    }

    @Override
    public void init(Map<Object, Object> conf) {
        this.defaultTimeout = ConfigExtension.getDefaultCacheTimeout(conf);
        registerCacheWindow(defaultTimeout);

        List<Object> list = (List) ConfigExtension.getCacheTimeoutList(conf);
        if (list != null) {
            for (Object obj : list) {
                Integer timeoutSecond = JStormUtils.parseInt(obj);
                if (timeoutSecond == null) {
                    continue;
                }
                registerCacheWindow(timeoutSecond);
            }
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public Object get(String key) {
        // @@@ TODO
        // in order to improve performance, it can be query from defaultWindow firstly, then others
        for (TimeCacheMap<String, Object> cacheWindow : cacheWindows.values()) {
            Object ret = cacheWindow.get(key);
            if (ret != null) {
                return ret;
            }
        }
        return null;
    }

    @Override
    public void getBatch(Map<String, Object> map) {
        for (String key : map.keySet()) {
            Object obj = get(key);
            map.put(key, obj);
        }
    }

    @Override
    public void remove(String key) {
        for (TimeCacheMap<String, Object> cacheWindow : cacheWindows.values()) {
            Object ret = cacheWindow.remove(key);
            if (ret != null) {
                return;
            }
        }
    }

    @Override
    public void removeBatch(Collection<String> keys) {
        for (String key : keys) {
            remove(key);
        }
    }

    @Override
    public void put(String key, Object value, int timeoutSecond) {
        Entry<Integer, TimeCacheMap<String, Object>> ceilingEntry = cacheWindows.ceilingEntry(timeoutSecond);
        if (ceilingEntry == null) {
            put(key, value);
        } else {
            remove(key);
            ceilingEntry.getValue().put(key, value);
        }
    }

    @Override
    public void put(String key, Object value) {
        remove(key);
        TimeCacheMap<String, Object> bestWindow = cacheWindows.get(defaultTimeout);
        bestWindow.put(key, value);
    }

    @Override
    public void putBatch(Map<String, Object> map) {
        for (Entry<String, Object> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void putBatch(Map<String, Object> map, int timeoutSeconds) {
        for (Entry<String, Object> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue(), timeoutSeconds);
        }
    }

    public int getDefaultTimeout() {
        return defaultTimeout;
    }

    public void setDefaultTimeout(int defaultTimeout) {
        this.defaultTimeout = defaultTimeout;
    }

}
