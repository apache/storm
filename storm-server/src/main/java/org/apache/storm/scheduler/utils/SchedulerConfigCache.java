/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.scheduler.utils;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.DaemonConfig;
import org.apache.storm.StormTimer;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class to cache the scheduler config and refresh after the cache expires.
 */
public class SchedulerConfigCache<T> {

    private static final Logger LOG = LoggerFactory.getLogger(SchedulerConfigCache.class);

    private final Reloadable<T> reloader;
    private AtomicReference<T> schedulerConfigAtomicReference;
    private long lastUpdateTimestamp;
    long configCacheExpirationMs;

    public SchedulerConfigCache(Map<String, Object> conf, Reloadable<T> reloader) {
        schedulerConfigAtomicReference = new AtomicReference<>();
        lastUpdateTimestamp = 0;
        this.reloader = reloader;
        configCacheExpirationMs = ObjectReader.getInt(conf.get(DaemonConfig.SCHEDULER_CONFIG_CACHE_EXPIRATION_SECS), 60) * 1000L;
    }

    public void prepare() {
        //refresh the cache here to make sure cache is available at very beginning
        refresh();
    }

    /**
     * Refresh the config only after the cache expires.
     * This is not thread-safe and should only be called in single thread.
     */
    public void refresh() {
        long current = Time.currentTimeMillis();
        if (lastUpdateTimestamp <= 0 || current > lastUpdateTimestamp + configCacheExpirationMs) {
            LOG.debug("refreshing scheduler config since cache is expired");
            T updatedConfig = reloader.reload();
            schedulerConfigAtomicReference.set(updatedConfig);
            lastUpdateTimestamp = current;
        } else {
            LOG.debug("skip refreshing scheduler config since cache is not yet expired;");
        }
    }

    /**
     * Get the scheduler config from cache.
     * This method is thead-safe and can be called in multiple threads.
     * @return the scheduler config
     */
    public T get() {
        return schedulerConfigAtomicReference.get();
    }

    public interface Reloadable<T> {
        /**
         * Reload and return the configs.
         * @return reloaded configs. It can't be null.
         */
        T reload();
    }
}
