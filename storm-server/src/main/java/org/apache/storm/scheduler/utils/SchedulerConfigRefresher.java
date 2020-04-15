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

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.DaemonConfig;
import org.apache.storm.StormTimer;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerConfigRefresher<T> {

    private static final Logger LOG = LoggerFactory.getLogger(SchedulerConfigRefresher.class);

    private final StormTimer configRefreshTimer;
    private AtomicReference<T> schedulerConfigAtomicReference;
    private final Reloadable<T> reloader;
    private final Map<String, Object> conf;

    public SchedulerConfigRefresher(Map<String, Object> conf, Reloadable<T> reloader) {
        configRefreshTimer = new StormTimer("scheduler-config-refresher", (t, e) -> {
            LOG.error("Error while refreshing scheduler config", e);
            Utils.exitProcess(20, "Error while refreshing scheduler config");
        });
        schedulerConfigAtomicReference = new AtomicReference<>();
        this.reloader = reloader;
        this.conf = conf;
    }

    public void start() {
        //fetch the config in a blocking manner to make sure config is available at very beginning
        refreshConfig();

        int configRefreshIntervalSec = ObjectReader.getInt(conf.get(DaemonConfig.SCHEDULER_CONFIG_REFRESH_INTERVAL_SECS), 60);
        configRefreshTimer.scheduleRecurring(configRefreshIntervalSec, configRefreshIntervalSec, this::refreshConfig);
    }

    private void refreshConfig() {
        LOG.debug("refreshing scheduler config");
        T updatedConfig = reloader.reload();
        schedulerConfigAtomicReference.set(updatedConfig);
    }

    public T getConfig() {
        return schedulerConfigAtomicReference.get();
    }

    public void shutdown() {
        try {
            configRefreshTimer.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public interface Reloadable<T> {
        /**
         * Reload and return the configs.
         * @return reloaded configs. It can't be null.
         */
        T reload();
    }
}
