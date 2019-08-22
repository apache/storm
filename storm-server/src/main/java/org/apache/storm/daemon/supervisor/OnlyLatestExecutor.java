/*
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

package org.apache.storm.daemon.supervisor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This allows you to submit a Runnable with a key.  If the previous submission for that key has not yet run, it will be replaced with the
 * latest one.
 */
public class OnlyLatestExecutor<K> {
    private static final Logger LOG = LoggerFactory.getLogger(OnlyLatestExecutor.class);
    private final Executor exec;
    private final ConcurrentMap<K, Runnable> latest;

    public OnlyLatestExecutor(Executor exec) {
        this.exec = exec;
        latest = new ConcurrentHashMap<>();
    }

    /**
     * Run something in the future, but replace it with the latest if it is taking too long.
     *
     * @param key what to use to dedupe things.
     * @param r   what you want to run.
     */
    public void execute(final K key, Runnable r) {
        Runnable old = latest.put(key, r);
        if (old == null) {
            //It was not there before so we need to run it.
            exec.execute(() -> {
                Runnable run = latest.remove(key);
                if (run != null) {
                    run.run();
                }
            });
        } else {
            LOG.debug("Replacing runnable for {} - {}", key, r);
        }
    }
}
