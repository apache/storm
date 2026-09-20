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

package org.apache.storm.utils;

import java.util.Map;
import java.util.concurrent.ThreadFactory;
import org.apache.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the {@link ThreadFactory} used by Storm's blocking I/O thread pools.
 *
 * <p>When {@link Config#STORM_VIRTUAL_THREADS_ENABLED} is true the factory creates virtual threads.
 * Otherwise it creates non-daemon platform threads with normal priority, matching
 * {@code Executors.defaultThreadFactory()}. In both modes threads are named
 * {@code <namePrefix>-<n>} with {@code n} starting at 0.
 */
public final class StormThreadFactory {

    private static final Logger LOG = LoggerFactory.getLogger(StormThreadFactory.class);

    private StormThreadFactory() {
    }

    /**
     * Create a thread factory for a pool.
     *
     * @param conf       cluster or topology configuration
     * @param namePrefix prefix for the thread names, e.g. {@code "nimbus-thrift-handler"}
     * @return a factory producing virtual or platform threads depending on the configuration
     */
    public static ThreadFactory create(Map<String, Object> conf, String namePrefix) {
        String prefix = namePrefix + "-";
        if (isVirtualEnabled(conf)) {
            return Thread.ofVirtual().name(prefix, 0).factory();
        }
        return Thread.ofPlatform()
                .name(prefix, 0)
                .daemon(false)
                .priority(Thread.NORM_PRIORITY)
                .factory();
    }

    /**
     * Whether virtual threads are enabled for blocking I/O pools.
     *
     * <p>Boolean and String values are honoured (strings are parsed as boolean). Any other type
     * is treated as disabled. This method never throws.
     */
    public static boolean isVirtualEnabled(Map<String, Object> conf) {
        if (conf == null) {
            return false;
        }
        Object value = conf.get(Config.STORM_VIRTUAL_THREADS_ENABLED);
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        LOG.warn("Ignoring {} value of type {} for {}; expected a boolean", value, value.getClass().getName(),
            Config.STORM_VIRTUAL_THREADS_ENABLED);
        return false;
    }
}
