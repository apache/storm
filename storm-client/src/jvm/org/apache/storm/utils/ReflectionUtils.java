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

import java.util.List;
import java.util.Map;
import org.apache.storm.Config;

public class ReflectionUtils {
    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static ReflectionUtils _instance = new ReflectionUtils();

    /**
     * Provide an instance of this class for delegates to use.  To mock out delegated methods, provide an instance of a subclass that
     * overrides the implementation of the delegated method.
     *
     * @param u a Utils instance
     * @return the previously set instance
     */
    public static ReflectionUtils setInstance(ReflectionUtils u) {
        ReflectionUtils oldInstance = _instance;
        _instance = u;
        return oldInstance;
    }

    @SuppressWarnings("unchecked")
    public static <T> T newInstance(String klass) {
        try {
            return newInstance((Class<T>) Class.forName(klass));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T newInstance(Class<T> klass) {
        return _instance.newInstanceImpl(klass);
    }

    @SuppressWarnings("unchecked")
    public static <T> T newInstance(String klass, Map<String, Object> conf) {
        try {
            return newInstance((Class<T>) Class.forName(klass), conf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T newInstance(Class<T> klass, Map<String, Object> conf) {
        try {
            try {
                return klass.getConstructor(Map.class).newInstance(conf);
            } catch (Exception e) {
                return klass.newInstance();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T newSchedulerStrategyInstance(String klass, Map<String, Object> conf) {
        List<String> allowedSchedulerStrategies = (List<String>) conf.get(Config.NIMBUS_SCHEDULER_STRATEGY_CLASS_WHITELIST);
        if (allowedSchedulerStrategies == null || allowedSchedulerStrategies.contains(klass)) {
            return newInstance(klass);
        } else {
            throw new DisallowedStrategyException(klass, allowedSchedulerStrategies);
        }
    }

    // Non-static impl methods exist for mocking purposes.
    public <T> T newInstanceImpl(Class<T> klass) {
        try {
            return klass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
