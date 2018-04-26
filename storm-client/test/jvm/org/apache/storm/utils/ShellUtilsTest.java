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

package org.apache.storm.utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.multilang.ShellMsg;
import org.apache.storm.task.TopologyContext;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ShellUtilsTest {

    private Map<String, Object> configureLogHandler(String className) {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_MULTILANG_LOG_HANDLER, className);
        return conf;
    }

    /**
     * A null config will throw IllegalArgumentException.
     */
    @Test(expected = IllegalArgumentException.class)
    public void getLogHandler_nullConf() {
        ShellUtils.getLogHandler(null);
    }

    /**
     * If a log handler is not configured, {@link DefaultShellLogHandler}
     * will be returned.
     */
    @Test
    public void getLogHandler_notConfigured() {
        ShellLogHandler logHandler = ShellUtils.getLogHandler(new HashMap<String, Object>());
        assertTrue(logHandler.getClass() == DefaultShellLogHandler.class);
    }

    /**
     * If a log handler cannot be found, a {@link RuntimeException} will be
     * thrown with {@link ClassNotFoundException} as the cause.
     */
    @Test
    public void getLogHandler_notFound() {
        try {
            configureLogHandler("class.not.Found");
        } catch (RuntimeException e) {
            assert (e.getCause().getClass() == ClassNotFoundException.class);
        }
    }

    /**
     * If a log handler is not an instance of {@link ShellLogHandler}, a
     * {@link RuntimeException} will be thrown with {@link ClassCastException}
     * as the cause.
     */
    @Test
    public void getLogHandler_notAShellLogHandler() {
        try {
            configureLogHandler("java.lang.String");
        } catch (RuntimeException e) {
            assert (e.getCause().getClass() == ClassCastException.class);
        }
    }

    /**
     * If a log handler is correctly configured, it will be returned.
     */
    @Test
    public void getLogHandler_customHandler() {
        Map<String, Object> conf = configureLogHandler("org.apache.storm.utils.ShellUtilsTest$CustomShellLogHandler");
        ShellLogHandler logHandler = ShellUtils.getLogHandler(conf);
        assertTrue(logHandler.getClass() == CustomShellLogHandler.class);
    }

    public static class CustomShellLogHandler implements ShellLogHandler {
        @Override
        public void setUpContext(Class<?> owner, ShellProcess process,
                                 TopologyContext context) {
        }

        @Override
        public void log(ShellMsg msg) {
        }
    }
}