/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.daemon.worker;

import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.storm.generated.LogConfig;
import org.apache.storm.generated.LogLevel;
import org.apache.storm.generated.LogLevelAction;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;


public class LogConfigManagerTest {
    private static final Logger LOG = LoggerFactory.getLogger(LogConfigManagerTest.class);

    public static LogLevel ll() {
        return new LogLevel(LogLevelAction.UPDATE);
    }

    public static LogLevel ll(long epoc) {
        LogLevel ret = ll();
        ret.set_reset_log_level_timeout_epoch(epoc);
        return ret;
    }

    public static LogLevel ll(String target, long epoc) {
        LogLevel ret = ll();
        ret.set_target_log_level(target);
        ret.set_reset_log_level_timeout_epoch(epoc);
        return ret;
    }

    public static LogLevel ll(String target, String reset, long epoc) {
        LogLevel ret = ll();
        ret.set_target_log_level(target);
        ret.set_reset_log_level(reset);
        ret.set_reset_log_level_timeout_epoch(epoc);
        return ret;
    }

    @Test
    public void testLogResetShouldNotTriggerForFutureTime() {
        try (SimulatedTime t = new SimulatedTime()) {
            long theFuture = Time.currentTimeMillis() + 1000;
            TreeMap<String, LogLevel> config = new TreeMap<>();
            config.put("foo", ll(theFuture));
            AtomicReference<TreeMap<String, LogLevel>> atomConf = new AtomicReference<>(config);

            LogConfigManager underTest = new LogConfigManagerUnderTest(atomConf);
            underTest.resetLogLevels();
            assertNotNull(atomConf.get());
        }
    }

    @Test
    public void testLogResetTriggersForPastTime() {
        try (SimulatedTime t = new SimulatedTime()) {
            long past = Time.currentTimeMillis() - 1000;
            TreeMap<String, LogLevel> config = new TreeMap<>();
            config.put("foo", ll("INFO", "WARN", past));
            AtomicReference<TreeMap<String, LogLevel>> atomConf = new AtomicReference<>(config);

            LogConfigManager underTest = new LogConfigManagerUnderTest(atomConf);
            underTest.resetLogLevels();
            assertEquals(new TreeMap<>(), atomConf.get());
        }
    }

    @Test
    public void testLogResetResetsDoesNothingForEmptyLogConfig() {
        TreeMap<String, LogLevel> config = new TreeMap<>();
        AtomicReference<TreeMap<String, LogLevel>> atomConf = new AtomicReference<>(config);

        LogConfigManager underTest = spy(new LogConfigManagerUnderTest(atomConf));
        underTest.resetLogLevels();
        assertEquals(new TreeMap<>(), atomConf.get());
        verify(underTest, never()).setLoggerLevel(anyObject(), anyObject(), anyObject());
    }

    @Test
    public void testLogResetResetsRootLoggerIfSet() {
        try (SimulatedTime t = new SimulatedTime()) {
            long past = Time.currentTimeMillis() - 1000;
            TreeMap<String, LogLevel> config = new TreeMap<>();
            config.put(LogManager.ROOT_LOGGER_NAME, ll("DEBUG", "WARN", past));
            AtomicReference<TreeMap<String, LogLevel>> atomConf = new AtomicReference<>(config);

            LogConfigManager underTest = spy(new LogConfigManagerUnderTest(atomConf));
            underTest.resetLogLevels();
            assertEquals(new TreeMap<>(), atomConf.get());
            verify(underTest).setLoggerLevel(anyObject(), eq(LogManager.ROOT_LOGGER_NAME), eq("WARN"));
        }
    }

    @Test
    public void testLogResetProperlyResetLogLevelAfterTimeout() throws InterruptedException {
        try (SimulatedTime t = new SimulatedTime()) {
            long inThirtySeconds = Time.currentTimeMillis() + 30_000;
            TreeMap<String, LogLevel> config = new TreeMap<>();
            config.put(LogManager.ROOT_LOGGER_NAME, ll("DEBUG", "WARN", inThirtySeconds));
            AtomicReference<TreeMap<String, LogLevel>> atomConf = new AtomicReference<>(config);

            LogConfigManager underTest = spy(new LogConfigManagerUnderTest(atomConf));

            TreeMap<String, LogLevel> expected = new TreeMap<>();
            LogLevel logLevel = new LogLevel(LogLevelAction.UPDATE);
            logLevel.set_target_log_level("DEBUG");
            logLevel.set_reset_log_level("WARN");
            logLevel.set_reset_log_level_timeout_epoch(30_000);
            expected.put(LogManager.ROOT_LOGGER_NAME, logLevel);

            underTest.resetLogLevels();
            assertEquals(expected, atomConf.get());
            verify(underTest, never()).setLoggerLevel(anyObject(), eq(LogManager.ROOT_LOGGER_NAME), anyString());

            // 11 seconds passed by, not timing out
            Time.advanceTimeSecs(11);
            underTest.resetLogLevels();
            assertEquals(expected, atomConf.get());
            verify(underTest, never()).setLoggerLevel(anyObject(), eq(LogManager.ROOT_LOGGER_NAME), anyString());

            // 22 seconds passed by, still not timing out
            Time.advanceTimeSecs(11);
            underTest.resetLogLevels();
            assertEquals(expected, atomConf.get());
            verify(underTest, never()).setLoggerLevel(anyObject(), eq(LogManager.ROOT_LOGGER_NAME), anyString());

            // 33 seconds passed by, timed out
            Time.advanceTimeSecs(11);
            underTest.resetLogLevels();
            assertEquals(new TreeMap<>(), atomConf.get());
            verify(underTest).setLoggerLevel(anyObject(), eq(LogManager.ROOT_LOGGER_NAME), eq("WARN"));
        }
    }

    @Test
    public void testLogResetsNamedLoggersWithPastTimeout() {
        try (SimulatedTime t = new SimulatedTime()) {
            long past = Time.currentTimeMillis() - 1000;
            TreeMap<String, LogLevel> config = new TreeMap<>();
            config.put("my_debug_logger", ll("DEBUG", "INFO", past));
            config.put("my_info_logger", ll("INFO", "WARN", past));
            config.put("my_error_logger", ll("ERROR", "INFO", past));
            AtomicReference<TreeMap<String, LogLevel>> atomConf = new AtomicReference<>(config);

            LogConfigManager underTest = spy(new LogConfigManagerUnderTest(atomConf));
            underTest.resetLogLevels();
            assertEquals(new TreeMap<>(), atomConf.get());
            verify(underTest).setLoggerLevel(anyObject(), eq("my_debug_logger"), eq("INFO"));
            verify(underTest).setLoggerLevel(anyObject(), eq("my_info_logger"), eq("WARN"));
            verify(underTest).setLoggerLevel(anyObject(), eq("my_error_logger"), eq("INFO"));
        }
    }

    @Test
    public void testProcessRootLogLevelToDebugSetsLoggerAndTimeout2() {
        try (SimulatedTime t = new SimulatedTime()) {
            LogConfig mockConfig = new LogConfig();
            AtomicReference<TreeMap<String, LogLevel>> mockConfigAtom = new AtomicReference<>(null);

            long inThirtySeconds = Time.currentTimeMillis() + 30_000;
            mockConfig.put_to_named_logger_level("ROOT", ll("DEBUG", inThirtySeconds));

            LogConfigManager underTest = spy(new LogConfigManagerUnderTest(mockConfigAtom));
            underTest.processLogConfigChange(mockConfig);
            // test that the set-logger-level function was not called
            LOG.info("Tests {}", mockConfigAtom.get());
            verify(underTest).setLoggerLevel(anyObject(), eq(""), eq("DEBUG"));

            LogLevel rootResult = mockConfigAtom.get().get(LogManager.ROOT_LOGGER_NAME);
            assertNotNull(rootResult);
            assertEquals(LogLevelAction.UPDATE, rootResult.get_action());
            assertEquals("DEBUG", rootResult.get_target_log_level());
            // defaults to INFO level when the logger isn't found previously
            assertEquals("INFO", rootResult.get_reset_log_level());
            assertEquals(inThirtySeconds, rootResult.get_reset_log_level_timeout_epoch());
        }
    }

    @Test
    public void testProcessLogConfigChangeThrowsNullPointerExceptionWhenTargetLogLevelIsNotSet() {
        LogConfigManager logConfigManager = new LogConfigManager();

        LogConfig logConfig = new LogConfig();
        LogLevel logLevel = new LogLevel();
        logLevel.set_action(LogLevelAction.UPDATE);
        logLevel.set_reset_log_level("INFO");
        logConfig.put_to_named_logger_level("RESET_LOG", logLevel);

        assertThrows(NullPointerException.class, () -> logConfigManager.processLogConfigChange(logConfig));
    }

    @Test
    public void testProcessLogConfigChangeExecutesSuccessfullyWhenTargetLogLevelIsSet() {
        LogConfigManager logConfigManager = new LogConfigManager();

        LogConfig logConfig = new LogConfig();
        LogLevel logLevel = new LogLevel();
        logLevel.set_action(LogLevelAction.UPDATE);
        logLevel.set_target_log_level("DEBUG");
        logConfig.put_to_named_logger_level("TARGET_LOG", logLevel);

        logConfigManager.processLogConfigChange(logConfig);
    }

    @Test
    public void testProcessRootLogLevelToDebugSetsLoggerAndTimeout() {
        try (SimulatedTime t = new SimulatedTime()) {
            LogConfig mockConfig = new LogConfig();
            AtomicReference<TreeMap<String, LogLevel>> mockConfigAtom = new AtomicReference<>(null);
            long inThirtySeconds = Time.currentTimeMillis() + 30_000;
            mockConfig.put_to_named_logger_level("ROOT", ll("DEBUG", inThirtySeconds));
            mockConfig.put_to_named_logger_level("my_debug_logger", ll("DEBUG", inThirtySeconds));
            mockConfig.put_to_named_logger_level("my_info_logger", ll("INFO", inThirtySeconds));
            mockConfig.put_to_named_logger_level("my_error_logger", ll("ERROR", inThirtySeconds));

            LOG.info("Tests {}", mockConfigAtom.get());

            LogConfigManager underTest = spy(new LogConfigManagerUnderTest(mockConfigAtom));
            underTest.processLogConfigChange(mockConfig);

            verify(underTest).setLoggerLevel(anyObject(), eq(""), eq("DEBUG"));
            verify(underTest).setLoggerLevel(anyObject(), eq("my_debug_logger"), eq("DEBUG"));
            verify(underTest).setLoggerLevel(anyObject(), eq("my_info_logger"), eq("INFO"));
            verify(underTest).setLoggerLevel(anyObject(), eq("my_error_logger"), eq("ERROR"));
        }
    }

    public static class LogConfigManagerUnderTest extends LogConfigManager {
        public LogConfigManagerUnderTest() {
            super();
        }

        public LogConfigManagerUnderTest(AtomicReference<TreeMap<String, LogLevel>> latestLogConfig) {
            super(latestLogConfig);
        }

        @Override
        public void setLoggerLevel(LoggerContext logContext, String loggerName, String newLevelStr) {
            //NOOP, we don't actually want to change log levels for tests
        }
    }
}
