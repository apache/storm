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

package org.apache.storm.daemon.worker;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.storm.generated.LogConfig;
import org.apache.storm.generated.LogLevel;
import org.apache.storm.generated.LogLevelAction;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogConfigManager {

    private static final Logger LOG = LoggerFactory.getLogger(LogConfigManager.class);

    private final AtomicReference<TreeMap<String, LogLevel>> latestLogConfig;
    private final Map<String, Level> originalLogLevels;

    public LogConfigManager() {
        this(new AtomicReference<>(new TreeMap<>()));
    }

    public LogConfigManager(AtomicReference<TreeMap<String, LogLevel>> latestLogConfig) {
        this.latestLogConfig = latestLogConfig;
        this.originalLogLevels = getLoggerLevels();
        LOG.info("Started with log levels: {}", originalLogLevels);
    }

    public void processLogConfigChange(LogConfig logConfig) {
        if (null != logConfig) {
            LOG.debug("Processing received log config: {}", logConfig);
            TreeMap<String, LogLevel> loggers = new TreeMap<>(logConfig.get_named_logger_level());
            LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
            Map<String, LogLevel> newLogConfigs = new HashMap<>();
            for (Map.Entry<String, LogLevel> entry : loggers.entrySet()) {
                String msgLoggerName = entry.getKey();
                msgLoggerName = ("ROOT".equalsIgnoreCase(msgLoggerName)) ? LogManager.ROOT_LOGGER_NAME : msgLoggerName;
                LogLevel loggerLevel = entry.getValue();
                // the new-timeouts map now contains logger => timeout
                if (loggerLevel.is_set_reset_log_level_timeout_epoch()) {
                    LogLevel copy = new LogLevel(loggerLevel);
                    if (originalLogLevels.containsKey(msgLoggerName)) {
                        copy.set_reset_log_level(originalLogLevels.get(msgLoggerName).name());
                    } else {
                        copy.set_reset_log_level(Level.INFO.name());
                    }

                    newLogConfigs.put(msgLoggerName, copy);
                }

            }

            // Look for deleted log timeouts
            TreeMap<String, LogLevel> latestConf = latestLogConfig.get();
            if (latestConf != null) {
                for (String loggerName : latestConf.descendingKeySet()) {
                    if (!newLogConfigs.containsKey(loggerName)) {
                        // if we had a timeout, but the timeout is no longer active
                        setLoggerLevel(logContext, loggerName, latestConf.get(loggerName).get_reset_log_level());

                    }
                }
            }

            // apply new log settings we just received
            // the merged configs are only for the reset logic
            for (String loggerName : new TreeSet<>(logConfig.get_named_logger_level().keySet())) {
                LogLevel logLevel = logConfig.get_named_logger_level().get(loggerName);
                loggerName = ("ROOT".equalsIgnoreCase(loggerName)) ? LogManager.ROOT_LOGGER_NAME : loggerName;
                LogLevelAction action = logLevel.get_action();
                if (action == LogLevelAction.UPDATE) {
                    setLoggerLevel(logContext, loggerName, logLevel.get_target_log_level());
                }

            }

            logContext.updateLoggers();
            latestLogConfig.set(new TreeMap<>(newLogConfigs));
            LOG.debug("New merged log config is {}", latestLogConfig.get());
        }
    }

    // function called on timer to reset log levels last set to DEBUG
    // also called from processLogConfigChange
    public void resetLogLevels() {
        TreeMap<String, LogLevel> latestLogLevelMap = latestLogConfig.get();

        LOG.debug("Resetting log levels: Latest log config is {}", latestLogLevelMap);

        LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);

        for (String loggerName : latestLogLevelMap.descendingKeySet()) {
            LogLevel loggerSetting = latestLogLevelMap.get(loggerName);
            long timeout = loggerSetting.get_reset_log_level_timeout_epoch();
            String resetLogLevel = loggerSetting.get_reset_log_level();
            if (timeout < Time.currentTimeMillis()) {
                LOG.info("{}: Resetting level to {}", loggerName, resetLogLevel);
                setLoggerLevel(loggerContext, loggerName, resetLogLevel);
                latestLogConfig.getAndUpdate(input -> {
                    TreeMap<String, LogLevel> result = new TreeMap<>(input);
                    result.remove(loggerName);
                    return result;
                });
            }
        }
        loggerContext.updateLoggers();
    }

    public Map<String, Level> getLoggerLevels() {
        Configuration loggerConfig = ((LoggerContext) LogManager.getContext(false)).getConfiguration();
        Map<String, Level> logLevelMap = new HashMap<>();
        for (Map.Entry<String, LoggerConfig> entry : loggerConfig.getLoggers().entrySet()) {
            logLevelMap.put(entry.getKey(), entry.getValue().getLevel());
        }
        return logLevelMap;
    }

    public void setLoggerLevel(LoggerContext logContext, String loggerName, String newLevelStr) {
        Level newLevel = Level.getLevel(newLevelStr);
        Configuration configuration = logContext.getConfiguration();
        LoggerConfig loggerConfig = configuration.getLoggerConfig(loggerName);
        if (loggerConfig.getName().equalsIgnoreCase(loggerName)) {
            LOG.info("Setting {} log level to: {}", loggerConfig, newLevel);
            loggerConfig.setLevel(newLevel);
        } else {
            // create a new config. Make it additive (true) s.t. inherit parents appenders
            LoggerConfig newLoggerConfig = new LoggerConfig(loggerName, newLevel, true);
            LOG.info("Adding config for: {} with level: {}", newLoggerConfig, newLevel);
            configuration.addLogger(loggerName, newLoggerConfig);
        }
    }
}
