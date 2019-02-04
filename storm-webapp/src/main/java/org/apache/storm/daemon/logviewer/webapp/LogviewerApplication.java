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

package org.apache.storm.daemon.logviewer.webapp;

import static org.apache.storm.DaemonConfig.LOGVIEWER_APPENDER_NAME;

import java.io.File;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.storm.daemon.common.AuthorizationExceptionMapper;
import org.apache.storm.daemon.logviewer.handler.LogviewerLogDownloadHandler;
import org.apache.storm.daemon.logviewer.handler.LogviewerLogPageHandler;
import org.apache.storm.daemon.logviewer.handler.LogviewerLogSearchHandler;
import org.apache.storm.daemon.logviewer.handler.LogviewerProfileHandler;
import org.apache.storm.daemon.logviewer.utils.ResourceAuthorizer;
import org.apache.storm.daemon.logviewer.utils.WorkerLogs;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.IHttpCredentialsPlugin;
import org.apache.storm.security.auth.ServerAuthUtils;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;

@ApplicationPath("")
public class LogviewerApplication extends Application {
    private static Map<String, Object> stormConf;
    private static StormMetricsRegistry metricsRegistry;
    private final Set<Object> singletons = new HashSet<>();

    /**
     * Constructor.
     */
    public LogviewerApplication() {
        String logRoot = ConfigUtils.workerArtifactsRoot(stormConf);
        String daemonLogRoot = logRootDir(ObjectReader.getString(stormConf.get(LOGVIEWER_APPENDER_NAME)));

        ResourceAuthorizer resourceAuthorizer = new ResourceAuthorizer(stormConf);
        WorkerLogs workerLogs = new WorkerLogs(stormConf, Paths.get(logRoot), metricsRegistry);

        LogviewerLogPageHandler logviewer = new LogviewerLogPageHandler(logRoot, daemonLogRoot, workerLogs, resourceAuthorizer,
            metricsRegistry);
        LogviewerProfileHandler profileHandler = new LogviewerProfileHandler(logRoot, resourceAuthorizer, metricsRegistry);
        LogviewerLogDownloadHandler logDownloadHandler = new LogviewerLogDownloadHandler(logRoot, daemonLogRoot,
                workerLogs, resourceAuthorizer, metricsRegistry);
        LogviewerLogSearchHandler logSearchHandler = new LogviewerLogSearchHandler(stormConf, Paths.get(logRoot), Paths.get(daemonLogRoot),
                resourceAuthorizer, metricsRegistry);
        IHttpCredentialsPlugin httpCredsHandler = ServerAuthUtils.getUiHttpCredentialsPlugin(stormConf);

        singletons.add(new LogviewerResource(logviewer, profileHandler, logDownloadHandler, logSearchHandler,
            httpCredsHandler, metricsRegistry));
        singletons.add(new AuthorizationExceptionMapper());
    }
    
    @Override
    public Set<Object> getSingletons() {
        return singletons;
    }

    /**
     * Spot to inject storm configuration before initializing LogviewerApplication instance.
     *
     * @param stormConf storm configuration
     * @param metricRegistry The metrics registry
     */
    public static void setup(Map<String, Object> stormConf, StormMetricsRegistry metricRegistry) {
        LogviewerApplication.stormConf = stormConf;
        LogviewerApplication.metricsRegistry = metricRegistry;
    }

    /**
     * Given an appender name, as configured, get the parent directory of the appender's log file.
     * Note that if anything goes wrong, this will throw an Error and exit.
     */
    private String logRootDir(String appenderName) {
        Appender appender = ((LoggerContext) LogManager.getContext()).getConfiguration().getAppender(appenderName);
        if (appenderName != null && appender != null && RollingFileAppender.class.isInstance(appender)) {
            return new File(((RollingFileAppender) appender).getFileName()).getParent();
        } else {
            throw new RuntimeException("Log viewer could not find configured appender, or the appender is not a FileAppender. "
                    + "Please check that the appender name configured in storm and log4j agree.");
        }
    }

}
