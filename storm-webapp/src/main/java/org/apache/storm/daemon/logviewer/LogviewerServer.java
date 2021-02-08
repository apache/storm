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

package org.apache.storm.daemon.logviewer;

import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.logviewer.utils.DirectoryCleaner;
import org.apache.storm.daemon.logviewer.utils.ExceptionMeterNames;
import org.apache.storm.daemon.logviewer.utils.LogCleaner;
import org.apache.storm.daemon.logviewer.utils.WorkerLogs;
import org.apache.storm.daemon.logviewer.webapp.LogviewerApplication;
import org.apache.storm.daemon.ui.FilterConfiguration;
import org.apache.storm.daemon.ui.UIHelpers;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main entry of Logviewer.
 */
public class LogviewerServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(LogviewerServer.class);
    private static final String stormHome = System.getProperty(ConfigUtils.STORM_HOME);
    public static final String STATIC_RESOURCE_DIRECTORY_PATH = stormHome + "/public";
    private final Meter meterShutdownCalls;

    private static Server mkHttpServer(StormMetricsRegistry metricsRegistry, Map<String, Object> conf) {
        Integer logviewerHttpPort = (Integer) conf.get(DaemonConfig.LOGVIEWER_PORT);
        Server ret = null;
        if (logviewerHttpPort != null && logviewerHttpPort >= 0) {
            LOG.info("Starting Logviewer HTTP servers...");
            String filterParamKey = DaemonConfig.LOGVIEWER_FILTER_PARAMS;
            String filterClass = (String) (conf.get(DaemonConfig.LOGVIEWER_FILTER));
            if (StringUtils.isBlank(filterClass)) {
                filterClass = (String) (conf.get(DaemonConfig.UI_FILTER));
                filterParamKey = DaemonConfig.UI_FILTER_PARAMS;
            }
            @SuppressWarnings("unchecked")
            Map<String, String> filterParams = (Map<String, String>) (conf.get(filterParamKey));
            FilterConfiguration filterConfiguration = new FilterConfiguration(filterClass, filterParams);
            final List<FilterConfiguration> filterConfigurations = Arrays.asList(filterConfiguration);

            final Integer httpsPort = ObjectReader.getInt(conf.get(DaemonConfig.LOGVIEWER_HTTPS_PORT), 0);
            final String httpsKsPath = (String) (conf.get(DaemonConfig.LOGVIEWER_HTTPS_KEYSTORE_PATH));
            final String httpsKsPassword = (String) (conf.get(DaemonConfig.LOGVIEWER_HTTPS_KEYSTORE_PASSWORD));
            final String httpsKsType = (String) (conf.get(DaemonConfig.LOGVIEWER_HTTPS_KEYSTORE_TYPE));
            final String httpsKeyPassword = (String) (conf.get(DaemonConfig.LOGVIEWER_HTTPS_KEY_PASSWORD));
            final String httpsTsPath = (String) (conf.get(DaemonConfig.LOGVIEWER_HTTPS_TRUSTSTORE_PATH));
            final String httpsTsPassword = (String) (conf.get(DaemonConfig.LOGVIEWER_HTTPS_TRUSTSTORE_PASSWORD));
            final String httpsTsType = (String) (conf.get(DaemonConfig.LOGVIEWER_HTTPS_TRUSTSTORE_TYPE));
            final Boolean httpsWantClientAuth = (Boolean) (conf.get(DaemonConfig.LOGVIEWER_HTTPS_WANT_CLIENT_AUTH));
            final Boolean httpsNeedClientAuth = (Boolean) (conf.get(DaemonConfig.LOGVIEWER_HTTPS_NEED_CLIENT_AUTH));
            final Boolean disableHttpBinding = (Boolean) (conf.get(DaemonConfig.LOGVIEWER_DISABLE_HTTP_BINDING));
            final boolean enableSslReload = ObjectReader.getBoolean(conf.get(DaemonConfig.LOGVIEWER_HTTPS_ENABLE_SSL_RELOAD), false);


            LogviewerApplication.setup(conf, metricsRegistry);
            ret = UIHelpers.jettyCreateServer(logviewerHttpPort, null, httpsPort, disableHttpBinding);

            UIHelpers.configSsl(ret, httpsPort, httpsKsPath, httpsKsPassword, httpsKsType, httpsKeyPassword,
                    httpsTsPath, httpsTsPassword, httpsTsType, httpsNeedClientAuth, httpsWantClientAuth, enableSslReload);

            ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
            try {
                context.setBaseResource(Resource.newResource(STATIC_RESOURCE_DIRECTORY_PATH));
            } catch (IOException e) {
                throw new RuntimeException("Can't locate static resource directory " + STATIC_RESOURCE_DIRECTORY_PATH);
            }

            context.setWelcomeFiles(new String[]{"logviewer.html"});
            context.setContextPath("/");
            ret.setHandler(context);

            ServletHolder holderPwd = new ServletHolder("default", DefaultServlet.class);
            holderPwd.setInitOrder(1);
            context.addServlet(holderPwd, "/");

            ServletHolder jerseyServlet = context.addServlet(ServletContainer.class, "/api/v1/*");
            jerseyServlet.setInitOrder(2);
            jerseyServlet.setInitParameter("javax.ws.rs.Application", LogviewerApplication.class.getName());

            UIHelpers.configFilters(context, filterConfigurations);
        }
        return ret;
    }

    private final Server httpServer;
    private boolean closed = false;

    /**
     * Constructor.
     * @param conf Logviewer conf for the servers
     * @param metricsRegistry The metrics registry
     */
    public LogviewerServer(Map<String, Object> conf, StormMetricsRegistry metricsRegistry) {
        httpServer = mkHttpServer(metricsRegistry, conf);
        meterShutdownCalls = metricsRegistry.registerMeter("logviewer:num-shutdown-calls");
        ExceptionMeterNames.registerMeters(metricsRegistry);
    }

    @VisibleForTesting
    void start() throws Exception {
        LOG.info("Starting Logviewer...");
        if (httpServer != null) {
            httpServer.start();
        }
    }

    @VisibleForTesting
    void awaitTermination() throws InterruptedException {
        httpServer.join();
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            //TODO this is causing issues...
            //if (httpServer != null) {
            //    httpServer.destroy();
            //}

            closed = true;
        }
    }

    /**
     * Main method to start the server.
     */
    public static void main(String [] args) throws Exception {
        Utils.setupDefaultUncaughtExceptionHandler();
        Map<String, Object> conf = ConfigUtils.readStormConfig();

        StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
        String logRoot = ConfigUtils.workerArtifactsRoot(conf);
        File logRootDir = new File(logRoot);
        logRootDir.mkdirs();
        WorkerLogs workerLogs = new WorkerLogs(conf, logRootDir.toPath(), metricsRegistry);
        DirectoryCleaner directoryCleaner = new DirectoryCleaner(metricsRegistry);

        try (LogviewerServer server = new LogviewerServer(conf, metricsRegistry);
             LogCleaner logCleaner = new LogCleaner(conf, workerLogs, directoryCleaner, logRootDir.toPath(), metricsRegistry)) {
            metricsRegistry.startMetricsReporters(conf);
            Utils.addShutdownHookWithForceKillIn1Sec(() -> {
                server.meterShutdownCalls.mark();
                metricsRegistry.stopMetricsReporters();
                server.close();
            });
            logCleaner.start();

            server.start();
            server.awaitTermination();
        }
    }
}
