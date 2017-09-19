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

import static org.apache.storm.DaemonConfig.UI_HEADER_BUFFER_BYTES;

import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.logviewer.utils.DirectoryCleaner;
import org.apache.storm.daemon.logviewer.utils.LogCleaner;
import org.apache.storm.daemon.logviewer.utils.WorkerLogs;
import org.apache.storm.daemon.logviewer.webapp.LogviewerApplication;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.ui.FilterConfiguration;
import org.apache.storm.ui.UIHelpers;
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
    private static final Meter meterShutdownCalls = StormMetricsRegistry.registerMeter("drpc:num-shutdown-calls");
    public static final String STATIC_RESOURCE_DIRECTORY_PATH = "./public";

    private static Server mkHttpServer(Map<String, Object> conf) {
        Integer logviewerHttpPort = (Integer) conf.get(DaemonConfig.LOGVIEWER_PORT);
        Server ret = null;
        if (logviewerHttpPort != null && logviewerHttpPort >= 0) {
            LOG.info("Starting Logviewer HTTP servers...");
            Integer headerBufferSize = ObjectReader.getInt(conf.get(UI_HEADER_BUFFER_BYTES));
            String filterClass = (String) (conf.get(DaemonConfig.UI_FILTER));
            @SuppressWarnings("unchecked")
            Map<String, String> filterParams = (Map<String, String>) (conf.get(DaemonConfig.UI_FILTER_PARAMS));
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

            LogviewerApplication.setup(conf);
            ret = UIHelpers.jettyCreateServer(logviewerHttpPort, null, httpsPort);

            UIHelpers.configSsl(ret, httpsPort, httpsKsPath, httpsKsPassword, httpsKsType, httpsKeyPassword,
                    httpsTsPath, httpsTsPassword, httpsTsType, httpsNeedClientAuth, httpsWantClientAuth);

            ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
            try {
                context.setBaseResource(Resource.newResource(STATIC_RESOURCE_DIRECTORY_PATH));
            } catch (IOException e) {
                throw new RuntimeException("Can't locate static resource directory " + STATIC_RESOURCE_DIRECTORY_PATH);
            }

            context.setContextPath("/");
            ret.setHandler(context);

            ServletHolder holderPwd = new ServletHolder("default", DefaultServlet.class);
            holderPwd.setInitOrder(1);
            context.addServlet(holderPwd,"/");

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
     */
    public LogviewerServer(Map<String, Object> conf) {
        httpServer = mkHttpServer(conf);
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
            //This is kind of useless...
            meterShutdownCalls.mark();

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
        Map<String, Object> conf = Utils.readStormConfig();

        String logRoot = ConfigUtils.workerArtifactsRoot(conf);
        File logRootFile = new File(logRoot);
        WorkerLogs workerLogs = new WorkerLogs(conf, logRootFile);
        DirectoryCleaner directoryCleaner = new DirectoryCleaner();

        try (LogviewerServer server = new LogviewerServer(conf);
             LogCleaner logCleaner = new LogCleaner(conf, workerLogs, directoryCleaner, logRootFile)) {
            Utils.addShutdownHookWithForceKillIn1Sec(() -> server.close());
            logCleaner.start();
            StormMetricsRegistry.startMetricsReporters(conf);
            server.start();
            server.awaitTermination();
        }
    }
}
