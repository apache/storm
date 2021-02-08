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

package org.apache.storm.daemon.ui;

import static org.apache.storm.utils.ConfigUtils.FILE_SEPARATOR;
import static org.apache.storm.utils.ConfigUtils.STORM_HOME;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import javax.servlet.DispatcherType;
import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.drpc.webapp.ReqContextFilter;
import org.apache.storm.daemon.ui.exceptionmappers.AuthorizationExceptionMapper;
import org.apache.storm.daemon.ui.exceptionmappers.DefaultExceptionMapper;
import org.apache.storm.daemon.ui.exceptionmappers.NotAliveExceptionMapper;
import org.apache.storm.daemon.ui.filters.AuthorizedUserFilter;
import org.apache.storm.daemon.ui.filters.HeaderResponseFilter;
import org.apache.storm.daemon.ui.filters.HeaderResponseServletFilter;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.IHttpCredentialsPlugin;
import org.apache.storm.security.auth.ServerAuthUtils;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class.
 *
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class UIServer {

    public static final Logger LOG = LoggerFactory.getLogger(UIServer.class);

    public static final String STORM_API_URL_PREFIX = "/api/v1/";

    /**
     * addRequestContextFilter.
     * @param context context
     * @param configName configName
     * @param conf conf
     */
    public static void addRequestContextFilter(ServletContextHandler context,
                                               String configName, Map<String, Object> conf) {
        IHttpCredentialsPlugin auth = ServerAuthUtils.getHttpCredentialsPlugin(conf, (String) conf.get(configName));
        ReqContextFilter filter = new ReqContextFilter(auth);
        context.addFilter(new FilterHolder(filter), "/*", EnumSet.allOf(DispatcherType.class));
    }

    /**
     * main.
     * @param args args
     */
    public static void main(String[] args) {

        Map<String, Object> conf = ConfigUtils.readStormConfig();
        int headerBufferSize = (int) conf.get(DaemonConfig.UI_HEADER_BUFFER_BYTES);
        final Integer httpsPort = ObjectReader.getInt(conf.get(DaemonConfig.UI_HTTPS_PORT), 0);
        final String httpsKsPath = (String) (conf.get(DaemonConfig.UI_HTTPS_KEYSTORE_PATH));
        final String httpsKsPassword = (String) (conf.get(DaemonConfig.UI_HTTPS_KEYSTORE_PASSWORD));
        final String httpsKsType = (String) (conf.get(DaemonConfig.UI_HTTPS_KEYSTORE_TYPE));
        final String httpsKeyPassword = (String) (conf.get(DaemonConfig.UI_HTTPS_KEY_PASSWORD));
        final String httpsTsPath = (String) (conf.get(DaemonConfig.UI_HTTPS_TRUSTSTORE_PATH));
        final String httpsTsPassword = (String) (conf.get(DaemonConfig.UI_HTTPS_TRUSTSTORE_PASSWORD));
        final String httpsTsType = (String) (conf.get(DaemonConfig.UI_HTTPS_TRUSTSTORE_TYPE));
        final Boolean httpsWantClientAuth = (Boolean) (conf.get(DaemonConfig.UI_HTTPS_WANT_CLIENT_AUTH));
        final Boolean httpsNeedClientAuth = (Boolean) (conf.get(DaemonConfig.UI_HTTPS_NEED_CLIENT_AUTH));
        final Boolean disableHttpBinding = (Boolean) (conf.get(DaemonConfig.UI_DISABLE_HTTP_BINDING));
        final boolean enableSslReload = ObjectReader.getBoolean(conf.get(DaemonConfig.UI_HTTPS_ENABLE_SSL_RELOAD), false);

        Server jettyServer =
                UIHelpers.jettyCreateServer(
                        (int) conf.get(DaemonConfig.UI_PORT), null, httpsPort, headerBufferSize, disableHttpBinding);

        UIHelpers.configSsl(jettyServer, httpsPort, httpsKsPath, httpsKsPassword, httpsKsType, httpsKeyPassword,
                httpsTsPath, httpsTsPassword, httpsTsType, httpsNeedClientAuth, httpsWantClientAuth, enableSslReload);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        jettyServer.setHandler(context);

        FilterConfiguration filterConfiguration =
                new FilterConfiguration(
                        (String) conf.get(DaemonConfig.UI_FILTER),
                        (Map<String, String>) conf.get(DaemonConfig.UI_FILTER_PARAMS)
                );
        final List<FilterConfiguration> filterConfigurationList = Arrays.asList(filterConfiguration);

        UIHelpers.configFilters(context, filterConfigurationList);

        StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
        ResourceConfig resourceConfig =
            new ResourceConfig()
                .packages("org.apache.storm.daemon.ui.resources")
                .registerInstances(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        super.bind(metricsRegistry).to(StormMetricsRegistry.class);
                    }
                })
                .register(AuthorizedUserFilter.class)
                .register(HeaderResponseFilter.class)
                .register(AuthorizationExceptionMapper.class)
                .register(NotAliveExceptionMapper.class)
                .register(DefaultExceptionMapper.class);

        ServletHolder jerseyServlet = new ServletHolder(new ServletContainer(resourceConfig));
        jerseyServlet.setInitOrder(0);
        context.addServlet(jerseyServlet, STORM_API_URL_PREFIX + "*");
        addRequestContextFilter(context, DaemonConfig.UI_HTTP_CREDS_PLUGIN, conf);


        // add special pathspec of static content mapped to the homePath
        ServletHolder holderHome = new ServletHolder("static-home", DefaultServlet.class);

        String packagedStaticFileLocation = System.getProperty(STORM_HOME) + FILE_SEPARATOR + "public/";

        if (Files.exists(Paths.get(packagedStaticFileLocation))) {
            holderHome.setInitParameter("resourceBase", packagedStaticFileLocation);
        } else {
            LOG.warn("Cannot find static file directory in " +  packagedStaticFileLocation
                    + " - assuming that UIServer is being launched"
                    + "in a development environment and not from a packaged release");
            String developmentStaticFileLocation =
                    UIServer.class.getProtectionDomain().getCodeSource().getLocation().getPath()
                            + "WEB-INF";
            if (Files.exists(Paths.get(developmentStaticFileLocation))) {
                holderHome.setInitParameter("resourceBase", developmentStaticFileLocation);
            } else {
                throw new RuntimeException("Cannot find static file directory in development "
                        + "location " + developmentStaticFileLocation);
            }
        }

        holderHome.setInitParameter("dirAllowed", "true");
        holderHome.setInitParameter("pathInfoOnly", "true");
        context.addFilter(new FilterHolder(new HeaderResponseServletFilter(metricsRegistry)), "/*", EnumSet.allOf(DispatcherType.class));
        context.addServlet(holderHome, "/*");


        // Lastly, the default servlet for root content (always needed, to satisfy servlet spec)
        ServletHolder holderPwd = new ServletHolder("default", DefaultServlet.class);
        holderPwd.setInitParameter("dirAllowed", "true");
        context.addServlet(holderPwd, "/");

        metricsRegistry.startMetricsReporters(conf);
        Utils.addShutdownHookWithForceKillIn1Sec(metricsRegistry::stopMetricsReporters);
        try {
            jettyServer.start();
            jettyServer.join();
        } catch (Throwable t) {
            LOG.error("Exception in UIServer: ", t);
            throw new RuntimeException(t);
        }

    }
}

