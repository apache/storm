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

package org.apache.storm.ui;

import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.drpc.webapp.ReqContextFilter;
import org.apache.storm.security.auth.IHttpCredentialsPlugin;
import org.apache.storm.security.auth.ServerAuthUtils;
import org.apache.storm.ui.filters.AuthorizedUserFilter;
import org.apache.storm.utils.Utils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.servlet.DispatcherType;
import java.util.EnumSet;
import java.util.Map;

/**
 * Main class.
 *
 */
public class Main {

    public static void addRequestContextFilter(ServletContextHandler context,
                                               String configName, Map<String, Object> conf) {
        IHttpCredentialsPlugin auth = ServerAuthUtils.getHttpCredentialsPlugin(conf, (String) conf.get(configName));
        ReqContextFilter filter = new ReqContextFilter(auth);
        context.addFilter(new FilterHolder(filter), "/*", EnumSet.allOf(DispatcherType.class));
    }

    public static void main(String[] args) throws Exception {

        Map<String, Object> conf = Utils.readStormConfig();

        ResourceConfig config = new ResourceConfig().packages("org.apache.storm.ui.resources")
                .register(AuthorizedUserFilter.class);


        ServletHolder jerseyServlet = new ServletHolder(new ServletContainer(config));
        jerseyServlet.setInitOrder(0);


        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");


        Server jettyServer = new Server(4443);
        jettyServer.setHandler(context);

        addRequestContextFilter(context, DaemonConfig.DRPC_HTTP_CREDS_PLUGIN, conf);

        context.addServlet(jerseyServlet, "/*");

        try {
            jettyServer.start();
            jettyServer.join();
        } finally {
            jettyServer.destroy();
        }
    }
}

