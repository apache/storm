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

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import com.sun.jersey.api.container.grizzly2.GrizzlyServerFactory;
import com.sun.jersey.api.core.ClassNamesResourceConfig;
import org.apache.storm.ui.resources.StormApiResource;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.servlet.ServletRegistration;
import org.glassfish.grizzly.servlet.WebappContext;
import com.sun.jersey.spi.container.servlet.ServletContainer;


/**
 * Main class.
 *
 */
public class Main {
    // Base URI the Grizzly HTTP server will listen on
    public static final String BASE_URI = "http://randshands.corp.ne1.yahoo.com:4443/";

    private static final String JERSEY_SERVLET_CONTEXT_PATH = "";

    /**
     * Starts Grizzly HTTP server exposing JAX-RS resources defined in this application.
     * @return Grizzly HTTP server.
     */
    public static HttpServer startServer() throws IOException {
        // create a resource config that scans for JAX-RS resources and providers
        // in org.apache.storm.ui package
        final Map<String, String > initParams = new HashMap();

        initParams.put("com.sun.jersey.config.property.packages", "org.apache.storm.ui.resources");
        // create and start a new instance of grizzly http server
        // exposing the Jersey application at BASE_URI


        
        return GrizzlyServerFactory.createHttpServer(URI.create(BASE_URI),
                new HttpHandler() {

                    @Override
                    public void service(Request rqst, Response rspns) throws Exception {
                        rspns.setStatus(404, "Not found");
                        rspns.getWriter().write("404: not found");
                    }
                });

    }

    /**
     * Main method.
     * @param args Unused
     * @throws IOException Ok for the UI server to go down
     */
    public static void main(final String[] args) throws IOException {
        final HttpServer server = startServer();

        WebappContext context = new WebappContext("WebappContext", JERSEY_SERVLET_CONTEXT_PATH);
        ServletRegistration registration = context.addServlet("ServletContainer", ServletContainer.class);
        registration.setInitParameter(ServletContainer.RESOURCE_CONFIG_CLASS,
                ClassNamesResourceConfig.class.getName());
        registration.setInitParameter(ClassNamesResourceConfig.PROPERTY_CLASSNAMES, StormApiResource.class.getName());
        registration.addMapping("/*");
        context.deploy(server);

        System.out.println(String.format("Jersey app started with WADL available at "
                + "%sapplication.wadl\nHit enter to stop it...", BASE_URI));
        System.in.read();
        server.stop();
    }
}

