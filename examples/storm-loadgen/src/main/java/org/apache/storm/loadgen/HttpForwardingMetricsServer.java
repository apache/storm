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

package org.apache.storm.loadgen;

import com.esotericsoftware.kryo.io.Input;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.storm.metric.api.IMetricsConsumer.DataPoint;
import org.apache.storm.metric.api.IMetricsConsumer.TaskInfo;
import org.apache.storm.serialization.KryoValuesDeserializer;
import org.apache.storm.utils.Utils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * A server that can listen for metrics from the HttpForwardingMetricsConsumer.
 */
public abstract class HttpForwardingMetricsServer {
    private Map<String, Object> conf;
    private Server server = null;
    private int port = -1;
    private String url = null;

    ThreadLocal<KryoValuesDeserializer> des = new ThreadLocal<KryoValuesDeserializer>() {
        @Override
        protected KryoValuesDeserializer initialValue() {
            return new KryoValuesDeserializer(conf);
        }
    };

    private class MetricsCollectionServlet extends HttpServlet {
        @Override
        protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            Input in = new Input(request.getInputStream());
            List<Object> metrics = des.get().deserializeFrom(in);
            handle((TaskInfo) metrics.get(0), (Collection<DataPoint>) metrics.get(1), (String) metrics.get(2));
            response.setStatus(HttpServletResponse.SC_OK);
        }
    }

    /**
     * Constructor.
     * @param conf the configuration for storm.
     */
    public HttpForwardingMetricsServer(Map<String, Object> conf) {
        this.conf = Utils.readStormConfig();
        if (conf != null) {
            this.conf.putAll(conf);
        }
    }

    //This needs to be thread safe
    public abstract void handle(TaskInfo taskInfo, Collection<DataPoint> dataPoints, String topologyId);

    /**
     * Start the server.
     * @param port the port it shuld listen on, or null/<= 0 to pick a free ephemeral port.
     */
    public void serve(Integer port) {
        try {
            if (server != null) {
                throw new RuntimeException("The server is already running");
            }
    
            if (port == null || port <= 0) {
                ServerSocket s = new ServerSocket(0);
                port = s.getLocalPort();
                s.close();
            }
            server = new Server(port);
            this.port = port;
            url = "http://" + InetAddress.getLocalHost().getHostName() + ":" + this.port + "/";
 
            ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
            context.setContextPath("/");
            server.setHandler(context);
 
            context.addServlet(new ServletHolder(new MetricsCollectionServlet()), "/*");

            server.start();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void serve() {
        serve(null);
    }

    public int getPort() {
        return port;
    }

    public String getUrl() {
        return url;
    }
}
