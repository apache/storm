/**
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
package org.apache.storm.daemon.drpc;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.daemon.drpc.webapp.DRPCApplication;
import org.apache.storm.daemon.drpc.webapp.ReqContextFilter;
import org.apache.storm.generated.DistributedRPC;
import org.apache.storm.generated.DistributedRPCInvocations;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.IHttpCredentialsPlugin;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.storm.security.auth.ThriftServer;
import org.apache.storm.ui.FilterConfiguration;
import org.apache.storm.ui.UIHelpers;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.shade.org.eclipse.jetty.server.Server;
import org.apache.storm.shade.org.eclipse.jetty.servlet.FilterHolder;
import org.apache.storm.shade.org.eclipse.jetty.servlet.FilterMapping;
import org.apache.storm.shade.org.eclipse.jetty.servlet.ServletContextHandler;
import org.apache.storm.shade.org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.shade.com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;

public class DRPCServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DRPCServer.class);
    private final static Meter meterShutdownCalls = StormMetricsRegistry.registerMeter("drpc:num-shutdown-calls");
   
    //TODO in the future this might be better in a common webapp location
    public static void addRequestContextFilter(ServletContextHandler context, String configName, Map<String, Object> conf) {
        IHttpCredentialsPlugin auth = AuthUtils.GetHttpCredentialsPlugin(conf, (String)conf.get(configName));
        ReqContextFilter filter = new ReqContextFilter(auth);
        context.addFilter(new FilterHolder(filter), "/*", FilterMapping.ALL);
    }
 
    private static ThriftServer mkHandlerServer(final DistributedRPC.Iface service, Integer port, Map<String, Object> conf) {
        ThriftServer ret = null;
        if (port != null && port > 0) {
            ret = new ThriftServer(conf, new DistributedRPC.Processor<>(service),
                    ThriftConnectionType.DRPC);
        }
        return ret;
    }

    private static ThriftServer mkInvokeServer(final DistributedRPCInvocations.Iface service, int port, Map<String, Object> conf) {
        return new ThriftServer(conf, new DistributedRPCInvocations.Processor<>(service),
                ThriftConnectionType.DRPC_INVOCATIONS);
    }
    
    private static Server mkHttpServer(Map<String, Object> conf, DRPC drpc) {
        Integer drpcHttpPort = (Integer) conf.get(Config.DRPC_HTTP_PORT);
        Server ret = null;
        if (drpcHttpPort != null && drpcHttpPort > 0) {
            LOG.info("Starting RPC HTTP servers...");
            String filterClass = (String) (conf.get(Config.DRPC_HTTP_FILTER));
            @SuppressWarnings("unchecked")
            Map<String, String> filterParams = (Map<String, String>) (conf.get(Config.DRPC_HTTP_FILTER_PARAMS));
            FilterConfiguration filterConfiguration = new FilterConfiguration(filterClass, filterParams);
            final List<FilterConfiguration> filterConfigurations = Arrays.asList(filterConfiguration);
            final Integer httpsPort = Utils.getInt(conf.get(Config.DRPC_HTTPS_PORT), 0);
            final String httpsKsPath = (String) (conf.get(Config.DRPC_HTTPS_KEYSTORE_PATH));
            final String httpsKsPassword = (String) (conf.get(Config.DRPC_HTTPS_KEYSTORE_PASSWORD));
            final String httpsKsType = (String) (conf.get(Config.DRPC_HTTPS_KEYSTORE_TYPE));
            final String httpsKeyPassword = (String) (conf.get(Config.DRPC_HTTPS_KEY_PASSWORD));
            final String httpsTsPath = (String) (conf.get(Config.DRPC_HTTPS_TRUSTSTORE_PATH));
            final String httpsTsPassword = (String) (conf.get(Config.DRPC_HTTPS_TRUSTSTORE_PASSWORD));
            final String httpsTsType = (String) (conf.get(Config.DRPC_HTTPS_TRUSTSTORE_TYPE));
            final Boolean httpsWantClientAuth = (Boolean) (conf.get(Config.DRPC_HTTPS_WANT_CLIENT_AUTH));
            final Boolean httpsNeedClientAuth = (Boolean) (conf.get(Config.DRPC_HTTPS_NEED_CLIENT_AUTH));

            //TODO a better way to do this would be great.
            DRPCApplication.setup(drpc);
            ret = UIHelpers.jettyCreateServer(drpcHttpPort, null, httpsPort);
            
            UIHelpers.configSsl(ret, httpsPort, httpsKsPath, httpsKsPassword, httpsKsType, httpsKeyPassword, httpsTsPath, httpsTsPassword, httpsTsType,
                    httpsNeedClientAuth, httpsWantClientAuth);
            
            ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
            context.setContextPath("/");
            ret.setHandler(context);

            ServletHolder jerseyServlet = context.addServlet(ServletContainer.class, "/*");
            jerseyServlet.setInitOrder(1);
            jerseyServlet.setInitParameter("javax.ws.rs.Application", DRPCApplication.class.getName());
            
            UIHelpers.configFilters(context, filterConfigurations);
            addRequestContextFilter(context, Config.DRPC_HTTP_CREDS_PLUGIN, conf);
        }
        return ret;
    }
    
    private final DRPC _drpc;
    private final ThriftServer _handlerServer;
    private final ThriftServer _invokeServer;
    private final Server _httpServer;
    private boolean _closed = false;

    public DRPCServer(Map<String, Object> conf) {
        _drpc = new DRPC(conf);
        DRPCThrift thrift = new DRPCThrift(_drpc);
        _handlerServer = mkHandlerServer(thrift, Utils.getInt(conf.get(Config.DRPC_PORT), null), conf);
        _invokeServer = mkInvokeServer(thrift, Utils.getInt(conf.get(Config.DRPC_INVOCATIONS_PORT), 3773), conf);
        _httpServer = mkHttpServer(conf, _drpc);
    }

    @VisibleForTesting
    void start() throws Exception {
        LOG.info("Starting Distributed RPC servers...");
        new Thread(() -> _invokeServer.serve()).start();

        if (_httpServer != null) {
            _httpServer.start();
        }
        
        if (_handlerServer != null) {
            _handlerServer.serve();
        } else {
            _httpServer.join();
        }
    }

    @Override
    public synchronized void close() {
        if (!_closed) {
            //This is kind of useless...
            meterShutdownCalls.mark();

            if (_handlerServer != null) {
                _handlerServer.stop();
            }

            if (_invokeServer != null) {
                _invokeServer.stop();
            }

            //TODO this is causing issues...
            //if (_httpServer != null) {
            //    _httpServer.destroy();
            //}
            
            _drpc.close();
            _closed  = true;
        }
    }
    
    public static void main(String [] args) throws Exception {
        Utils.setupDefaultUncaughtExceptionHandler();
        Map<String, Object> conf = ConfigUtils.readStormConfig();
        try (DRPCServer server = new DRPCServer(conf)) {
            Utils.addShutdownHookWithForceKillIn1Sec(() -> server.close());
            StormMetricsRegistry.startMetricsReporters(conf);
            server.start();
        }
    }
}
