/**
 * Licensed to the Apache Software Foundation (ASF)
 * under one or more contributor license agreements.
 * See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.ui;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.DispatcherType;
import javax.servlet.Servlet;
import javax.ws.rs.core.SecurityContext;

import com.google.common.collect.Lists;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.DaemonConfig;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.BoltAggregateStats;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.CommonAggregateStats;
import org.apache.storm.generated.ComponentAggregateStats;
import org.apache.storm.generated.ComponentPageInfo;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GetInfoOptions;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.LogConfig;
import org.apache.storm.generated.LogLevel;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NumErrorsChoice;
import org.apache.storm.generated.OwnerResourceSummary;
import org.apache.storm.generated.ProfileAction;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SupervisorPageInfo;
import org.apache.storm.generated.SupervisorSummary;
import org.apache.storm.generated.TopologyHistoryInfo;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologyPageInfo;
import org.apache.storm.generated.TopologyStats;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.generated.WorkerSummary;
import org.apache.storm.logging.filters.AccessLoggingFilter;
import org.apache.storm.stats.StatsUtil;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.TopologySpoutLag;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.VersionInfo;
import org.apache.storm.utils.WebAppUtils;
import org.apache.thrift.TException;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.json.simple.JSONValue;

public class UiHelpers {

    private static final Object[][] PRETTY_SEC_DIVIDERS = {
        new Object[]{ "s", 60 },
        new Object[]{ "m", 60 },
        new Object[]{ "h", 24 },
        new Object[]{ "d", null }
    };

    private static final Object[][] PRETTY_MS_DIVIDERS = {
        new Object[]{ "ms", 1000 },
        new Object[]{ "s", 60 },
        new Object[]{ "m", 60 },
        new Object[]{ "h", 24 },
        new Object[]{ "d", null }
    };

    /**
     * Prettigy uptime string.
     * @param val val.
     * @param dividers dividers.
     * @return prettified uptime string.
     */
    public static String prettyUptimeStr(String val, Object[][] dividers) {
        int uptime = Integer.parseInt(val);
        LinkedList<String> tmp = new LinkedList<>();
        for (Object[] divider : dividers) {
            if (uptime > 0) {
                String state = (String) divider[0];
                Integer div = (Integer) divider[1];
                if (div != null) {
                    tmp.addFirst(uptime % div + state);
                    uptime = uptime / div;
                } else {
                    tmp.addFirst(uptime + state);
                }
            }
        }
        return Joiner.on(" ").join(tmp);
    }

    /**
     * See above javadoc.
     * @param sec uptime in seconds.
     * @return prettified uptime string.
     */
    public static String prettyUptimeSec(String sec) {
        return prettyUptimeStr(sec, PRETTY_SEC_DIVIDERS);
    }

    public static String prettyUptimeSec(int secs) {
        return prettyUptimeStr(String.valueOf(secs), PRETTY_SEC_DIVIDERS);
    }

    public static String prettyUptimeMs(String ms) {
        return prettyUptimeStr(ms, PRETTY_MS_DIVIDERS);
    }

    public static String prettyUptimeMs(int ms) {
        return prettyUptimeStr(String.valueOf(ms), PRETTY_MS_DIVIDERS);
    }

    /**
     * url formatter for log links.
     * @param fmt string format
     * @param args hostname and other arguments.
     * @return string formatter
     */
    public static String urlFormat(String fmt, Object... args) {
        String[] argsEncoded = new String[args.length];
        for (int i = 0; i < args.length; i++) {
            argsEncoded[i] = URLEncoder.encode(String.valueOf(args[i]));
        }
        return String.format(fmt, argsEncoded);
    }

    /**
     * Prettified executor info.
     * @param e from Nimbus call
     * @return prettified executor info string
     */
    public static String prettyExecutorInfo(ExecutorInfo e) {
        return "[" + e.get_task_start() + "-" + e.get_task_end() + "]";
    }

    /**
     * Unauthorized user json.
     * @param user User id.
     * @return Unauthorized user json.
     */
    public static Map<String, Object> unauthorizedUserJson(String user) {
        return ImmutableMap.of(
            "error", "No Authorization",
            "errorMessage", String.format("User %s is not authorized.", user));
    }

    private static ServerConnector mkSslConnector(Server server, Integer port, String ksPath, String ksPassword, String ksType,
                                                  String keyPassword, String tsPath, String tsPassword, String tsType,
                                                  Boolean needClientAuth, Boolean wantClientAuth, Integer headerBufferSize) {
        SslContextFactory factory = new SslContextFactory();
        factory.setExcludeCipherSuites("SSL_RSA_WITH_RC4_128_MD5", "SSL_RSA_WITH_RC4_128_SHA");
        factory.setExcludeProtocols("SSLv3");
        factory.setRenegotiationAllowed(false);
        factory.setKeyStorePath(ksPath);
        factory.setKeyStoreType(ksType);
        factory.setKeyStorePassword(ksPassword);
        factory.setKeyManagerPassword(keyPassword);

        if (tsPath != null && tsPassword != null && tsType != null) {
            factory.setTrustStorePath(tsPath);
            factory.setTrustStoreType(tsType);
            factory.setTrustStorePassword(tsPassword);
        }

        if (needClientAuth != null && needClientAuth) {
            factory.setNeedClientAuth(true);
        } else if (wantClientAuth != null && wantClientAuth) {
            factory.setWantClientAuth(true);
        }

        HttpConfiguration httpsConfig = new HttpConfiguration();
        httpsConfig.addCustomizer(new SecureRequestCustomizer());
        if (null != headerBufferSize) {
            httpsConfig.setRequestHeaderSize(headerBufferSize);
        }
        ServerConnector sslConnector = new ServerConnector(server,
                                                           new SslConnectionFactory(factory, HttpVersion.HTTP_1_1.asString()),
                                                           new HttpConnectionFactory(httpsConfig));
        sslConnector.setPort(port);
        return sslConnector;
    }

    public static void configSsl(Server server, Integer port, String ksPath, String ksPassword, String ksType,
                                 String keyPassword, String tsPath, String tsPassword, String tsType,
                                 Boolean needClientAuth, Boolean wantClientAuth) {
        configSsl(server, port, ksPath, ksPassword, ksType, keyPassword,
                  tsPath, tsPassword, tsType, needClientAuth, wantClientAuth, null);
    }

    public static void configSsl(Server server, Integer port, String ksPath, String ksPassword, String ksType,
                                 String keyPassword, String tsPath, String tsPassword, String tsType,
                                 Boolean needClientAuth, Boolean wantClientAuth, Integer headerBufferSize) {
        if (port > 0) {
            server.addConnector(mkSslConnector(server, port, ksPath, ksPassword, ksType, keyPassword,
                                               tsPath, tsPassword, tsType, needClientAuth, wantClientAuth, headerBufferSize));
        }
    }

    public static FilterHolder corsFilterHandle() {
        FilterHolder filterHolder = new FilterHolder(new CrossOriginFilter());
        filterHolder.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "*");
        filterHolder.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "GET, POST, PUT");
        filterHolder.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM,
                                      "X-Requested-With, X-Requested-By, Access-Control-Allow-Origin, Content-Type, Content-Length, "
                                              + "Accept, Origin");
        filterHolder.setInitParameter(CrossOriginFilter.ACCESS_CONTROL_ALLOW_ORIGIN_HEADER, "*");
        return filterHolder;
    }

    public static FilterHolder mkAccessLoggingFilterHandle() {
        return new FilterHolder(new AccessLoggingFilter());
    }

    public static void configFilter(Server server, Servlet servlet, List<FilterConfiguration> filtersConfs) {
        configFilter(server, servlet, filtersConfs, null);
    }

    /**
     * Config filter.
     * @param server Server
     * @param servlet Servlet
     * @param filtersConfs FiltersConfs
     * @param params Filter params
     */
    public static void configFilter(Server server, Servlet servlet, List<FilterConfiguration> filtersConfs, Map<String, String> params) {
        if (filtersConfs != null) {
            ServletHolder servletHolder = new ServletHolder(servlet);
            servletHolder.setInitOrder(0);
            if (params != null) {
                servletHolder.setInitParameters(params);
            }
            ServletContextHandler context = new ServletContextHandler(server, "/");
            context.addServlet(servletHolder, "/");
            configFilters(context, filtersConfs);
            server.setHandler(context);
        }
    }

    /**
     * Config filters.
     * @param context Servlet context
     * @param filtersConfs filter confs
     */
    public static void configFilters(ServletContextHandler context, List<FilterConfiguration> filtersConfs) {
        context.addFilter(corsFilterHandle(), "/*", EnumSet.allOf(DispatcherType.class));
        for (FilterConfiguration filterConf : filtersConfs) {
            String filterName = filterConf.getFilterName();
            String filterClass = filterConf.getFilterClass();
            Map<String, String> filterParams = filterConf.getFilterParams();
            if (filterClass != null) {
                FilterHolder filterHolder = new FilterHolder();
                filterHolder.setClassName(filterClass);
                if (filterName != null) {
                    filterHolder.setName(filterName);
                } else {
                    filterHolder.setName(filterClass);
                }
                if (filterParams != null) {
                    filterHolder.setInitParameters(filterParams);
                } else {
                    filterHolder.setInitParameters(new HashMap<>());
                }
                context.addFilter(filterHolder, "/*", EnumSet.allOf(DispatcherType.class));
            }
        }
        context.addFilter(mkAccessLoggingFilterHandle(), "/*", EnumSet.allOf(DispatcherType.class));
    }

    /**
     * Construct a Jetty Server instance.
     */
    public static Server jettyCreateServer(Integer port, String host, Integer httpsPort) {
        return jettyCreateServer(port, host, httpsPort, null);
    }

    /**
     * Construct a Jetty Server instance.
     */
    public static Server jettyCreateServer(Integer port, String host, Integer httpsPort, Integer headerBufferSize) {
        Server server = new Server();

        if (httpsPort == null || httpsPort <= 0) {
            HttpConfiguration httpConfig = new HttpConfiguration();
            httpConfig.setSendDateHeader(true);
            if (null != headerBufferSize) {
                httpConfig.setRequestHeaderSize(headerBufferSize);
            }
            ServerConnector httpConnector = new ServerConnector(server, new HttpConnectionFactory(httpConfig));
            httpConnector.setPort(ObjectReader.getInt(port, 80));
            httpConnector.setIdleTimeout(200000);
            httpConnector.setHost(host);
            server.addConnector(httpConnector);
        }

        return server;
    }

    /**
     * Modified version of run-jetty
     * Assumes configurator sets handler.
     */
    public static void stormRunJetty(Integer port, String host, Integer httpsPort, Integer headerBufferSize,
                                     IConfigurator configurator) throws Exception {
        Server s = jettyCreateServer(port, host, httpsPort, headerBufferSize);
        if (configurator != null) {
            configurator.execute(s);
        }
        s.start();
    }

    public static void stormRunJetty(Integer port, Integer headerBufferSize, IConfigurator configurator) throws Exception {
        stormRunJetty(port, null, null, headerBufferSize, configurator);
    }

    public static String wrapJsonInCallback(String callback, String response) {
        return callback + "(" + response + ");";
    }

    public static Map getJsonResponseHeaders(String callback, Map headers) {
        Map<String, String> headersResult = new HashMap<>();
        headersResult.put("Cache-Control", "no-cache, no-store");
        headersResult.put("Access-Control-Allow-Origin", "*");
        headersResult.put("Access-Control-Allow-Headers",
                          "Content-Type, Access-Control-Allow-Headers, Access-Controler-Allow-Origin, "
                                  + "X-Requested-By, X-Csrf-Token, "
                                  + "Authorization, X-Requested-With");
        if (callback != null) {
            headersResult.put("Content-Type", "application/javascript;charset=utf-8");
        } else {
            headersResult.put("Content-Type", "application/json;charset=utf-8");
        }
        if (headers != null) {
            headersResult.putAll(headers);
        }
        return headersResult;
    }

    public static String getJsonResponseBody(Object data, String callback, boolean needSerialize) {
        String serializedData = needSerialize ? JSONValue.toJSONString(data) : (String) data;
        return callback != null ? wrapJsonInCallback(callback, serializedData) : serializedData;
    }

    /**
     * Converts exception into json map.
     * @param ex Exception to be converted.
     * @param statusCode Status code to be returned.
     * @return Map to be converted into json.
     */
    public static Map exceptionToJson(Exception ex, int statusCode) {
        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        return ImmutableMap.of(
                "error", statusCode
                        + " "
                        + HttpStatus.getMessage(statusCode),
                "errorMessage", sw.toString());
    }

    /**
     * Converts thrift call result into map fit for UI/api.
     * @param clusterSummary Obtained from Nimbus.
     * @param user User Making request
     * @param conf Storm Conf
     * @return Cluster Summary for display on UI/monitoring purposes via API
     */
    public static Map<String, Object> getClusterSummary(ClusterSummary clusterSummary, String user,
                                                        Map<String, Object> conf) {
        Map<String, Object> result = new HashMap();
        List<SupervisorSummary> supervisorSummaries = clusterSummary.get_supervisors();
        List<TopologySummary> topologySummaries = clusterSummary.get_topologies();

        Integer usedSlots =
                supervisorSummaries.stream().mapToInt(
                SupervisorSummary::get_num_used_workers).sum();
        Integer totalSlots =
                supervisorSummaries.stream().mapToInt(
                        SupervisorSummary::get_num_workers).sum();

        Integer totalTasks =
                topologySummaries.stream().mapToInt(
                        TopologySummary::get_num_tasks).sum();
        Integer totalExecutors =
                topologySummaries.stream().mapToInt(
                        TopologySummary::get_num_executors).sum();

        Double supervisorTotalMemory =
                supervisorSummaries.stream().mapToDouble(x -> x.get_total_resources().getOrDefault(
                        Constants.COMMON_TOTAL_MEMORY_RESOURCE_NAME,
                        x.get_total_resources().get(Config.SUPERVISOR_MEMORY_CAPACITY_MB)
                        )
                ).sum();

        Double supervisorTotalCpu =
                supervisorSummaries.stream().mapToDouble(x -> x.get_total_resources().getOrDefault(
                        Constants.COMMON_CPU_RESOURCE_NAME,
                        x.get_total_resources().get(Config.SUPERVISOR_CPU_CAPACITY)
                        )
                ).sum();

        Double supervisorUsedMemory =
                supervisorSummaries.stream().mapToDouble(SupervisorSummary::get_used_mem).sum();
        Double supervisorUsedCpu =
                supervisorSummaries.stream().mapToDouble(SupervisorSummary::get_used_cpu).sum();
        Double supervisorFragementedCpu =
                supervisorSummaries.stream().mapToDouble(
                        SupervisorSummary::get_fragmented_cpu).sum();
        Double supervisorFragmentedMem =
                supervisorSummaries.stream().mapToDouble(
                        SupervisorSummary::get_fragmented_mem).sum();


        result.put("user", user);
        result.put("stormVersion", VersionInfo.getVersion());
        result.put("supervisors", supervisorSummaries.size());
        result.put("topologies", clusterSummary.get_topologies_size());
        result.put("slotsUsed", usedSlots);
        result.put("slotsTotal", totalSlots);
        result.put("slotsFree", totalSlots - usedSlots);
        result.put("tasksTotal", totalTasks);
        result.put("totalExecutors", totalExecutors);

        result.put("totalMem", supervisorTotalMemory);
        result.put("totalCpu", supervisorTotalCpu);
        result.put("availMem", supervisorTotalMemory - supervisorUsedMemory);
        result.put("availCpu", supervisorTotalCpu - supervisorUsedCpu);
        result.put("fragmentedMem", supervisorFragmentedMem);
        result.put("fragmentedCpu", supervisorFragementedCpu);
        result.put("schedulerDisplayResource",
                conf.get(DaemonConfig.SCHEDULER_DISPLAY_RESOURCE));
        result.put("memAssignedPercentUtil", supervisorTotalMemory > 0
                ? ((supervisorTotalMemory - supervisorUsedMemory)  * 100.0)
                / supervisorTotalMemory : 0.0);
        result.put("cpuAssignedPercentUtil", supervisorTotalCpu > 0
                ? ((supervisorTotalCpu - supervisorUsedCpu) * 100.0)
                / supervisorTotalCpu : 0.0);
        result.put("bugtracker-url", conf.get(DaemonConfig.UI_PROJECT_BUGTRACKER_URL));
        result.put("central-log-url", conf.get(DaemonConfig.UI_CENTRAL_LOGGING_URL));
        return result;
    }

    /**
     * Prettify OwnerResourceSummary.
     * @param ownerResourceSummary
     * @return Map of prettified OwnerResourceSummary.
     */
    public static Map<String, Object> unpackOwnerResourceSummary(OwnerResourceSummary ownerResourceSummary) {
        Map<String, Object> result = new HashMap();

        Double memoryGuarantee = Double.valueOf(-1);
        if (ownerResourceSummary.is_set_memory_guarantee()) {
            memoryGuarantee = ownerResourceSummary.get_memory_guarantee();
        }

        Double cpuGuaranteee = Double.valueOf(-1);
        if (ownerResourceSummary.is_set_cpu_guarantee()) {
            cpuGuaranteee = ownerResourceSummary.get_cpu_guarantee();
        }

        int isolatedNodeGuarantee = -1;
        if (ownerResourceSummary.is_set_isolated_node_guarantee()) {
            isolatedNodeGuarantee = ownerResourceSummary.get_isolated_node_guarantee();
        }

        Double memoryGuaranteeRemaining = Double.valueOf(-1);
        if (ownerResourceSummary.is_set_memory_guarantee_remaining()) {
            memoryGuaranteeRemaining = ownerResourceSummary.get_memory_guarantee_remaining();
        }

        Double cpuGuaranteeRemaining = Double.valueOf(-1);
        if (ownerResourceSummary.is_set_cpu_guarantee_remaining()) {
            cpuGuaranteeRemaining = ownerResourceSummary.get_cpu_guarantee_remaining();
        }

        result.put("owner", ownerResourceSummary.get_owner());
        result.put("totalTopologies", ownerResourceSummary.get_total_topologies());
        result.put("totalExecutors", ownerResourceSummary.get_total_executors());
        result.put("totalWorkers", ownerResourceSummary.get_total_workers());
        result.put("totalTasks", ownerResourceSummary.get_total_tasks());
        result.put("totalMemoryUsage", ownerResourceSummary.get_memory_usage());
        result.put("totalCpuUsage", ownerResourceSummary.get_cpu_usage());

        result.put("memoryGuarantee", memoryGuarantee);
        result.put("cpuGuarantee", cpuGuaranteee);
        result.put("isolatedNodes", isolatedNodeGuarantee);

        result.put("memoryGuaranteeRemaining", memoryGuaranteeRemaining);
        result.put("cpuGuaranteeRemaining", cpuGuaranteeRemaining);
        result.put("totalReqOnHeapMem", ownerResourceSummary.get_requested_on_heap_memory());

        result.put("totalReqOffHeapMem", ownerResourceSummary.get_requested_off_heap_memory());

        result.put("totalReqMem", ownerResourceSummary.get_requested_total_memory());
        result.put("totalReqCpu", ownerResourceSummary.get_requested_cpu());
        result.put("totalAssignedOnHeapMem", ownerResourceSummary.get_assigned_on_heap_memory());
        result.put("totalAssignedOffHeapMem", ownerResourceSummary.get_assigned_off_heap_memory());

        return result;
    }

    /**
     * Get prettified ownerResourceSummaries.
     * @param ownerResourceSummaries ownerResourceSummaries from thrift call
     * @param conf Storm conf
     * @return map to be converted to json.
     */
    public static Map<String, Object> getOwnerResourceSummaries(
            List<OwnerResourceSummary> ownerResourceSummaries, Map<String, Object> conf) {
        Map<String, Object> result = new HashMap();

        result.put("schedulerDisplayResource", conf.get(DaemonConfig.SCHEDULER_DISPLAY_RESOURCE));

        List<Map<String, Object>> ownerSummaries = new ArrayList();

        for (OwnerResourceSummary ownerResourceSummary : ownerResourceSummaries) {
            ownerSummaries.add(unpackOwnerResourceSummary(ownerResourceSummary));
        }
        result.put("owners", ownerSummaries);
        return result;
    }

    public static Map<String, Object> getTopologyMap(TopologySummary topologySummary) {
        Map<String, Object> result = new HashMap();
        result.put("id", topologySummary.get_id());
        result.put("encodedId", URLEncoder.encode(topologySummary.get_id()));
        result.put("owner", topologySummary.get_owner());
        result.put("name", topologySummary.get_name());
        result.put("status", topologySummary.get_status());
        result.put("uptime", UiHelpers.prettyUptimeSec(topologySummary.get_uptime_secs()));
        result.put("uptimeSeconds", topologySummary.get_uptime_secs());
        result.put("tasksTotal", topologySummary.get_num_tasks());
        result.put("workersTotal", topologySummary.get_num_workers());
        result.put("executorsTotal", topologySummary.get_num_executors());
        result.put("replicationCount", topologySummary.get_replication_count());
        result.put("schedulerInfo", topologySummary.get_sched_status());
        result.put("requestedMemOnHeap", topologySummary.get_requested_memonheap());
        result.put("requestedMemOffHeap", topologySummary.get_requested_memoffheap());
        result.put("requestedTotalMem",
                topologySummary.get_requested_memoffheap() + topologySummary.get_assigned_memonheap());
        result.put("requestedCpu", topologySummary.get_requested_cpu());
        result.put("assignedMemOnHeap", topologySummary.get_assigned_memonheap());
        result.put("assignedMemOffHeap", topologySummary.get_assigned_memoffheap());
        result.put("assignedTotalMem",
                topologySummary.get_assigned_memoffheap() + topologySummary.get_assigned_memonheap());
        result.put("assignedCpu", topologySummary.get_assigned_cpu());
        result.put("topologyVersion", topologySummary.get_topology_version());
        result.put("stormVersion", topologySummary.get_storm_version());
        return result;
    }

    /**
     * Get a specific owner resource summary.
     * @param ownerResourceSummaries Result from thrift call.
     * @param client
     * @param id Owner id.
     * @param config Storm conf.
     * @return prettified owner resource summary.
     */
    public static Map<String, Object> getOwnerResourceSummary(
            List<OwnerResourceSummary> ownerResourceSummaries,
            Nimbus.Iface client, String id, Map<String, Object> config) {
        Map<String, Object> result = new HashMap();
        result.put("schedulerDisplayResource", config.get(DaemonConfig.SCHEDULER_DISPLAY_RESOURCE));

        if (ownerResourceSummaries.isEmpty()) {
            return unpackOwnerResourceSummary(new OwnerResourceSummary(id));
        }

        List<TopologySummary> topologies = null;
        try {
             topologies = client.getClusterInfo().get_topologies();
        } catch (TException e) {
            e.printStackTrace();
        }
        List<Map> topologySummaries = getTopologiesMap(id, topologies);

        result.putAll(unpackOwnerResourceSummary(ownerResourceSummaries.get(0)));
        result.put("topologies", topologySummaries);

        return result;
    }

    private static List<Map> getTopologiesMap(String id, List<TopologySummary> topologies) {
        List<Map> topologySummaries = new ArrayList();

        for(TopologySummary topologySummary : topologies) {
            if (id == null || topologySummary.get_owner() == id) {
                topologySummaries.add(getTopologyMap(topologySummary));
            }
        }
        return topologySummaries;
    }

    public static String getLogviewerLink(String host, String fname, Map<String, Object> config, int port) {
        if (isSecureLogviewer(config)) {
            return UiHelpers.urlFormat("https://%s:%s/api/v1/log?file=%s", host, config.get(DaemonConfig.LOGVIEWER_HTTPS_PORT), fname);
        } else {
            return UiHelpers.urlFormat("http://%s:%s/api/v1/log?file=%s", host, config.get(DaemonConfig.LOGVIEWER_PORT), fname);
        }
    }

    /**
     * Get log link to supervisor log.
     * @param host supervisor host name
     * @param config storm config
     * @return log link.
     */
    public static String getSupervisorLogLink(String host, Map<String, Object> config) {
        if (isSecureLogviewer(config)) {
            return UiHelpers.urlFormat("https://%s:%s/api/v1/daemonlog?file=supervisor.log", host, config.get(DaemonConfig.LOGVIEWER_HTTPS_PORT));
        }
        return UiHelpers.urlFormat("http://%s:%s/api/v1/daemonlog?file=supervisor.log", host, config.get(DaemonConfig.LOGVIEWER_PORT));
    }

    /**
     * Get log link to supervisor log.
     * @param host supervisor host name
     * @param config storm config
     * @return log link.
     */
    public static String getWorkerLogLink(String host, int port, Map<String, Object> config, String topologyId) {
        return getLogviewerLink(host, WebAppUtils.logsFilename(topologyId, String.valueOf(port)), config, port);
    }

    /**
     * Get supervisor info in a map.
     * @param supervisorSummary from nimbus call.
     * @param config Storm config.
     * @return prettified supervisor info map.
     */
    public static Map<String, Object> getPrettifiedSupervisorMap(SupervisorSummary supervisorSummary,
                                                                 Map<String, Object> config) {
        Map<String, Object> result = new HashMap();

        result.put("id", supervisorSummary.get_supervisor_id());
        result.put("host", supervisorSummary.get_host());
        result.put("uptime", UiHelpers.prettyUptimeSec(supervisorSummary.get_uptime_secs()));
        result.put("uptimeSeconds", supervisorSummary.get_uptime_secs());
        result.put("slotsTotal", supervisorSummary.get_num_workers());
        result.put("slotsUsed", supervisorSummary.get_num_used_workers());
        result.put("slotsFree",
                Integer.max(supervisorSummary.get_num_workers() - supervisorSummary.get_num_used_workers(), 0));
        Map<String, Double> totalResources = supervisorSummary.get_total_resources();
        Double totalMemory = totalResources.getOrDefault(Constants.COMMON_TOTAL_MEMORY_RESOURCE_NAME,
                totalResources.get(Config.SUPERVISOR_MEMORY_CAPACITY_MB));
        result.put("totalMem",
                totalMemory
        );
        Double totalCpu = totalResources.getOrDefault(Constants.COMMON_CPU_RESOURCE_NAME,
                totalResources.get(Config.SUPERVISOR_CPU_CAPACITY));
        result.put("totalCpu",
                totalCpu);
        result.put("usedMem", supervisorSummary.get_used_mem());
        result.put("usedCpu", supervisorSummary.get_used_cpu());
        result.put("logLink", getSupervisorLogLink(supervisorSummary.get_host(), config));
        result.put("availMem", totalMemory - supervisorSummary.get_used_mem());
        result.put("availCpu", totalCpu - supervisorSummary.get_used_cpu());
        result.put("version", supervisorSummary.get_version());
        return result;
    }

    /**
     * Get topology history.
     * @param topologyHistory from Nimbus call.
     * @return map ready to be returned.
     */
    public static Map<String, Object> getTopologyHistoryInfo(TopologyHistoryInfo topologyHistory) {
        Map<String, Object> result = new HashMap();
        result.put("topo-history", topologyHistory.get_topo_ids());
        return result;
    }

    /**
     * Check if logviewer is secure.
     * @param config Storm config.
     * @return true if logiviwer is secure.
     */
    public static boolean isSecureLogviewer(Map<String, Object> config) {
        if (config.containsKey(DaemonConfig.LOGVIEWER_HTTPS_PORT)) {
            int logviewerPort = (int) config.get(DaemonConfig.LOGVIEWER_HTTPS_PORT);
            if (logviewerPort >= 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get logviewer port depending on whether the logviewer is secure or not.
     * @param config Storm config.
     * @return appropriate port.
     */
    public static int getLogviewerPort(Map<String, Object> config) {
        if (isSecureLogviewer(config)) {
            return (int) config.get(DaemonConfig.LOGVIEWER_HTTPS_PORT);
        }
        return (int) config.get(DaemonConfig.LOGVIEWER_PORT);
    }

    public static List<Map> getWorkerSummaries(SupervisorPageInfo supervisorPageInfo,
                                               Map<String, Object> config) {
        List<Map> workerSummaries = new ArrayList();
        for (WorkerSummary workerSummary : supervisorPageInfo.get_worker_summaries()) {
            workerSummaries.add(getWorkerSummaryMap(workerSummary, config));
        }
        return workerSummaries;
    }

    private static Map getWorkerSummaryMap(WorkerSummary workerSummary, Map<String, Object> config) {
        Map<String, Object> result = new HashMap();
        result.put("supervisorId", workerSummary.get_supervisor_id());
        result.put("host", workerSummary.get_host());
        result.put("port", workerSummary.get_port());
        result.put("topologyId", workerSummary.get_topology_id());
        result.put("topologyName", workerSummary.get_topology_name());
        result.put("executorsTotal", workerSummary.get_num_executors());
        result.put("assignedMemOnHeap", workerSummary.get_assigned_memonheap());
        result.put("assignedMemOffHeap", workerSummary.get_assigned_memoffheap());
        result.put("assignedCpu", workerSummary.get_assigned_cpu());
        result.put("componentNumTasks", workerSummary.get_component_to_num_tasks());
        result.put("uptime", UiHelpers.prettyUptimeSec(workerSummary.get_uptime_secs()));
        result.put("uptimeSeconds", workerSummary.get_uptime_secs());
        result.put("workerLogLink", getWorkerLogLink(workerSummary.get_host(),
                workerSummary.get_port(), config, workerSummary.get_topology_id()));
        return result;
    }

    /**
     * getSupervisorSummary.
     * @param supervisors supervisor summary list.
     * @param securityContext security context injected.
     * @param config Storm config.
     * @return Prettified JSON.
     */
    public static Map<String, Object> getSupervisorSummary(
            List<SupervisorSummary> supervisors, SecurityContext securityContext, Map<String, Object> config) {
        Map<String, Object> result = new HashMap();
        addLogviewerInfo(config, result);
        List<Map> supervisorMaps = getSupervisorsMap(supervisors, config);
        result.put("supervisors", supervisorMaps);
        result.put("schedulerDisplayResource", config.get(DaemonConfig.SCHEDULER_DISPLAY_RESOURCE));

        return result;
    }

    private static List<Map> getSupervisorsMap(List<SupervisorSummary> supervisors, Map<String, Object> config) {
        List<Map> supervisorMaps = new ArrayList();
        for (SupervisorSummary supervisorSummary : supervisors) {
            supervisorMaps.add(getPrettifiedSupervisorMap(supervisorSummary, config));
        }
        return supervisorMaps;
    }

    private static void addLogviewerInfo(Map<String, Object> config, Map<String, Object> result) {
        result.put("logviewerPort", getLogviewerPort(config));
        String logviewerScheme = "http";
        if (isSecureLogviewer(config)) {
            logviewerScheme = "https";
        }
        result.put("logviewerScheme", logviewerScheme);
    }

    public static Map<String, Object> getSupervisorPageInfo(
            SupervisorPageInfo supervisorPageInfo, Map<String,Object> config) {
        Map<String, Object> result = new HashMap();
        result.put("workers", getWorkerSummaries(supervisorPageInfo, config));
        result.put("schedulerDisplayResource", config.get(DaemonConfig.SCHEDULER_DISPLAY_RESOURCE));
        List<Map> supervisorMaps = getSupervisorsMap(supervisorPageInfo.get_supervisor_summaries(), config);
        result.put("supervisors", supervisorMaps);

        return result;
    }

    public static Map<String, Object> getAllTopologiesSummary(
            List<TopologySummary> topologies, Map<String,Object> config) {
        Map<String, Object> result = new HashMap();
        result.put("topologies", getTopologiesMap(null, topologies));
        result.put("schedulerDisplayResource", config.get(DaemonConfig.SCHEDULER_DISPLAY_RESOURCE));
        return result;
    }

    public static String getWindowHint(String window) {
        if (window == ":all-time") {
            return "All time";
        }
        return UiHelpers.prettyUptimeSec(window);
    }

    public static Map<String, Double> getStatDisplayMap(Map<String, Double> rawDisplayMap) {
        Map<String, Double> result = new HashMap();
        for (Map.Entry<String, Double> entry : rawDisplayMap.entrySet()) {
            result.put(getWindowHint(entry.getKey()), entry.getValue());
        }

        return result;
    }

    public static Map<String, Object> getTopologySummary(TopologyPageInfo topologyPageInfo, String window, Map<String, Object> config, String remoteUser) {
        Map<String, Object> result = new HashMap();
        Map<String, Object> topologyConf = (Map<String, Object>) JSONValue.parse(topologyPageInfo.get_topology_conf());
        double messageTimeout = (double) topologyConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS);
        Map<String, Object> unpackedTopologyPageInfo =
                unpackTopologyInfo(topologyPageInfo, window, config);
        result.putAll(unpackedTopologyPageInfo);
        result.put("user", remoteUser);
        result.put("window", window);
        result.put("windowHint", getWindowHint(window));
        result.put("msgTimeout", messageTimeout);
        result.put("configuration", topologyConf);
        result.put("visualizationTable", new ArrayList());
        result.put("schedulerDisplayResource", config.get(DaemonConfig.SCHEDULER_DISPLAY_RESOURCE));
        return result;
    }

    private static Map<String, Long> getStatDisplayMapLong(Map<String,Long> window_to_transferred) {
        Map<String, Long> result = new HashMap();
        for (Map.Entry<String, Long> entry : window_to_transferred.entrySet()) {
            result.put(getWindowHint(entry.getKey()), entry.getValue());
        }

        return result;
    }

    private static Map<String, Object> getCommonAggStatsMap(CommonAggregateStats commonAggregateStats) {
        Map<String, Object> result = new HashMap();
        result.put("executors", commonAggregateStats.get_num_executors());
        result.put("tasks", commonAggregateStats.get_num_tasks());
        result.put("emitted", commonAggregateStats.get_emitted());
        result.put("transferred", commonAggregateStats.get_transferred());
        result.put("acked", commonAggregateStats.get_acked());
        result.put("failed", commonAggregateStats.get_failed());
        result.put(
                "requestedMemOnHeap",
                commonAggregateStats.get_resources_map().get(Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME)
        );
        result.put(
                "requestedMemOffHeap",
                commonAggregateStats.get_resources_map().get(Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME));
        result.put(
                "requestedCpu" ,
                commonAggregateStats.get_resources_map().get(Constants.COMMON_CPU_RESOURCE_NAME));
        return result;
    }

    private static Map<String, Object> getSpoutAggStatsMap(
            ComponentAggregateStats componentAggregateStats, String spoutId) {
        Map<String, Object> result = new HashMap();
        result.putAll(getCommonAggStatsMap(componentAggregateStats.get_common_stats()));
        result.put("spoutId", spoutId);
        result.put("encodedSpoutId", URLEncoder.encode(spoutId));
        result.put("completeLatency",
                componentAggregateStats.get_specific_stats().get_spout().get_complete_latency_ms());
        return result;
    }

    private static Map<String, Object> getBoltAggStatsMap(
            ComponentAggregateStats componentAggregateStats, String boltId) {
        Map<String, Object> result = new HashMap();
        result.putAll(getCommonAggStatsMap(componentAggregateStats.get_common_stats()));
        result.put("boltId", boltId);
        result.put("encodedBoltId", URLEncoder.encode(boltId));
        BoltAggregateStats boltAggregateStats = componentAggregateStats.get_specific_stats().get_bolt();
        result.put("capacity", StatsUtil.floatStr(boltAggregateStats.get_capacity()));
        result.put("executeLatency", StatsUtil.floatStr(boltAggregateStats.get_execute_latency_ms()));
        result.put("executed", boltAggregateStats.get_executed());
        result.put("processLatency", StatsUtil.floatStr(boltAggregateStats.get_process_latency_ms()));
        return result;
    }

    private static Map<String, Object> getTopologyStatsMap(TopologyStats topologyStats) {
        Map<String, Object> result = new HashMap();

        Map<String, Double> statDisplayMap = getStatDisplayMap(topologyStats.get_window_to_emitted());
        result.put("windowPretty", statDisplayMap.keySet().toArray());
        result.put("emitted", statDisplayMap);
        result.put("transferred", getStatDisplayMapLong(topologyStats.get_window_to_transferred()));
        result.put("completeLatency", getStatDisplayMap(topologyStats.get_window_to_complete_latencies_ms()));
        result.put("acked", getStatDisplayMapLong(topologyStats.get_window_to_acked()));
        result.put("failed", getStatDisplayMapLong(topologyStats.get_window_to_failed()));
        result.put("window", topologyStats.get_window_to_emitted().keySet().toArray());
        return result;
    }

    private static Map<String,Object> unpackTopologyInfo(TopologyPageInfo topologyPageInfo, String window, Map<String,Object> config) {
        Map<String, Object> result = new HashMap();
        result.put("id", topologyPageInfo.get_id());
        result.put("encodedId", URLEncoder.encode(topologyPageInfo.get_id()));
        result.put("owner", topologyPageInfo.get_owner());
        result.put("name", topologyPageInfo.get_name());
        result.put("status", topologyPageInfo.get_status());
        result.put("uptime", UiHelpers.prettyUptimeSec(topologyPageInfo.get_uptime_secs()));
        result.put("uptimeSeconds", topologyPageInfo.get_uptime_secs());
        result.put("tasksTotal", topologyPageInfo.get_num_tasks());
        result.put("workersTotal", topologyPageInfo.get_num_workers());
        result.put("executorsTotal", topologyPageInfo.get_num_executors());
        result.put("schedulerInfo", topologyPageInfo.get_sched_status());
        result.put("requestedMemOnHeap", topologyPageInfo.get_requested_memonheap());
        result.put("requestedMemOffHeap", topologyPageInfo.get_requested_memoffheap());
        result.put("requestedCpu", topologyPageInfo.get_requested_cpu());
        result.put("requestedTotalMem",
                topologyPageInfo.get_requested_memonheap() + topologyPageInfo.get_requested_memoffheap()
        );
        result.put("assignedMemOnHeap", topologyPageInfo.get_assigned_memonheap());
        result.put("assignedMemOffHeap", topologyPageInfo.get_assigned_memoffheap());
        result.put("assignedTotalMem", topologyPageInfo.get_assigned_memonheap() +
                topologyPageInfo.get_assigned_memoffheap());
        result.put("assignedCpu", topologyPageInfo.get_assigned_cpu());
        result.put("requestedRegularOnHeapMem", topologyPageInfo.get_requested_regular_on_heap_memory());
        result.put("requestedSharedOnHeapMem", topologyPageInfo.get_requested_shared_on_heap_memory());
        result.put("requestedRegularOffHeapMem", topologyPageInfo.get_requested_regular_off_heap_memory());
        result.put("requestedSharedOffHeapMem", topologyPageInfo.get_requested_shared_off_heap_memory());
        result.put("assignedRegularOnHeapMem", topologyPageInfo.get_assigned_regular_on_heap_memory());
        result.put("assignedSharedOnHeapMem", topologyPageInfo.get_assigned_shared_on_heap_memory());
        result.put("assignedRegularOffHeapMem", topologyPageInfo.get_assigned_regular_off_heap_memory());
        result.put("assignedSharedOffHeapMem", topologyPageInfo.get_assigned_shared_off_heap_memory());
        result.put("topologyStats", getTopologyStatsMap(topologyPageInfo.get_topology_stats()));
        List<Map> workerSummaries = new ArrayList();
        for(WorkerSummary workerSummary : topologyPageInfo.get_workers()) {
            workerSummaries.add(getWorkerSummaryMap(workerSummary, config));
        }
        result.put("workers", workerSummaries);

        Map<String, ComponentAggregateStats> spouts = topologyPageInfo.get_id_to_spout_agg_stats();
        List<Map> spoutStats = new ArrayList();

        for (Map.Entry<String, ComponentAggregateStats> spoutEntry : spouts.entrySet()) {
            spoutStats.add(getSpoutAggStatsMap(spoutEntry.getValue(), spoutEntry.getKey()));
        }
        result.put("spouts", spoutStats);

        Map<String, ComponentAggregateStats> bolts = topologyPageInfo.get_id_to_bolt_agg_stats();
        List<Map> boltStats = new ArrayList();

        for (Map.Entry<String, ComponentAggregateStats> boltEntry : bolts.entrySet()) {
            boltStats.add(getBoltAggStatsMap(boltEntry.getValue(), boltEntry.getKey()));
        }
        result.put("bolts", boltStats);


        result.put("configuration", topologyPageInfo.get_topology_conf());
        boolean debuggingEnabled = topologyPageInfo.get_debug_options().is_enable();
        result.put("debug", debuggingEnabled);
        double samplingPct = 10;
        if (debuggingEnabled) {
            samplingPct = topologyPageInfo.get_debug_options().get_samplingpct();
        }
        result.put("samplingPct", samplingPct);
        result.put("replicationCount", topologyPageInfo.get_replication_count());
        result.put("topologyVersion", topologyPageInfo.get_topology_version());
        result.put("stormVersion", topologyPageInfo.get_storm_version());
        return result;
    }

    public static Map<String, Object> getTopologyWorkers(TopologyInfo topologyInfo, String id, Map config) {
        Map<String, Object> result = new HashMap();
        List<Map> executorSummaries = new ArrayList();
        for (ExecutorSummary executorSummary : topologyInfo.get_executors()) {
            Map<String, Object> executorSummaryMap = new HashMap();
            executorSummaryMap.put("host", executorSummary.get_host());
            executorSummaryMap.put("port", executorSummary.get_port());
            executorSummaries.add(executorSummaryMap);
        }
        HashSet hashSet = new HashSet();
        hashSet.addAll(executorSummaries);
        executorSummaries.clear();
        executorSummaries.addAll(hashSet);
        result.put("hostPortList", executorSummaries);
        addLogviewerInfo(result, config);
        return result;
    }


    public static Map<String, Map<String, Object>> getTopologyLag(StormTopology userTopology, Map<String,Object> config) {
        return TopologySpoutLag.lag(userTopology, config);
    }

    public static Map<String, List<ExecutorSummary>> getBoltExecutors(List<ExecutorSummary> executorSummaries,
                                                                StormTopology stormTopology, boolean sys) {
        Map<String, List<ExecutorSummary>> result = new HashMap();
        for (ExecutorSummary executorSummary : executorSummaries) {
            if (StatsUtil.componentType(stormTopology, executorSummary.get_component_id()) == "bolt" &&
                    (sys || !Utils.isSystemId(executorSummary.get_component_id()))) {
                List<ExecutorSummary> executorSummaryList = result.getOrDefault(executorSummary.get_component_id(), new ArrayList());
                executorSummaryList.add(executorSummary);
                result.put(executorSummary.get_component_id(), executorSummaryList);
            }
        }
        return result;
    }

    public static Map<String, List<ExecutorSummary>> getSpoutExecutors(List<ExecutorSummary> executorSummaries, StormTopology stormTopology) {
        Map<String, List<ExecutorSummary>> result = new HashMap();
        for (ExecutorSummary executorSummary : executorSummaries) {
            if (StatsUtil.componentType(stormTopology, executorSummary.get_component_id()) == "spout") {
                List<ExecutorSummary> executorSummaryList = result.getOrDefault(executorSummary.get_component_id(), new ArrayList());
                executorSummaryList.add(executorSummary);
                result.put(executorSummary.get_component_id(), executorSummaryList);
            }
        }
        return result;
    }

    public static String sanitizeStreamName(String streamName) {
        Pattern pattern = Pattern.compile("(?![A-Za-z_\\-:\\.]).");
        Pattern pattern2 = Pattern.compile("^[A-Za-z]");
        Matcher matcher = pattern2.matcher(streamName);
        Matcher matcher2 = pattern.matcher("\\s" + streamName);
        if (matcher.find()) {
            matcher2 = pattern.matcher(streamName);
        }
        return matcher2.replaceAll("_");
    }

    public static  Map<String, Map<String,Long>> sanitizeTransferredStats( Map<String, Map<String,Long>> stats) {
        Map<String, Map<String,Long>> result = new HashMap();
        for (Map.Entry<String, Map<String,Long>> entry : stats.entrySet()) {
            Map<String,Long> temp = new HashMap();
            for (Map.Entry<String,Long> innerEntry : entry.getValue().entrySet()) {
                temp.put(sanitizeStreamName(innerEntry.getKey()), innerEntry.getValue());
            }
            result.put(entry.getKey(), temp);
        }
        return result;
    }

    public static Map<String, Object> getStatMapFromExecutorSummary(ExecutorSummary executorSummary) {
        Map<String, Object> result = new HashMap();
        result.put("host", executorSummary.get_host());
        result.put("port", executorSummary.get_port());
        result.put("uptime_secs", executorSummary.get_uptime_secs());
        result.put("transferred", null);
        if (executorSummary.is_set_stats()) {
            result.put("transferred", sanitizeTransferredStats(executorSummary.get_stats().get_transferred()));
        }
        return result;
    }

    public static Map<String, Object> getInputMap(Map.Entry<GlobalStreamId,Grouping> entryInput) {
        Map<String, Object> result = new HashMap();
        result.put("component", entryInput.getKey().get_componentId());
        result.put("stream", entryInput.getKey().get_streamId());
        result.put("sani-stream", sanitizeStreamName(entryInput.getKey().get_streamId()));
        result.put("grouping", entryInput.getValue());
        return result;
    }

    public static Map<String, Object> getVisualizationData(
            Nimbus.Iface client, String window, String topoId, boolean sys) throws TException {
        GetInfoOptions getInfoOptions = new GetInfoOptions();
        getInfoOptions.set_num_err_choice(NumErrorsChoice.ONE);
        TopologyInfo topologyInfo = client.getTopologyInfoWithOpts(topoId, getInfoOptions);
        StormTopology stormTopology = client.getTopology(topoId);
        Map<String, List<ExecutorSummary>> boltSummaries = getBoltExecutors(topologyInfo.get_executors(), stormTopology, sys);
        Map<String, List<ExecutorSummary>> spoutSummaries = getSpoutExecutors(topologyInfo.get_executors(), stormTopology);

        Map<String, SpoutSpec> spoutSpecs = stormTopology.get_spouts();
        Map<String, Bolt> boltSpecs = stormTopology.get_bolts();

        Map<String, Object> result = new HashMap();

        for (Map.Entry<String, SpoutSpec> spoutSpecMapEntry : spoutSpecs.entrySet()) {
            String spoutComponentId = spoutSpecMapEntry.getKey();
            if (spoutSummaries.containsKey(spoutComponentId)) {
                Map<String, Object> spoutData = new HashMap();
                spoutData.put("type", "spout");
                spoutData.put("capacity", 0);
                Map<String, Map> spoutStreamsStats = StatsUtil.spoutStreamsStats(spoutSummaries.get(spoutComponentId), true);
                spoutData.put("latency", spoutStreamsStats.get("complete-latencies").get(window));
                spoutData.put("transferred", spoutStreamsStats.get("transferred").get(window));
                spoutData.put("stats", spoutSummaries.get(
                        spoutComponentId).stream().map(
                                UiHelpers::getStatMapFromExecutorSummary).collect(Collectors.toList()));
                spoutData.put("link", UiHelpers.urlFormat("/component.html?id=%s&topology_id=%s", spoutComponentId, topoId));

                result.put("inputs",
                    spoutSpecMapEntry.getValue().get_common().get_inputs().entrySet().stream().map(
                            UiHelpers::getInputMap).collect(Collectors.toList())
                );
                result.put(spoutComponentId, spoutData);
            }
        }

        for (Map.Entry<String, Bolt> boltEntry : boltSpecs.entrySet()) {
            String boltComponentId = boltEntry.getKey();
            if (boltSummaries.containsKey(boltComponentId) && (sys || Utils.isSystemId(boltComponentId))) {
                Map<String, Object> boltMap = new HashMap();
                boltMap.put("type", "bolt");
                boltMap.put("capacity", StatsUtil.computeBoltCapacity(boltSummaries.get(boltSummaries)));
                Map<String, Map> boltStreamsStats = StatsUtil.boltStreamsStats(boltSummaries.get(boltSummaries), true);
                boltMap.put("latency", boltStreamsStats.get("process-latencies").get(window));
                boltMap.put("transferred", boltStreamsStats.get("transferred").get(window));
                boltMap.put("stats", boltSummaries.get(
                        boltComponentId).stream().map(
                        UiHelpers::getStatMapFromExecutorSummary).collect(Collectors.toList()));
                boltMap.put("link", UiHelpers.urlFormat("/component.html?id=%s&topology_id=%s", boltComponentId, topoId));

                result.put("inputs",
                        boltEntry.getValue().get_common().get_inputs().entrySet().stream().map(
                                UiHelpers::getInputMap).collect(Collectors.toList())
                );
                result.put(boltComponentId, boltMap);
            }
        }
        return result;
    }

    public static Map<String, Object> getStreamBox(Object visualization) {
        Map<String, Object> visualizationData = (Map<String, Object>) visualization;
        Map<String, Object> result = new HashMap();
        Map<String, Object> temp = (Map<String, Object>) visualizationData.get("inputs");
        result.put("stream", temp.get("stream"));
        result.put("sani-stream", temp.get("sani-stream"));
        result.put("checked", !Utils.isSystemId((String) temp.get("stream")));
        return result;
    }

    public static Map<String, Object> getBuildVisualization(
            Nimbus.Iface client, Map<String, Object> config, String window, String id, boolean sys) throws TException {
        Map<String, Object> result = new HashMap();
        Map<String, Object> visualizationData = getVisualizationData(client, window, id, sys);
        List<Map> streamBoxes = visualizationData.entrySet().stream().map(UiHelpers::getStreamBox).collect(Collectors.toList());
        result.put("visualizationTable", Lists.partition(streamBoxes, 4));
        return result;
    }

    public static Map<String, Object> getActiveAction(ProfileRequest profileRequest, Map config, String topologyId) {
        Map<String, Object> result = new HashMap();
        result.put("host", profileRequest.get_nodeInfo().get_node());
        result.put("port", String.valueOf(profileRequest.get_nodeInfo().get_port().iterator().next()));
        result.put("dumplink",
                getWorkerDumpLink(
                        profileRequest.get_nodeInfo().get_node(),
                        profileRequest.get_nodeInfo().get_port().iterator().next(), topologyId, config
                        ));
        result.put("timestamp", System.currentTimeMillis() - profileRequest.get_time_stamp());
        return result;
    }

    public static List getActiveProfileActions(Nimbus.Iface client, String id, String component, Map config) throws TException {
        List<ProfileRequest> profileRequests =
                client.getComponentPendingProfileActions(id, component, ProfileAction.JPROFILE_STOP);
        return profileRequests.stream().map( x -> UiHelpers.getActiveAction(x, config, id)).collect(Collectors.toList());
    }

    public static String getWorkerDumpLink(String host, long port, String topologyId, Map<String, Object> config) {
        if(isSecureLogviewer(config)) {
            return UiHelpers.urlFormat(
                    "https://%s:%s/api/v1/dumps/%s/%s",
                    URLEncoder.encode(host), config.get(DaemonConfig.LOGVIEWER_HTTPS_PORT),
                    URLEncoder.encode(topologyId),
                    URLEncoder.encode(host) + ":" + URLEncoder.encode(String.valueOf(port))
            );
        } else {
            return UiHelpers.urlFormat(
                    "http://%s:%s/api/v1/dumps/%s/%s",
                    URLEncoder.encode(host), config.get(DaemonConfig.LOGVIEWER_PORT),
                    URLEncoder.encode(topologyId),
                    URLEncoder.encode(host) + ":" + URLEncoder.encode(String.valueOf(port))
            );
        }

    }

    public static Map<String, Object> getComponentPage(
            Nimbus.Iface client, String id, String component,
            String window, boolean sys, String user, Map config) throws TException {
        Map<String, Object> result = new HashMap();
        ComponentPageInfo componentPageInfo = client.getComponentPageInfo(
                id, component, window, sys
        );
        result.put("user", user);
        result.put("id" , component);
        result.put("encodedId", URLEncoder.encode(component));
        result.put("name", componentPageInfo.get_topology_name());
        result.put("executors", componentPageInfo.get_num_executors());
        result.put("tasks", componentPageInfo.get_num_tasks());
        result.put("requestedMemOnHeap",
                componentPageInfo.get_resources_map().get(Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME));
        result.put("requestedMemOffHeap",
                componentPageInfo.get_resources_map().get(Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME));
        result.put("requestedCpu",
                componentPageInfo.get_resources_map().get(Constants.COMMON_CPU_RESOURCE_NAME));

        result.put("schedulerDisplayResource", config.get(DaemonConfig.SCHEDULER_DISPLAY_RESOURCE));
        result.put("topologyId", id);
        result.put("topologyStatus", componentPageInfo.get_topology_status());
        result.put("encodedTopologyId", URLEncoder.encode(id));
        result.put("window", window);
        result.put("componentType", componentPageInfo.get_component_type().toString().toLowerCase());
        result.put("windowHint", getWindowHint(window));
        result.put("debug", componentPageInfo.is_set_debug_options() && componentPageInfo.get_debug_options().is_enable());
        double samplingPct = 10;
        if (componentPageInfo.is_set_debug_options()) {
            samplingPct = componentPageInfo.get_debug_options().get_samplingpct();
        }
        result.put("samplingPct", samplingPct);
        result.put("eventLogLink", getLogviewerLink(componentPageInfo.get_eventlog_host(),
                WebAppUtils.eventLogsFilename(id, String.valueOf(componentPageInfo.get_eventlog_port())),
                config, componentPageInfo.get_eventlog_port()));
        result.put("profilingAndDebuggingCapable", !Utils.isOnWindows());
        result.put("profileActionEnabled", config.get(DaemonConfig.WORKER_PROFILER_ENABLED));


        result.put("profilerActive", getActiveProfileActions(client, id, component, config));
        return result;
    }

    public static Map<String, Object> getTopolgoyLogConfig(LogConfig logConfig) {
        Map<String, Object> result = new HashMap();
        for(Map.Entry<String, LogLevel> entry : logConfig.get_named_logger_level().entrySet()) {
            Map temp = new HashMap();
            temp.put("target_level", entry.getValue().get_target_log_level());
            temp.put("reset_level", entry.getValue().get_reset_log_level());
            temp.put("timeout", entry.getValue().get_reset_log_level_timeout_secs());
            temp.put("timeout_epoch", entry.getValue().get_reset_log_level_timeout_epoch());
            result.put(entry.getKey(), temp);
        }
        Map temp = new HashMap();
        temp.put("namedLoggerLevels", temp);
        return result;
    }
}
