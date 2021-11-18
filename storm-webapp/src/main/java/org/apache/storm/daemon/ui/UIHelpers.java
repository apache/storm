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

package org.apache.storm.daemon.ui;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.DispatcherType;
import javax.servlet.Servlet;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.common.ReloadableSslContextFactory;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.BoltAggregateStats;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.CommonAggregateStats;
import org.apache.storm.generated.ComponentAggregateStats;
import org.apache.storm.generated.ComponentPageInfo;
import org.apache.storm.generated.ComponentType;
import org.apache.storm.generated.ErrorInfo;
import org.apache.storm.generated.ExecutorAggregateStats;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GetInfoOptions;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.LogConfig;
import org.apache.storm.generated.LogLevel;
import org.apache.storm.generated.LogLevelAction;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.NumErrorsChoice;
import org.apache.storm.generated.OwnerResourceSummary;
import org.apache.storm.generated.ProfileAction;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.SpecificAggregateStats;
import org.apache.storm.generated.SpoutAggregateStats;
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
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceRequest;
import org.apache.storm.stats.StatsUtil;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.IVersionInfo;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.TopologySpoutLag;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.VersionInfo;
import org.apache.storm.utils.WebAppUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class UIHelpers {
    private static final Logger LOG = LoggerFactory.getLogger(UIHelpers.class);
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
     * Prettify uptime string.
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
     * Prettify uptime string.
     * @param sec uptime in seconds.
     * @return prettified uptime string.
     */
    public static String prettyUptimeSec(String sec) {
        return prettyUptimeStr(sec, PRETTY_SEC_DIVIDERS);
    }

    /**
     * prettyUptimeSec.
     * @param secs secs
     * @return prettyUptimeSec
     */
    public static String prettyUptimeSec(int secs) {
        return prettyUptimeStr(String.valueOf(secs), PRETTY_SEC_DIVIDERS);
    }

    /**
     * prettyUptimeMs.
     * @param ms ms
     * @return prettyUptimeMs
     */
    public static String prettyUptimeMs(String ms) {
        return prettyUptimeStr(ms, PRETTY_MS_DIVIDERS);
    }

    /**
     * prettyUptimeMs.
     * @param ms ms
     * @return prettyUptimeMs
     */
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
            argsEncoded[i] = Utils.urlEncodeUtf8(String.valueOf(args[i]));
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

    private static ServerConnector mkSslConnector(Server server, Integer port, String ksPath,
                                                  String ksPassword, String ksType,
                                                  String keyPassword, String tsPath,
                                                  String tsPassword, String tsType,
                                                  Boolean needClientAuth, Boolean wantClientAuth,
                                                  Integer headerBufferSize, boolean enableSslReload) {
        SslContextFactory factory = new ReloadableSslContextFactory(enableSslReload);
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
        ServerConnector sslConnector = new ServerConnector(
                server,
                new SslConnectionFactory(factory, HttpVersion.HTTP_1_1.asString()),
                new HttpConnectionFactory(httpsConfig)
        );
        sslConnector.setPort(port);
        return sslConnector;
    }

    public static void configSsl(Server server, Integer port, String ksPath,
                                 String ksPassword, String ksType,
                                 String keyPassword, String tsPath,
                                 String tsPassword, String tsType,
                                 Boolean needClientAuth, Boolean wantClientAuth, boolean enableSslReload) {
        configSsl(server, port, ksPath, ksPassword, ksType, keyPassword,
                  tsPath, tsPassword, tsType, needClientAuth, wantClientAuth, null, enableSslReload);
    }

    /**
     * configSsl.
     * @param server server
     * @param port port
     * @param ksPath ksPath
     * @param ksPassword ksPassword
     * @param ksType ksType
     * @param keyPassword keyPassword
     * @param tsPath tsPath
     * @param tsPassword tsPassword
     * @param tsType tsType
     * @param needClientAuth needClientAuth
     * @param wantClientAuth wantClientAuth
     * @param headerBufferSize headerBufferSize
     * @param enableSslReload enable ssl reload
     */
    public static void configSsl(Server server, Integer port, String ksPath,
                                 String ksPassword, String ksType,
                                 String keyPassword, String tsPath,
                                 String tsPassword, String tsType,
                                 Boolean needClientAuth,
                                 Boolean wantClientAuth, Integer headerBufferSize,
                                 boolean enableSslReload) {
        if (port > 0) {
            server.addConnector(
                    mkSslConnector(
                            server, port, ksPath, ksPassword, ksType, keyPassword,
                            tsPath, tsPassword, tsType,
                            needClientAuth, wantClientAuth, headerBufferSize, enableSslReload
                    )
            );
        }
    }

    /**
     * corsFilterHandle.
     * @return corsFilterHandle
     */
    public static FilterHolder corsFilterHandle() {
        FilterHolder filterHolder = new FilterHolder(new CrossOriginFilter());
        filterHolder.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "*");
        filterHolder.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "GET, POST, PUT");
        filterHolder.setInitParameter(
                CrossOriginFilter.ALLOWED_ORIGINS_PARAM,
                "X-Requested-With, X-Requested-By, Access-Control-Allow-Origin,"
                        + " Content-Type, Content-Length, Accept, Origin");
        filterHolder.setInitParameter(CrossOriginFilter.ACCESS_CONTROL_ALLOW_ORIGIN_HEADER, "*");
        return filterHolder;
    }

    /**
     * mkAccessLoggingFilterHandle.
     * @return mkAccessLoggingFilterHandle
     */
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
    public static void configFilter(Server server, Servlet servlet,
                                    List<FilterConfiguration> filtersConfs,
                                    Map<String, String> params) {
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
    public static void configFilters(ServletContextHandler context,
                                     List<FilterConfiguration> filtersConfs) {
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
    public static Server jettyCreateServer(Integer port, String host, Integer httpsPort, Boolean disableHttpBinding) {
        return jettyCreateServer(port, host, httpsPort, null, disableHttpBinding);
    }

    /**
     * Construct a Jetty Server instance.
     */
    public static Server jettyCreateServer(Integer port, String host,
                                           Integer httpsPort, Integer headerBufferSize, Boolean disableHttpBinding) {
        Server server = new Server();

        if (httpsPort == null || httpsPort <= 0 || disableHttpBinding == null || disableHttpBinding == false) {
            HttpConfiguration httpConfig = new HttpConfiguration();
            httpConfig.setSendDateHeader(true);
            if (null != headerBufferSize) {
                httpConfig.setRequestHeaderSize(headerBufferSize);
            }
            ServerConnector httpConnector = new ServerConnector(
                    server, new HttpConnectionFactory(httpConfig)
            );
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
    public static void stormRunJetty(Integer port, String host,
                                     Integer httpsPort, Integer headerBufferSize,
                                     IConfigurator configurator) throws Exception {
        Server s = jettyCreateServer(port, host, httpsPort, headerBufferSize, false);
        if (configurator != null) {
            configurator.execute(s);
        }
        s.start();
    }

    public static void stormRunJetty(Integer port, Integer headerBufferSize,
                                     IConfigurator configurator) throws Exception {
        stormRunJetty(port, null, null, headerBufferSize, configurator);
    }

    /**
     * wrapJsonInCallback.
     * @param callback callbackParameterName
     * @param response response
     * @return wrapJsonInCallback
     */
    public static String wrapJsonInCallback(String callback, String response) {
        return callback + "(" + response + ");";
    }

    /**
     * getJsonResponseHeaders.
     * @param callback callbackParameterName
     * @param headers headers
     * @return getJsonResponseHeaders
     */
    public static Map getJsonResponseHeaders(String callback, Map headers) {
        Map<String, String> headersResult = new HashMap<>();
        headersResult.put("Cache-Control", "no-cache, no-store");
        headersResult.put("Access-Control-Allow-Origin", "*");
        headersResult.put("Access-Control-Allow-Headers",
                          "Content-Type, Access-Control-Allow-Headers, "
                                  + "Access-Controler-Allow-Origin, "
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

    public static Response makeStandardResponse(Object data, String callback) {
        return makeStandardResponse(data, callback, true, Response.Status.OK);
    }

    public static Response makeStandardResponse(Object data, String callback, Response.Status status) {
        return makeStandardResponse(data, callback, true, status);
    }

    /**
     * makeStandardResponse.
     * @param data data
     * @param callback callbackParameterName
     * @param needsSerialization needsSerialization
     * @return makeStandardResponse
     */
    public static Response makeStandardResponse(
            Object data, String callback, boolean needsSerialization, Response.Status status) {
        String body = getJsonResponseBody(data, callback, needsSerialization);
        Response.ResponseBuilder responseBuilder = Response.status(status).entity(body);
        Map<String, String> headers = getJsonResponseHeaders(callback, null);
        for (Map.Entry<String, String> headerEntry: headers.entrySet()) {
            responseBuilder.header(headerEntry.getKey(), headerEntry.getValue());
        }
        return responseBuilder.build();
    }

    private static final AtomicReference<List<Map<String, String>>> MEMORIZED_VERSIONS = new AtomicReference<>();
    private static final AtomicReference<Map<String, String>> MEMORIZED_FULL_VERSION = new AtomicReference<>();

    private static Map<String, String> toJsonStruct(IVersionInfo info) {
        Map<String, String> ret = new HashMap<>();
        ret.put("version", info.getVersion());
        ret.put("revision", info.getRevision());
        ret.put("branch", info.getBranch());
        ret.put("date", info.getDate());
        ret.put("user", info.getUser());
        ret.put("url", info.getUrl());
        ret.put("srcChecksum", info.getSrcChecksum());
        return ret;
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

        if (MEMORIZED_VERSIONS.get() == null) {
            //Races are okay this is just to avoid extra work for each page load.
            NavigableMap<String, IVersionInfo> versionsMap = Utils.getAlternativeVersionsMap(conf);
            List<Map<String, String>> versionList = new ArrayList<>();
            for (Map.Entry<String, IVersionInfo> entry : versionsMap.entrySet()) {
                Map<String, String> single = new HashMap<>(toJsonStruct(entry.getValue()));
                single.put("versionMatch", entry.getKey());
                versionList.add(single);
            }
            MEMORIZED_VERSIONS.set(versionList);
        }
        List<Map<String, String>> versions = MEMORIZED_VERSIONS.get();
        if (!versions.isEmpty()) {
            result.put("alternativeWorkerVersions", versions);
        }

        if (MEMORIZED_FULL_VERSION.get() == null) {
            MEMORIZED_FULL_VERSION.set(toJsonStruct(VersionInfo.OUR_FULL_VERSION));
        }

        result.put("user", user);
        result.put("stormVersion", VersionInfo.getVersion());
        result.put("stormVersionInfo", MEMORIZED_FULL_VERSION.get());
        List<SupervisorSummary> supervisorSummaries = clusterSummary.get_supervisors();
        result.put("supervisors", supervisorSummaries.size());
        result.put("topologies", clusterSummary.get_topologies_size());

        int usedSlots =
                supervisorSummaries.stream().mapToInt(
                        SupervisorSummary::get_num_used_workers).sum();
        result.put("slotsUsed", usedSlots);

        int totalSlots =
                supervisorSummaries.stream().mapToInt(
                        SupervisorSummary::get_num_workers).sum();
        result.put("slotsTotal", totalSlots);
        result.put("slotsFree", totalSlots - usedSlots);

        List<TopologySummary> topologySummaries = clusterSummary.get_topologies();
        int totalTasks =
                topologySummaries.stream().mapToInt(
                        TopologySummary::get_num_tasks).sum();
        result.put("tasksTotal", totalTasks);

        int totalExecutors =
                topologySummaries.stream().mapToInt(
                        TopologySummary::get_num_executors).sum();
        result.put("executorsTotal", totalExecutors);


        double supervisorTotalMemory =
                supervisorSummaries.stream().mapToDouble(x -> x.get_total_resources().getOrDefault(
                        Constants.COMMON_TOTAL_MEMORY_RESOURCE_NAME,
                        x.get_total_resources().get(Config.SUPERVISOR_MEMORY_CAPACITY_MB)
                        )
                ).sum();
        result.put("totalMem", supervisorTotalMemory);

        double supervisorTotalCpu =
                supervisorSummaries.stream().mapToDouble(x -> x.get_total_resources().getOrDefault(
                        Constants.COMMON_CPU_RESOURCE_NAME,
                        x.get_total_resources().get(Config.SUPERVISOR_CPU_CAPACITY)
                        )
                ).sum();
        result.put("totalCpu", supervisorTotalCpu);

        double supervisorUsedMemory =
            supervisorSummaries.stream().mapToDouble(SupervisorSummary::get_used_mem).sum();
        result.put("availMem", supervisorTotalMemory - supervisorUsedMemory);

        double supervisorUsedCpu =
            supervisorSummaries.stream().mapToDouble(SupervisorSummary::get_used_cpu).sum();
        result.put("availCpu", supervisorTotalCpu - supervisorUsedCpu);
        result.put("fragmentedMem", supervisorSummaries.stream().mapToDouble(SupervisorSummary::get_fragmented_mem).sum());
        result.put("fragmentedCpu", supervisorSummaries.stream().mapToDouble(SupervisorSummary::get_fragmented_cpu).sum());
        result.put("schedulerDisplayResource",
                conf.get(DaemonConfig.SCHEDULER_DISPLAY_RESOURCE));
        result.put("memAssignedPercentUtil", supervisorTotalMemory > 0
                ? StatsUtil.floatStr((supervisorUsedMemory  * 100.0) / supervisorTotalMemory) : "0.0");
        result.put("cpuAssignedPercentUtil", supervisorTotalCpu > 0
                ? StatsUtil.floatStr((supervisorUsedCpu * 100.0) / supervisorTotalCpu) : "0.0");
        result.put("bugtracker-url", conf.get(DaemonConfig.UI_PROJECT_BUGTRACKER_URL));
        result.put("central-log-url", conf.get(DaemonConfig.UI_CENTRAL_LOGGING_URL));

        Map<String, Double> usedGenericResources = new HashMap<>();
        Map<String, Double> totalGenericResources = new HashMap<>();
        for (SupervisorSummary ss : supervisorSummaries) {
            usedGenericResources = NormalizedResourceRequest.addResourceMap(usedGenericResources, ss.get_used_generic_resources());
            totalGenericResources = NormalizedResourceRequest.addResourceMap(totalGenericResources, ss.get_total_resources());
        }
        Map<String, Double> availGenericResources = NormalizedResourceRequest
                .subtractResourceMap(totalGenericResources, usedGenericResources);
        result.put("availGenerics", prettifyGenericResources(availGenericResources));
        result.put("totalGenerics", prettifyGenericResources(totalGenericResources));
        return result;
    }

    private static String prettifyGenericResources(Map<String, Double> resourceMap) {
        if (resourceMap == null) {
            return null;
        }
        TreeMap<String, Double> treeGenericResources = new TreeMap<>(); // use TreeMap for deterministic ordering
        treeGenericResources.putAll(resourceMap);
        NormalizedResourceRequest.removeNonGenericResources(treeGenericResources);
        return treeGenericResources.toString()
                .replaceAll("[{}]", "")
                .replace(",", "");
    }

    /**
     * Prettify OwnerResourceSummary.
     * @param ownerResourceSummary ownerResourceSummary
     * @return Map of prettified OwnerResourceSummary.
     */
    public static Map<String, Object> unpackOwnerResourceSummary(
            OwnerResourceSummary ownerResourceSummary) {

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

        Map<String, Object> result = new HashMap();
        result.put("owner", ownerResourceSummary.get_owner());
        result.put("totalTopologies", ownerResourceSummary.get_total_topologies());
        result.put("totalExecutors", ownerResourceSummary.get_total_executors());
        result.put("totalWorkers", ownerResourceSummary.get_total_workers());
        result.put("totalTasks", ownerResourceSummary.get_total_tasks());
        result.put("totalMemoryUsage", ownerResourceSummary.get_memory_usage());
        result.put("totalCpuUsage", ownerResourceSummary.get_cpu_usage());

        result.put("memoryGuarantee", memoryGuarantee != -1 ? memoryGuarantee : "N/A");
        result.put("cpuGuarantee", cpuGuaranteee != -1 ? cpuGuaranteee : "N/A");
        result.put("isolatedNodes", isolatedNodeGuarantee);

        result.put("memoryGuaranteeRemaining",
                memoryGuaranteeRemaining != -1 ? memoryGuaranteeRemaining : "N/A");
        result.put("cpuGuaranteeRemaining",
                cpuGuaranteeRemaining != -1 ? cpuGuaranteeRemaining : "N/A");
        result.put("totalReqOnHeapMem", ownerResourceSummary.get_requested_on_heap_memory());

        result.put("totalReqOffHeapMem", ownerResourceSummary.get_requested_off_heap_memory());

        result.put("totalReqMem", ownerResourceSummary.get_requested_total_memory());
        result.put("totalReqCpu", ownerResourceSummary.get_requested_cpu());
        result.put("totalAssignedOnHeapMem",
                ownerResourceSummary.get_assigned_on_heap_memory()
        );
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

    /**
     * getTopologyMap.
     * @param topologySummary topologySummary
     * @return getTopologyMap
     */
    public static Map<String, Object> getTopologyMap(TopologySummary topologySummary) {
        Map<String, Object> result = new HashMap();
        result.put("id", topologySummary.get_id());
        result.put("encodedId", Utils.urlEncodeUtf8(topologySummary.get_id()));
        result.put("owner", topologySummary.get_owner());
        result.put("name", topologySummary.get_name());
        result.put("status", topologySummary.get_status());
        result.put("uptime", UIHelpers.prettyUptimeSec(topologySummary.get_uptime_secs()));
        result.put("uptimeSeconds", topologySummary.get_uptime_secs());
        result.put("tasksTotal", topologySummary.get_num_tasks());
        result.put("workersTotal", topologySummary.get_num_workers());
        result.put("executorsTotal", topologySummary.get_num_executors());
        result.put("replicationCount", topologySummary.get_replication_count());
        result.put("schedulerInfo", topologySummary.get_sched_status());
        result.put("requestedMemOnHeap", topologySummary.get_requested_memonheap());
        result.put("requestedMemOffHeap", topologySummary.get_requested_memoffheap());
        result.put("requestedTotalMem",
                topologySummary.get_requested_memoffheap()
                        + topologySummary.get_assigned_memonheap());
        result.put("requestedCpu", topologySummary.get_requested_cpu());
        result.put("requestedGenericResources", prettifyGenericResources(topologySummary.get_requested_generic_resources()));
        result.put("assignedMemOnHeap", topologySummary.get_assigned_memonheap());
        result.put("assignedMemOffHeap", topologySummary.get_assigned_memoffheap());
        result.put("assignedTotalMem",
                topologySummary.get_assigned_memoffheap()
                        + topologySummary.get_assigned_memonheap());
        result.put("assignedCpu", topologySummary.get_assigned_cpu());
        result.put("assignedGenericResources", prettifyGenericResources(topologySummary.get_assigned_generic_resources()));
        result.put("topologyVersion", topologySummary.get_topology_version());
        result.put("stormVersion", topologySummary.get_storm_version());
        return result;
    }

    /**
     * Get a specific owner resource summary.
     * @param ownerResourceSummaries Result from thrift call.
     * @param client client
     * @param id Owner id.
     * @param config Storm conf.
     * @return prettified owner resource summary.
     */
    public static Map<String, Object> getOwnerResourceSummary(
            List<OwnerResourceSummary> ownerResourceSummaries,
            Nimbus.Iface client, String id, Map<String, Object> config) throws TException {
        Map<String, Object> result = new HashMap();
        result.put("schedulerDisplayResource", config.get(DaemonConfig.SCHEDULER_DISPLAY_RESOURCE));

        if (ownerResourceSummaries.isEmpty()) {
            return unpackOwnerResourceSummary(new OwnerResourceSummary(id));
        }

        List<TopologySummary> topologies = client.getTopologySummaries();
        List<Map> topologySummaries = getTopologiesMap(id, topologies);

        result.putAll(unpackOwnerResourceSummary(ownerResourceSummaries.get(0)));
        result.put("topologies", topologySummaries);

        return result;
    }

    /**
     * getTopologiesMap.
     * @param id id
     * @param topologies topologies
     * @return getTopologiesMap
     */
    private static List<Map> getTopologiesMap(String id, List<TopologySummary> topologies) {
        List<Map> topologySummaries = new ArrayList();

        for (TopologySummary topologySummary : topologies) {
            if (id == null || topologySummary.get_owner().equals(id)) {
                topologySummaries.add(getTopologyMap(topologySummary));
            }
        }
        return topologySummaries;
    }

    /**
     * getLogviewerLink.
     * @param host host
     * @param fname fname
     * @param config config
     * @param port port
     * @return getLogviewerLink.
     */
    public static String getLogviewerLink(String host, String fname,
                                          Map<String, Object> config, int port) {
        if (isSecureLogviewer(config)) {
            return UIHelpers.urlFormat("https://%s:%s/api/v1/log?file=%s",
                    host, config.get(DaemonConfig.LOGVIEWER_HTTPS_PORT), fname);
        } else {
            return UIHelpers.urlFormat("http://%s:%s/api/v1/log?file=%s",
                    host, config.get(DaemonConfig.LOGVIEWER_PORT), fname);
        }
    }

    /**
     * Get log link to nimbus log.
     * @param host nimbus host name
     * @param config storm config
     * @return log link.
     */
    public static String getNimbusLogLink(String host, Map<String, Object> config) {
        if (isSecureLogviewer(config)) {
            return UIHelpers.urlFormat("https://%s:%s/api/v1/daemonlog?file=nimbus.log",
                    host, config.get(DaemonConfig.LOGVIEWER_HTTPS_PORT));
        }
        return UIHelpers.urlFormat("http://%s:%s/api/v1/daemonlog?file=nimbus.log",
                host, config.get(DaemonConfig.LOGVIEWER_PORT));
    }

    /**
     * Get log link to supervisor log.
     * @param host supervisor host name
     * @param config storm config
     * @return log link.
     */
    public static String getSupervisorLogLink(String host, Map<String, Object> config) {
        if (isSecureLogviewer(config)) {
            return UIHelpers.urlFormat("https://%s:%s/api/v1/daemonlog?file=supervisor.log",
                    host, config.get(DaemonConfig.LOGVIEWER_HTTPS_PORT));
        }
        return UIHelpers.urlFormat("http://%s:%s/api/v1/daemonlog?file=supervisor.log",
                host, config.get(DaemonConfig.LOGVIEWER_PORT));
    }

    /**
     * Get log link to supervisor log.
     * @param host supervisor host name
     * @param config storm config
     * @return log link.
     */
    public static String getWorkerLogLink(String host, int port,
                                          Map<String, Object> config, String topologyId) {
        return getLogviewerLink(host,
                WebAppUtils.logsFilename(
                        topologyId, String.valueOf(port)),
                config, port
        );
    }

    /**
     * Get supervisor info in a map.
     * @param supervisorSummary from nimbus call.
     * @param config Storm config.
     * @return prettified supervisor info map.
     */
    public static Map<String, Object> getPrettifiedSupervisorMap(
            SupervisorSummary supervisorSummary,
            Map<String, Object> config) {
        Map<String, Object> result = new HashMap();

        result.put("id", supervisorSummary.get_supervisor_id());
        result.put("host", supervisorSummary.get_host());
        result.put("uptime", UIHelpers.prettyUptimeSec(supervisorSummary.get_uptime_secs()));
        result.put("blacklisted", supervisorSummary.is_blacklisted());
        result.put("uptimeSeconds", supervisorSummary.get_uptime_secs());
        result.put("slotsTotal", supervisorSummary.get_num_workers());
        result.put("slotsUsed", supervisorSummary.get_num_used_workers());
        result.put("slotsFree",
                Integer.max(supervisorSummary.get_num_workers()
                        - supervisorSummary.get_num_used_workers(), 0));
        Map<String, Double> totalResources = supervisorSummary.get_total_resources();
        Double totalMemory = totalResources.getOrDefault(
                Constants.COMMON_TOTAL_MEMORY_RESOURCE_NAME,
                totalResources.get(Config.SUPERVISOR_MEMORY_CAPACITY_MB)
        );
        result.put("totalMem",
                totalMemory
        );
        Double totalCpu = totalResources.getOrDefault(
                Constants.COMMON_CPU_RESOURCE_NAME,
                totalResources.get(Config.SUPERVISOR_CPU_CAPACITY)
        );
        result.put("totalCpu",
                totalCpu);
        result.put("usedMem", supervisorSummary.get_used_mem());
        result.put("usedCpu", supervisorSummary.get_used_cpu());
        result.put(
                "logLink",
                getSupervisorLogLink(supervisorSummary.get_host(), config)
        );
        result.put("availMem", totalMemory - supervisorSummary.get_used_mem());
        result.put("availCpu", totalCpu - supervisorSummary.get_used_cpu());
        result.put("version", supervisorSummary.get_version());

        Map<String, Double> totalGenericResources = new HashMap<>(totalResources);
        result.put("totalGenericResources", prettifyGenericResources(totalGenericResources));
        Map<String, Double> usedGenericResources = supervisorSummary.get_used_generic_resources();
        result.put("usedGenericResources", prettifyGenericResources(usedGenericResources));
        Map<String, Double> availGenericResources = NormalizedResourceRequest
                .subtractResourceMap(totalGenericResources, usedGenericResources);
        result.put("availGenericResources", prettifyGenericResources(availGenericResources));

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

    /**
     * getWorkerSummaries.
     * @param supervisorPageInfo supervisorPageInfo
     * @param config config
     * @return getWorkerSummaries
     */
    public static List<Map> getWorkerSummaries(SupervisorPageInfo supervisorPageInfo,
                                               Map<String, Object> config) {
        List<Map> workerSummaries = new ArrayList();
        if (supervisorPageInfo.is_set_worker_summaries()) {
            for (WorkerSummary workerSummary : supervisorPageInfo.get_worker_summaries()) {
                workerSummaries.add(getWorkerSummaryMap(workerSummary, config));
            }
        }
        return workerSummaries;
    }

    /**
     * getWorkerSummaryMap.
     * @param workerSummary workerSummary
     * @param config config
     * @return getWorkerSummaryMap
     */
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
        result.put("uptime", UIHelpers.prettyUptimeSec(workerSummary.get_uptime_secs()));
        result.put("uptimeSeconds", workerSummary.get_uptime_secs());
        result.put("workerLogLink", getWorkerLogLink(workerSummary.get_host(),
                workerSummary.get_port(), config, workerSummary.get_topology_id()));
        result.put("owner", workerSummary.get_owner());
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
            List<SupervisorSummary> supervisors,
            SecurityContext securityContext, Map<String, Object> config) {
        Map<String, Object> result = new HashMap();
        addLogviewerInfo(config, result);
        List<Map> supervisorMaps = getSupervisorsMap(supervisors, config);
        result.put("supervisors", supervisorMaps);
        result.put("schedulerDisplayResource",
                config.get(DaemonConfig.SCHEDULER_DISPLAY_RESOURCE)
        );

        return result;
    }

    /**
     * getSupervisorsMap.
     * @param supervisors supervisors
     * @param config config
     * @return getSupervisorsMap
     */
    private static List<Map> getSupervisorsMap(List<SupervisorSummary> supervisors,
                                               Map<String, Object> config) {
        List<Map> supervisorMaps = new ArrayList<>();
        for (SupervisorSummary supervisorSummary : supervisors) {
            supervisorMaps.add(getPrettifiedSupervisorMap(supervisorSummary, config));
        }
        return supervisorMaps;
    }

    /**
     * addLogviewerInfo.
     * @param config config
     * @param result result
     */
    private static void addLogviewerInfo(Map<String, Object> config, Map<String, Object> result) {
        result.put("logviewerPort", getLogviewerPort(config));
        String logviewerScheme = "http";
        if (isSecureLogviewer(config)) {
            logviewerScheme = "https";
        }
        result.put("logviewerScheme", logviewerScheme);
    }

    /**
     * getSupervisorPageInfo.
     * @param supervisorPageInfo supervisorPageInfo
     * @param config config
     * @return getSupervisorPageInfo
     */
    public static Map<String, Object> getSupervisorPageInfo(
            SupervisorPageInfo supervisorPageInfo, Map<String, Object> config) {
        Map<String, Object> result = new HashMap<>();
        result.put("workers", getWorkerSummaries(supervisorPageInfo, config));
        result.put("schedulerDisplayResource", config.get(DaemonConfig.SCHEDULER_DISPLAY_RESOURCE));
        List<Map> supervisorMaps = getSupervisorsMap(supervisorPageInfo.get_supervisor_summaries(), config);
        result.put("supervisors", supervisorMaps);
        addLogviewerInfo(config, result);
        return result;
    }

    /**
     * getAllTopologiesSummary.
     * @param topologies topologies
     * @param config config
     * @return getAllTopologiesSummary
     */
    public static Map<String, Object> getAllTopologiesSummary(
            List<TopologySummary> topologies, Map<String, Object> config) {
        Map<String, Object> result = new HashMap();
        result.put("topologies", getTopologiesMap(null, topologies));
        result.put("schedulerDisplayResource", config.get(DaemonConfig.SCHEDULER_DISPLAY_RESOURCE));
        return result;
    }

    /**
     * getWindowHint.
     * @param window window
     * @return getWindowHint
     */
    public static String getWindowHint(String window) {
        if (window.equals(":all-time")) {
            return "All time";
        }
        return UIHelpers.prettyUptimeSec(window);
    }

    /**
     * getStatDisplayMap.
     * @param rawDisplayMap rawDisplayMap
     * @return getStatDisplayMap
     */
    public static Map<String, Double> getStatDisplayMap(Map<String, Double> rawDisplayMap) {
        Map<String, Double> result = new HashMap();
        for (Map.Entry<String, Double> entry : rawDisplayMap.entrySet()) {
            result.put(getWindowHint(entry.getKey()), entry.getValue());
        }

        return result;
    }

    /**
     * getTopologySummary.
     * @param topologyPageInfo topologyPageInfo
     * @param window window
     * @param config config
     * @param remoteUser remoteUser
     * @return getTopologySummary
     */
    public static Map<String, Object> getTopologySummary(TopologyPageInfo topologyPageInfo,
                                                         String window, Map<String, Object> config, String remoteUser) {
        Map<String, Object> result = new HashMap();
        Map<String, Object> topologyConf = (Map<String, Object>) JSONValue.parse(topologyPageInfo.get_topology_conf());
        long messageTimeout = (long) topologyConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS);
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
        result.put("bugtracker-url", config.get(DaemonConfig.UI_PROJECT_BUGTRACKER_URL));
        result.put("central-log-url", config.get(DaemonConfig.UI_CENTRAL_LOGGING_URL));
        return result;
    }

    /**
     * getStatDisplayMapLong.
     * @param windowToTransferred windowToTransferred
     * @return getStatDisplayMapLong
     */
    private static Map<String, Long> getStatDisplayMapLong(Map<String, Long> windowToTransferred) {
        Map<String, Long> result = new HashMap();
        for (Map.Entry<String, Long> entry : windowToTransferred.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * getCommonAggStatsMap.
     * @param commonAggregateStats commonAggregateStats
     * @return getCommonAggStatsMap
     */
    private static Map<String, Object> getCommonAggStatsMap(CommonAggregateStats commonAggregateStats) {
        Map<String, Object> result = new HashMap();
        result.put("executors", commonAggregateStats.get_num_executors());
        result.put("tasks", commonAggregateStats.get_num_tasks());
        result.put("emitted", commonAggregateStats.get_emitted());
        result.put("transferred", commonAggregateStats.get_transferred());
        result.put("acked", commonAggregateStats.get_acked());
        result.put("failed", commonAggregateStats.get_failed());
        if (commonAggregateStats.is_set_resources_map()) {
            result.put(
                    "requestedMemOnHeap",
                    commonAggregateStats.get_resources_map().get(Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME)
            );
            result.put(
                    "requestedMemOffHeap",
                    commonAggregateStats.get_resources_map().get(Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME));
            result.put(
                    "requestedCpu",
                    commonAggregateStats.get_resources_map().get(Constants.COMMON_CPU_RESOURCE_NAME));
            result.put(
                    "requestedGenericResourcesComp",
                    prettifyGenericResources(commonAggregateStats.get_resources_map()));
        }
        return result;
    }

    /**
     * getTruncatedErrorString.
     * @param errorString errorString
     * @return getTruncatedErrorString
     */
    private static String getTruncatedErrorString(String errorString) {
        return errorString.substring(0, Math.min(errorString.length(), 200));
    }

    /**
     * getSpoutAggStatsMap.
     * @param componentAggregateStats componentAggregateStats
     * @param window window
     * @return getSpoutAggStatsMap
     */
    private static Map<String, Object> getSpoutAggStatsMap(
            ComponentAggregateStats componentAggregateStats, String window) {
        Map<String, Object> result = new HashMap();
        SpoutAggregateStats spoutAggregateStats = componentAggregateStats.get_specific_stats().get_spout();
        CommonAggregateStats commonStats = componentAggregateStats.get_common_stats();
        result.put("window", window);
        result.put("windowPretty", getWindowHint(window));
        result.put("emitted", commonStats.get_emitted());
        result.put("transferred", commonStats.get_transferred());
        result.put("acked", commonStats.get_acked());
        result.put("failed", commonStats.get_failed());
        result.put("completeLatency", spoutAggregateStats.get_complete_latency_ms());

        ErrorInfo lastError = componentAggregateStats.get_last_error();
        result.put("lastError", Objects.isNull(lastError) ?  "" : getTruncatedErrorString(lastError.get_error()));
        return result;
    }

    /**
     * getBoltAggStatsMap.
     * @param componentAggregateStats componentAggregateStats
     * @param window window
     * @return getBoltAggStatsMap
     */
    private static Map<String, Object> getBoltAggStatsMap(
            ComponentAggregateStats componentAggregateStats, String window) {
        Map<String, Object> result = new HashMap();
        CommonAggregateStats commonStats = componentAggregateStats.get_common_stats();
        result.put("window", window);
        result.put("windowPretty", getWindowHint(window));
        result.put("emitted", commonStats.get_emitted());
        result.put("transferred", commonStats.get_transferred());
        result.put("acked", commonStats.get_acked());
        result.put("failed", commonStats.get_failed());
        BoltAggregateStats boltAggregateStats = componentAggregateStats.get_specific_stats().get_bolt();
        result.put("executeLatency", StatsUtil.floatStr(boltAggregateStats.get_execute_latency_ms()));
        result.put("executed", boltAggregateStats.get_executed());
        result.put("processLatency", StatsUtil.floatStr(boltAggregateStats.get_process_latency_ms()));
        result.put("capacity", StatsUtil.floatStr(boltAggregateStats.get_capacity()));
        return result;
    }

    /**
     * nullToZero.
     * @param value value
     * @return nullToZero
     */
    private static Long nullToZero(Long value) {
        return !Objects.isNull(value) ? value : 0;
    }

    /**
     * nullToZero.
     * @param value value
     * @return nullToZero
     */
    private static Double nullToZero(Double value) {
        return !Objects.isNull(value) ? value : 0;
    }

    /**
     * getBoltInputStats.
     * @param globalStreamId globalStreamId
     * @param componentAggregateStats componentAggregateStats
     * @return getBoltInputStats
     */
    private static Map<String, Object> getBoltInputStats(GlobalStreamId globalStreamId,
                                                         ComponentAggregateStats componentAggregateStats) {
        Map<String, Object> result = new HashMap();
        SpecificAggregateStats specificAggregateStats = componentAggregateStats.get_specific_stats();
        BoltAggregateStats boltAggregateStats = specificAggregateStats.get_bolt();
        CommonAggregateStats commonAggregateStats = componentAggregateStats.get_common_stats();
        String componentId = globalStreamId.get_componentId();
        result.put("component", componentId);
        result.put("encodedComponentId", Utils.urlEncodeUtf8(componentId));
        result.put("stream", globalStreamId.get_streamId());
        result.put("executeLatency", StatsUtil.floatStr(boltAggregateStats.get_execute_latency_ms()));
        result.put("processLatency", StatsUtil.floatStr(boltAggregateStats.get_process_latency_ms()));
        result.put("executed", nullToZero(boltAggregateStats.get_executed()));
        result.put("acked", nullToZero(commonAggregateStats.get_acked()));
        result.put("failed", nullToZero(commonAggregateStats.get_failed()));
        return result;
    }

    /**
     * getBoltOutputStats.
     * @param streamId streamId
     * @param componentAggregateStats componentAggregateStats
     * @return getBoltOutputStats
     */
    private static Map<String, Object> getBoltOutputStats(String streamId,
                                                          ComponentAggregateStats componentAggregateStats) {
        Map<String, Object> result = new HashMap();
        result.put("stream", streamId);
        CommonAggregateStats commonStats = componentAggregateStats.get_common_stats();
        result.put("emitted", nullToZero(commonStats.get_emitted()));
        result.put("transferred", nullToZero(commonStats.get_transferred()));
        return result;
    }

    /**
     * getSpoutOutputStats.
     * @param streamId streamId
     * @param componentAggregateStats componentAggregateStats
     * @return getSpoutOutputStats
     */
    private static Map<String, Object> getSpoutOutputStats(String streamId,
                                                           ComponentAggregateStats componentAggregateStats) {
        SpecificAggregateStats specificAggregateStats = componentAggregateStats.get_specific_stats();
        SpoutAggregateStats spoutAggregateStats = specificAggregateStats.get_spout();
        Map<String, Object> result = new HashMap();
        result.put("stream", streamId);
        CommonAggregateStats commonStats = componentAggregateStats.get_common_stats();
        result.put("emitted", nullToZero(commonStats.get_emitted()));
        result.put("transferred", nullToZero(commonStats.get_transferred()));
        result.put("completeLatency", StatsUtil.floatStr(spoutAggregateStats.get_complete_latency_ms()));
        result.put("acked", nullToZero(commonStats.get_acked()));
        result.put("failed", nullToZero(commonStats.get_failed()));
        return result;
    }

    /**
     * getBoltExecutorStats.
     * @param topologyId topologyId
     * @param config config
     * @param executorAggregateStats executorAggregateStats
     * @return getBoltExecutorStats
     */
    private static Map<String, Object> getBoltExecutorStats(String topologyId, Map<String, Object> config,
                                                            ExecutorAggregateStats executorAggregateStats) {
        Map<String, Object> result = new HashMap();
        ExecutorSummary executorSummary = executorAggregateStats.get_exec_summary();
        ExecutorInfo executorInfo = executorSummary.get_executor_info();
        String executorId = prettyExecutorInfo(executorInfo);
        result.put("id", executorId);
        result.put("encodedId", Utils.urlEncodeUtf8(executorId));
        result.put("uptime", prettyUptimeSec(executorSummary.get_uptime_secs()));
        result.put("uptimeSeconds", executorSummary.get_uptime_secs());
        String host = executorSummary.get_host();
        result.put("host", host);
        int port = executorSummary.get_port();
        result.put("port", port);
        
        ComponentAggregateStats componentAggregateStats = executorAggregateStats.get_stats();
        CommonAggregateStats commonAggregateStats = componentAggregateStats.get_common_stats();
        result.put("emitted", nullToZero(commonAggregateStats.get_emitted()));
        result.put("transferred", nullToZero(commonAggregateStats.get_transferred()));
        
        SpecificAggregateStats specificAggregateStats = componentAggregateStats.get_specific_stats();
        BoltAggregateStats boltAggregateStats = specificAggregateStats.get_bolt();
        result.put("capacity",  StatsUtil.floatStr(nullToZero(boltAggregateStats.get_capacity())));
        result.put("executeLatency", StatsUtil.floatStr(boltAggregateStats.get_execute_latency_ms()));
        result.put("executed", nullToZero(boltAggregateStats.get_executed()));
        result.put("processLatency", StatsUtil.floatStr(boltAggregateStats.get_process_latency_ms()));
        result.put("acked", nullToZero(commonAggregateStats.get_acked()));
        result.put("failed", nullToZero(commonAggregateStats.get_failed()));
        result.put("workerLogLink", getWorkerLogLink(host, port, config, topologyId));
        return result;
    }

    /**
     * getSpoutExecutorStats.
     * @param topologyId topologyId
     * @param config config
     * @param executorAggregateStats executorAggregateStats
     * @return getSpoutExecutorStats
     */
    private static Map<String, Object> getSpoutExecutorStats(String topologyId, Map<String, Object> config,
                                                             ExecutorAggregateStats executorAggregateStats) {
        Map<String, Object> result = new HashMap();
        ExecutorSummary executorSummary = executorAggregateStats.get_exec_summary();
        ExecutorInfo executorInfo = executorSummary.get_executor_info();
        ComponentAggregateStats componentAggregateStats = executorAggregateStats.get_stats();
        SpecificAggregateStats specificAggregateStats = componentAggregateStats.get_specific_stats();
        SpoutAggregateStats spoutAggregateStats = specificAggregateStats.get_spout();
        CommonAggregateStats commonAggregateStats = componentAggregateStats.get_common_stats();
        String executorId = prettyExecutorInfo(executorInfo);
        result.put("id", executorId);
        result.put("encodedId", Utils.urlEncodeUtf8(executorId));
        result.put("uptime", prettyUptimeSec(executorSummary.get_uptime_secs()));
        result.put("uptimeSeconds", executorSummary.get_uptime_secs());
        String host = executorSummary.get_host();
        result.put("host", host);
        int port = executorSummary.get_port();
        result.put("port", port);
        result.put("emitted", nullToZero(commonAggregateStats.get_emitted()));
        result.put("transferred", nullToZero(commonAggregateStats.get_transferred()));
        result.put("completeLatency", StatsUtil.floatStr(spoutAggregateStats.get_complete_latency_ms()));
        result.put("acked", nullToZero(commonAggregateStats.get_acked()));
        result.put("failed", nullToZero(commonAggregateStats.get_failed()));
        result.put("workerLogLink", getWorkerLogLink(host, port, config, topologyId));
        return result;
    }

    /**
     * getComponentLastErrorInfo.
     * Internal helper method that populates a hashmap with the component's most recently reported error.
     * If the component has no such error reported, an empty "template" suitable for return over the
     * REST api is returned.
     *
     * @param lastError errorInfo The components most recently reported error.
     * @param config config Topology configuration map.
     * @param topologyId topologyId.
     * @return Map of values representing details about the most recently reported error.
     */
    private static Map<String, Object> getComponentLastErrorInfo(ErrorInfo lastError, Map config, String topologyId) {
        Map<String, Object> result = new HashMap<>();

        // Maintain backwards compatibility by defaulting these fields to empty string or null.
        // If the lastError parameter is non-null, these keys will be populated with the appropriate values below.
        result.put("lastError", "");
        result.put("errorHost", "");
        result.put("errorPort", (Integer) null);
        result.put("errorWorkerLogLink", "");
        result.put("errorTime", null);
        result.put("errorLapsedSecs", null);

        if (!Objects.isNull(lastError)) {
            result.putAll(getComponentErrorInfo(lastError, config, topologyId, true));
        }
        return result;
    }

    /**
     * getComponentErrorInfo.
     * @param errorInfo errorInfo
     * @param config config
     * @param topologyId topologyId
     * @param asLastError Pass a value of true if the result is to be used as part of a components 'lastError' response.
     *                    Pass a value of false if the result is to be used as part of a components 'errors' response.
     * @return getComponentErrorInfo
     */
    private static Map<String, Object> getComponentErrorInfo(ErrorInfo errorInfo, Map config,
                                                             String topologyId, boolean asLastError) {
        Map<String, Object> result = new HashMap();
        result.put("errorTime",
                errorInfo.get_error_time_secs());
        String host = errorInfo.get_host();
        result.put("errorHost", host);
        int port = errorInfo.get_port();
        result.put("errorPort", port);
        result.put("errorWorkerLogLink", getWorkerLogLink(host, port, config, topologyId));
        result.put("errorLapsedSecs", Time.deltaSecs(errorInfo.get_error_time_secs()));

        if (asLastError) {
            result.put("lastError", getTruncatedErrorString(errorInfo.get_error()));
        } else {
            result.put("error", errorInfo.get_error());
        }

        return result;
    }

    /**
     * getComponentErrors.
     * @param errorInfoList errorInfoList
     * @param topologyId topologyId
     * @param config config
     * @return getComponentErrors
     */
    private static Map<String, Object> getComponentErrors(List<ErrorInfo> errorInfoList,
                                                          String topologyId, Map config) {
        Map<String, Object> result = new HashMap();
        errorInfoList.sort(Comparator.comparingInt(ErrorInfo::get_error_time_secs));
        result.put(
                "componentErrors",
                errorInfoList.stream().map(e -> getComponentErrorInfo(e, config, topologyId, false))
                        .collect(Collectors.toList())
        );
        return result;
    }

    /**
     * getTopologyErrors.
     * @param errorInfoList errorInfoList
     * @param topologyId topologyId
     * @param config config
     * @return getTopologyErrors
     */
    private static Map<String, Object> getTopologyErrors(List<ErrorInfo> errorInfoList,
                                                         String topologyId, Map config) {
        Map<String, Object> result = new HashMap();
        errorInfoList.sort(Comparator.comparingInt(ErrorInfo::get_error_time_secs));
        result.put(
                "topologyErrors",
                errorInfoList.stream().map(e -> getComponentErrorInfo(e, config, topologyId, false))
                        .collect(Collectors.toList())
        );
        return result;
    }

    /**
     * getTopologySpoutAggStatsMap.
     * @param componentAggregateStats componentAggregateStats
     * @param spoutId spoutId
     * @return getTopologySpoutAggStatsMap
     */
    private static Map<String, Object> getTopologySpoutAggStatsMap(ComponentAggregateStats componentAggregateStats,
                                                                   String spoutId, Map<String, Object> config, String topologyId) {
        Map<String, Object> result = new HashMap();
        CommonAggregateStats commonStats = componentAggregateStats.get_common_stats();
        result.putAll(getCommonAggStatsMap(commonStats));
        result.put("spoutId", spoutId);
        result.put("encodedSpoutId", Utils.urlEncodeUtf8(spoutId));
        SpoutAggregateStats spoutAggregateStats = componentAggregateStats.get_specific_stats().get_spout();
        result.put("completeLatency", StatsUtil.floatStr(spoutAggregateStats.get_complete_latency_ms()));
        result.putAll(getComponentLastErrorInfo(componentAggregateStats.get_last_error(), config, topologyId));
        return result;
    }

    /**
     * getTopologyBoltAggStatsMap.
     * @param componentAggregateStats componentAggregateStats
     * @param boltId boltId
     * @return getTopologyBoltAggStatsMap
     */
    private static Map<String, Object> getTopologyBoltAggStatsMap(ComponentAggregateStats componentAggregateStats,
                                                                  String boltId, Map<String, Object> config, String topologyId) {
        Map<String, Object> result = new HashMap();
        CommonAggregateStats commonStats = componentAggregateStats.get_common_stats();
        result.putAll(getCommonAggStatsMap(commonStats));
        result.put("boltId", boltId);
        result.put("encodedBoltId", Utils.urlEncodeUtf8(boltId));
        BoltAggregateStats boltAggregateStats = componentAggregateStats.get_specific_stats().get_bolt();
        result.put("capacity", StatsUtil.floatStr(boltAggregateStats.get_capacity()));
        result.put("executeLatency", StatsUtil.floatStr(boltAggregateStats.get_execute_latency_ms()));
        result.put("executed", boltAggregateStats.get_executed());
        result.put("processLatency", StatsUtil.floatStr(boltAggregateStats.get_process_latency_ms()));
        result.putAll(getComponentLastErrorInfo(componentAggregateStats.get_last_error(), config, topologyId));
        return result;
    }

    /**
     * getTopologyStatsMap.
     * @param topologyStats topologyStats
     * @return getTopologyStatsMap
     */
    private static List<Map> getTopologyStatsMap(TopologyStats topologyStats) {
        List<Map> result = new ArrayList();

        Map<String, Long> emittedStatDisplayMap = getStatDisplayMapLong(topologyStats.get_window_to_emitted());
        Map<String, Long> transferred = getStatDisplayMapLong(topologyStats.get_window_to_transferred());
        Map<String, Double> completeLatency = getStatDisplayMap(topologyStats.get_window_to_complete_latencies_ms());
        Map<String, Long> acked = getStatDisplayMapLong(topologyStats.get_window_to_acked());
        Map<String, Long> failed = getStatDisplayMapLong(topologyStats.get_window_to_failed());
        for (String window : emittedStatDisplayMap.keySet()) {
            Map<String, Object> temp = new HashMap();
            temp.put("windowPretty", getWindowHint(window));
            temp.put("window", window);
            temp.put("emitted", emittedStatDisplayMap.get(window));
            temp.put("transferred", transferred.get(window));
            temp.put("completeLatency", StatsUtil.floatStr(completeLatency.get(getWindowHint(window))));
            temp.put("acked", acked.getOrDefault(window, 0L));
            temp.put("failed", failed.getOrDefault(window, 0L));

            result.add(temp);
        }
        return result;
    }

    /**
     * unpackTopologyInfo.
     * @param topologyPageInfo topologyPageInfo
     * @param window window
     * @param config config
     * @return unpackTopologyInfo
     */
    private static Map<String, Object> unpackTopologyInfo(TopologyPageInfo topologyPageInfo, String window, Map<String, Object> config) {
        Map<String, Object> result = new HashMap();
        result.put("id", topologyPageInfo.get_id());
        result.put("encodedId", Utils.urlEncodeUtf8(topologyPageInfo.get_id()));
        result.put("owner", topologyPageInfo.get_owner());
        result.put("name", topologyPageInfo.get_name());
        result.put("status", topologyPageInfo.get_status());
        result.put("uptime", UIHelpers.prettyUptimeSec(topologyPageInfo.get_uptime_secs()));
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
        result.put("assignedTotalMem", topologyPageInfo.get_assigned_memonheap()
                + topologyPageInfo.get_assigned_memoffheap());
        result.put("assignedCpu", topologyPageInfo.get_assigned_cpu());
        result.put("requestedRegularOnHeapMem", topologyPageInfo.get_requested_regular_on_heap_memory());
        result.put("requestedSharedOnHeapMem", topologyPageInfo.get_requested_shared_on_heap_memory());
        result.put("requestedRegularOffHeapMem", topologyPageInfo.get_requested_regular_off_heap_memory());
        result.put("requestedSharedOffHeapMem", topologyPageInfo.get_requested_shared_off_heap_memory());
        result.put("requestedGenericResources", prettifyGenericResources(topologyPageInfo.get_requested_generic_resources()));
        result.put("assignedRegularOnHeapMem", topologyPageInfo.get_assigned_regular_on_heap_memory());
        result.put("assignedSharedOnHeapMem", topologyPageInfo.get_assigned_shared_on_heap_memory());
        result.put("assignedRegularOffHeapMem", topologyPageInfo.get_assigned_regular_off_heap_memory());
        result.put("assignedSharedOffHeapMem", topologyPageInfo.get_assigned_shared_off_heap_memory());
        result.put("assignedGenericResources", prettifyGenericResources(topologyPageInfo.get_assigned_generic_resources()));
        result.put("topologyStats", getTopologyStatsMap(topologyPageInfo.get_topology_stats()));
        List<Map> workerSummaries = new ArrayList();
        if (topologyPageInfo.is_set_workers()) {
            for (WorkerSummary workerSummary : topologyPageInfo.get_workers()) {
                workerSummaries.add(getWorkerSummaryMap(workerSummary, config));
            }
        }
        result.put("workers", workerSummaries);

        Map<String, ComponentAggregateStats> spouts = topologyPageInfo.get_id_to_spout_agg_stats();
        List<Map> spoutStats = new ArrayList();

        for (Map.Entry<String, ComponentAggregateStats> spoutEntry : spouts.entrySet()) {
            spoutStats.add(getTopologySpoutAggStatsMap(spoutEntry.getValue(), spoutEntry.getKey(), config, topologyPageInfo.get_id()));
        }
        result.put("spouts", spoutStats);

        Map<String, ComponentAggregateStats> bolts = topologyPageInfo.get_id_to_bolt_agg_stats();
        List<Map> boltStats = new ArrayList();

        for (Map.Entry<String, ComponentAggregateStats> boltEntry : bolts.entrySet()) {
            boltStats.add(getTopologyBoltAggStatsMap(boltEntry.getValue(), boltEntry.getKey(), config, topologyPageInfo.get_id()));
        }
        result.put("bolts", boltStats);


        result.put("configuration", topologyPageInfo.get_topology_conf());
        boolean debuggingEnabled = false;
        if (topologyPageInfo.is_set_debug_options()) {
            debuggingEnabled = topologyPageInfo.get_debug_options().is_enable();
        }
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

    /**
     * getTopologyWorkers.
     * @param topologyInfo topologyInfo
     * @param config config
     * @return getTopologyWorkers.
     */
    public static Map<String, Object> getTopologyWorkers(TopologyInfo topologyInfo, Map config) {
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
        Map<String, Object> result = new HashMap();
        result.put("hostPortList", executorSummaries);
        addLogviewerInfo(config, result);
        return result;
    }


    /**
     * getTopologyLag.
     * @param userTopology userTopology
     * @param config config
     * @return getTopologyLag.
     */
    public static Map<String, Map<String, Object>> getTopologyLag(StormTopology userTopology, Map<String, Object> config) {
        Boolean disableLagMonitoring = (Boolean) (config.get(DaemonConfig.UI_DISABLE_SPOUT_LAG_MONITORING));
        return disableLagMonitoring ? Collections.EMPTY_MAP : TopologySpoutLag.lag(userTopology, config);
    }

    /**
     * getBoltExecutors.
     * @param executorSummaries executorSummaries
     * @param stormTopology stormTopology
     * @param sys sys
     * @return getBoltExecutors.
     */
    public static Map<String, List<ExecutorSummary>> getBoltExecutors(List<ExecutorSummary> executorSummaries,
                                                                StormTopology stormTopology, boolean sys) {
        Map<String, List<ExecutorSummary>> result = new HashMap();
        for (ExecutorSummary executorSummary : executorSummaries) {
            if (StatsUtil.componentType(stormTopology, executorSummary.get_component_id()).equals("bolt")
                    && (sys || !Utils.isSystemId(executorSummary.get_component_id()))) {
                List<ExecutorSummary> executorSummaryList = result.getOrDefault(executorSummary.get_component_id(), new ArrayList());
                executorSummaryList.add(executorSummary);
                result.put(executorSummary.get_component_id(), executorSummaryList);
            }
        }
        return result;
    }

    /**
     * getSpoutExecutors.
     * @param executorSummaries executorSummaries
     * @param stormTopology stormTopology
     * @return getSpoutExecutors.
     */
    public static Map<String, List<ExecutorSummary>> getSpoutExecutors(List<ExecutorSummary> executorSummaries,
                                                                       StormTopology stormTopology) {
        Map<String, List<ExecutorSummary>> result = new HashMap();
        for (ExecutorSummary executorSummary : executorSummaries) {
            if (StatsUtil.componentType(stormTopology, executorSummary.get_component_id()).equals("spout")) {
                List<ExecutorSummary> executorSummaryList = result.getOrDefault(executorSummary.get_component_id(), new ArrayList());
                executorSummaryList.add(executorSummary);
                result.put(executorSummary.get_component_id(), executorSummaryList);
            }
        }
        return result;
    }

    /**
     * sanitizeStreamName.
     * @param streamName streamName
     * @return sanitizeStreamName
     */
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

    /**
     * sanitizeTransferredStats.
     * @param stats stats
     * @return sanitizeTransferredStats
     */
    public static  Map<String, Map<String, Long>> sanitizeTransferredStats(Map<String, Map<String, Long>> stats) {
        Map<String, Map<String, Long>> result = new HashMap();
        for (Map.Entry<String, Map<String, Long>> entry : stats.entrySet()) {
            Map<String, Long> temp = new HashMap();
            for (Map.Entry<String, Long> innerEntry : entry.getValue().entrySet()) {
                temp.put(sanitizeStreamName(innerEntry.getKey()), innerEntry.getValue());
            }
            result.put(entry.getKey(), temp);
        }
        return result;
    }

    /**
     * getStatMapFromExecutorSummary.
     * @param executorSummary executorSummary
     * @return getStatMapFromExecutorSummary
     */
    public static Map<String, Object> getStatMapFromExecutorSummary(ExecutorSummary executorSummary) {
        Map<String, Object> result = new HashMap();
        result.put(":host", executorSummary.get_host());
        result.put(":port", executorSummary.get_port());
        result.put(":uptime_secs", executorSummary.get_uptime_secs());
        result.put(":transferred", null);
        if (executorSummary.is_set_stats()) {
            result.put(":transferred", sanitizeTransferredStats(executorSummary.get_stats().get_transferred()));
        }
        return result;
    }



    /**
     * getInputMap.
     * @param entryInput entryInput
     * @return getInputMap
     */
    public static Map<String, Object> getInputMap(Map.Entry<GlobalStreamId, Grouping> entryInput) {
        Map<String, Object> result = new HashMap();
        result.put(":component", entryInput.getKey().get_componentId());
        result.put(":stream", entryInput.getKey().get_streamId());
        result.put(":sani-stream", sanitizeStreamName(entryInput.getKey().get_streamId()));
        result.put(":grouping", entryInput.getValue().getSetField().getFieldName());
        return result;
    }

    /**
     * getVisualizationData.
     * @param client client
     * @param window window
     * @param topoId topoId
     * @param sys sys
     * @return getVisualizationData
     * @throws TException TException
     */
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
                spoutData.put(":type", "spout");
                spoutData.put(":capacity", 0);
                Map<String, Map> spoutStreamsStats =
                    StatsUtil.spoutStreamsStats(spoutSummaries.get(spoutComponentId), sys);
                spoutData.put(":latency", spoutStreamsStats.get("complete-latencies").get(window));
                spoutData.put(":transferred", spoutStreamsStats.get("transferred").get(window));
                spoutData.put(":stats", spoutSummaries.get(
                        spoutComponentId).stream().map(
                                UIHelpers::getStatMapFromExecutorSummary).collect(Collectors.toList()));
                spoutData.put(
                        ":link",
                        UIHelpers.urlFormat("/component.html?id=%s&topology_id=%s", spoutComponentId, topoId)
                );

                spoutData.put(":inputs",
                    spoutSpecMapEntry.getValue().get_common().get_inputs().entrySet().stream().map(
                            UIHelpers::getInputMap).collect(Collectors.toList())
                );
                result.put(spoutComponentId, spoutData);
            }
        }

        for (Map.Entry<String, Bolt> boltEntry : boltSpecs.entrySet()) {
            String boltComponentId = boltEntry.getKey();
            if (boltSummaries.containsKey(boltComponentId) && (sys || !Utils.isSystemId(boltComponentId))) {
                Map<String, Object> boltMap = new HashMap();
                boltMap.put(":type", "bolt");
                boltMap.put(":capacity", StatsUtil.computeBoltCapacity(boltSummaries.get(boltComponentId)));
                Map<String, Map> boltStreamsStats =
                        StatsUtil.boltStreamsStats(boltSummaries.get(boltComponentId), sys);
                boltMap.put(":latency", boltStreamsStats.get("process-latencies").get(window));
                boltMap.put(":transferred", boltStreamsStats.get("transferred").get(window));
                boltMap.put(":stats", boltSummaries.get(
                        boltComponentId).stream().map(
                        UIHelpers::getStatMapFromExecutorSummary).collect(Collectors.toList()));
                boltMap.put(
                        ":link",
                        UIHelpers.urlFormat("/component.html?id=%s&topology_id=%s", boltComponentId, topoId)
                );

                boltMap.put(":inputs",
                        boltEntry.getValue().get_common().get_inputs().entrySet().stream().map(
                                UIHelpers::getInputMap).collect(Collectors.toList())
                );
                result.put(boltComponentId, boltMap);
            }
        }
        return result;
    }

    /**
     * getStreamBox.
     * @param visualization visualization
     * @return getStreamBox
     */
    public static Map<String, Object> getStreamBox(Object visualization) {
        Map<String, Object> visualizationData = (Map<String, Object>) visualization;
        Map<String, Object> result = new HashMap();
        Map<String, Object> temp = (Map<String, Object>) visualizationData.get("inputs");
        result.put("stream", temp.get("stream"));
        result.put("sani-stream", temp.get("sani-stream"));
        result.put("checked", !Utils.isSystemId((String) temp.get("stream")));
        return result;
    }

    /**
     * getBuildVisualization.
     * @param client client
     * @param config config
     * @param window window
     * @param id id
     * @param sys sys
     * @return getBuildVisualization
     * @throws TException TException
     */
    public static Map<String, Object> getBuildVisualization(
            Nimbus.Iface client, Map<String, Object> config, String window, String id, boolean sys)
            throws TException {
        Map<String, Object> result = new HashMap();
        Map<String, Object> visualizationData = getVisualizationData(client, window, id, sys);
        List<Map> streamBoxes = visualizationData.entrySet().stream().map(UIHelpers::getStreamBox).collect(Collectors.toList());
        result.put("visualizationTable", Lists.partition(streamBoxes, 4));
        return result;
    }

    /**
     * getActiveAction.
     * @param profileRequest profileRequest
     * @param config config
     * @param topologyId topologyId
     * @return getActiveAction
     */
    public static Map<String, Object> getActiveAction(ProfileRequest profileRequest, Map config, String topologyId) {
        Map<String, Object> result = new HashMap();
        result.put("host", profileRequest.get_nodeInfo().get_node());
        result.put("port", String.valueOf(profileRequest.get_nodeInfo().get_port().toArray()[0]));
        result.put("dumplink",
                getWorkerDumpLink(
                        profileRequest.get_nodeInfo().get_node(),
                        (Long) profileRequest.get_nodeInfo().get_port().toArray()[0], topologyId, config
                        ));
        result.put("timestamp", System.currentTimeMillis() - profileRequest.get_time_stamp());
        return result;
    }

    /**
     * getActiveProfileActions.
     * @param client client
     * @param id id
     * @param component component
     * @param config config
     * @return getActiveProfileActions
     * @throws TException TException
     */
    public static List getActiveProfileActions(Nimbus.Iface client, String id, String component, Map config) throws TException {
        List<ProfileRequest> profileRequests =
                client.getComponentPendingProfileActions(id, component, ProfileAction.JPROFILE_STOP);
        return profileRequests.stream().map(x -> UIHelpers.getActiveAction(x, config, id)).collect(Collectors.toList());
    }

    /**
     * getWorkerDumpLink.
     *
     * @param host host
     * @param port port
     * @param topologyId topologyId
     * @param config config
     * @return getWorkerDumpLink
     */
    public static String getWorkerDumpLink(String host, long port, String topologyId, Map<String, Object> config) {
        if (isSecureLogviewer(config)) {
            return UIHelpers.urlFormat(
                "https://%s:%s/api/v1/dumps/%s/%s",
                Utils.urlEncodeUtf8(host), config.get(DaemonConfig.LOGVIEWER_HTTPS_PORT),
                Utils.urlEncodeUtf8(topologyId),
                Utils.urlEncodeUtf8(host)
                + ":" + Utils.urlEncodeUtf8(String.valueOf(port))
            );
        } else {
            return UIHelpers.urlFormat(
                "http://%s:%s/api/v1/dumps/%s/%s",
                Utils.urlEncodeUtf8(host), config.get(DaemonConfig.LOGVIEWER_PORT),
                Utils.urlEncodeUtf8(topologyId),
                Utils.urlEncodeUtf8(host) + ":" + Utils.urlEncodeUtf8(String.valueOf(port))
            );
        }
    }

    /**
     * unpackBoltPageInfo.
     * @param componentPageInfo componentPageInfo
     * @param topologyId topologyId
     * @param window window
     * @param sys sys
     * @param config config
     * @return unpackBoltPageInfo
     */
    public static Map<String, Object> unpackBoltPageInfo(ComponentPageInfo componentPageInfo,
                                                         String topologyId, String window, boolean sys,
                                                         Map config) {
        Map<String, Object> result = new HashMap<>();

        result.put(
            "boltStats",
            componentPageInfo.get_window_to_stats().entrySet().stream().map(
                e -> getBoltAggStatsMap(e.getValue(), e.getKey())
            ).collect(Collectors.toList())
        );
        result.put(
            "inputStats",
            componentPageInfo.get_gsid_to_input_stats().entrySet().stream().map(
                e -> getBoltInputStats(e.getKey(), e.getValue())
            ).collect(Collectors.toList())
        );
        result.put(
            "outputStats",
            componentPageInfo.get_sid_to_output_stats().entrySet().stream().map(
                e -> getBoltOutputStats(e.getKey(), e.getValue())
            ).collect(Collectors.toList())
        );
        result.put(
            "executorStats",
            componentPageInfo.get_exec_stats().stream().map(
                e -> getBoltExecutorStats(topologyId, config, e)
            ).collect(Collectors.toList())
        );
        result.putAll(getComponentErrors(componentPageInfo.get_errors(), topologyId, config));
        return result;
    }

    /**
     * unpackSpoutPageInfo.
     * @param componentPageInfo componentPageInfo
     * @param topologyId topologyId
     * @param window window
     * @param sys sys
     * @param config config
     * @return unpackSpoutPageInfo
     */
    public static Map<String, Object> unpackSpoutPageInfo(ComponentPageInfo componentPageInfo,
                                                          String topologyId, String window, boolean sys,
                                                          Map config) {
        Map<String, Object> result = new HashMap<>();
        result.put(
            "spoutSummary",
            componentPageInfo.get_window_to_stats().entrySet().stream().map(
                e -> getSpoutAggStatsMap(e.getValue(), e.getKey())
            ).collect(Collectors.toList())
        );
        result.put(
            "outputStats",
            componentPageInfo.get_sid_to_output_stats().entrySet().stream().map(
                e -> getSpoutOutputStats(e.getKey(), e.getValue())
            ).collect(Collectors.toList())
        );
        result.put(
            "executorStats",
            componentPageInfo.get_exec_stats().stream().map(
                e -> getSpoutExecutorStats(topologyId, config, e)
            ).collect(Collectors.toList())
        );
        result.putAll(getComponentErrors(componentPageInfo.get_errors(), topologyId, config));
        return result;
    }

    /**
     * getComponentPage.
     * @param client client
     * @param id id
     * @param component component
     * @param window window
     * @param sys sys
     * @param user user
     * @param config config
     * @return getComponentPage
     * @throws TException TException
     */
    public static Map<String, Object> getComponentPage(
            Nimbus.Iface client, String id, String component,
            String window, boolean sys, String user, Map config) throws TException {
        Map<String, Object> result = new HashMap();
        ComponentPageInfo componentPageInfo = client.getComponentPageInfo(
                id, component, window, sys
        );

        if (componentPageInfo.get_component_type().equals(ComponentType.BOLT)) {
            result.putAll(unpackBoltPageInfo(componentPageInfo, id, window, sys, config));
        } else if ((componentPageInfo.get_component_type().equals(ComponentType.SPOUT))) {
            result.putAll(unpackSpoutPageInfo(componentPageInfo, id, window, sys, config));
        }

        result.put("user", user);
        result.put("id" , component);
        result.put("encodedId", Utils.urlEncodeUtf8(component));
        result.put("name", componentPageInfo.get_topology_name());
        result.put("executors", componentPageInfo.get_num_executors());
        result.put("tasks", componentPageInfo.get_num_tasks());
        result.put("requestedMemOnHeap",
                componentPageInfo.get_resources_map().get(Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME));
        result.put("requestedMemOffHeap",
                componentPageInfo.get_resources_map().get(Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME));
        result.put("requestedCpu",
                componentPageInfo.get_resources_map().get(Constants.COMMON_CPU_RESOURCE_NAME));
        result.put("requestedGenericResources",
                prettifyGenericResources(componentPageInfo.get_resources_map()));

        result.put("schedulerDisplayResource", config.get(DaemonConfig.SCHEDULER_DISPLAY_RESOURCE));
        result.put("topologyId", id);
        result.put("topologyStatus", componentPageInfo.get_topology_status());
        result.put("encodedTopologyId", Utils.urlEncodeUtf8(id));
        result.put("window", window);
        result.put("componentType", componentPageInfo.get_component_type().toString().toLowerCase());
        result.put("windowHint", getWindowHint(window));
        result.put("debug", componentPageInfo.is_set_debug_options() && componentPageInfo.get_debug_options().is_enable());
        double samplingPct = 10;
        if (componentPageInfo.is_set_debug_options()) {
            samplingPct = componentPageInfo.get_debug_options().get_samplingpct();
        }
        result.put("samplingPct", samplingPct);
        String eventlogHost = componentPageInfo.get_eventlog_host();
        if (null != eventlogHost && !eventlogHost.isEmpty()) {
            result.put("eventLogLink", getLogviewerLink(eventlogHost,
                    WebAppUtils.eventLogsFilename(id, String.valueOf(componentPageInfo.get_eventlog_port())),
                    config, componentPageInfo.get_eventlog_port()));
        }
        result.put("profilingAndDebuggingCapable", !Utils.isOnWindows());
        result.put("profileActionEnabled", config.get(DaemonConfig.WORKER_PROFILER_ENABLED));


        result.put("profilerActive", getActiveProfileActions(client, id, component, config));
        return result;
    }

    /**
     * getTopolgoyLogConfig.
     * @param logConfig logConfig
     * @return getTopolgoyLogConfig
     */
    public static Map<String, Object> getTopolgoyLogConfig(LogConfig logConfig) {
        Map<String, Object> result = new HashMap();
        if (logConfig.is_set_named_logger_level()) {
            for (Map.Entry<String, LogLevel> entry : logConfig.get_named_logger_level().entrySet()) {
                Map temp = new HashMap();
                temp.put("target_level", entry.getValue().get_target_log_level());
                temp.put("reset_level", entry.getValue().get_reset_log_level());
                temp.put("timeout", entry.getValue().get_reset_log_level_timeout_secs());
                temp.put("timeout_epoch", entry.getValue().get_reset_log_level_timeout_epoch());
                result.put(entry.getKey(), temp);
            }
        }
        Map finalResult = new HashMap();
        finalResult.put("namedLoggerLevels", result);
        return finalResult;
    }

    /**
     * getTopologyOpResponse.
     * @param id id
     * @param op op
     * @return getTopologyOpResponse
     */
    public static Map<String, Object> getTopologyOpResponse(String id, String op) {
        Map<String, Object> result = new HashMap();
        result.put("topologyOperation", op);
        result.put("topologyId", id);
        result.put("status", "success");
        return result;
    }

    /**
     * putTopologyActivate.
     * @param client client
     * @param id id
     * @return putTopologyActivate
     * @throws TException TException
     */
    public static Map<String, Object> putTopologyActivate(Nimbus.Iface client, String id) throws TException {
        GetInfoOptions getInfoOptions = new GetInfoOptions();
        getInfoOptions.set_num_err_choice(NumErrorsChoice.NONE);
        TopologyInfo topologyInfo = client.getTopologyInfoWithOpts(id, getInfoOptions);
        client.activate(topologyInfo.get_name());
        return getTopologyOpResponse(id, "activate");
    }

    /**
     * putTopologyDeactivate.
     * @param client client
     * @param id id
     * @return putTopologyDeactivate
     * @throws TException TException
     */
    public static Map<String, Object> putTopologyDeactivate(Nimbus.Iface client, String id) throws TException {
        GetInfoOptions getInfoOptions = new GetInfoOptions();
        getInfoOptions.set_num_err_choice(NumErrorsChoice.NONE);
        TopologyInfo topologyInfo = client.getTopologyInfoWithOpts(id, getInfoOptions);
        client.deactivate(topologyInfo.get_name());
        return getTopologyOpResponse(id, "deactivate");
    }

    /**
     * putTopologyDebugActionSpct.
     * @param client client
     * @param id id
     * @param action action
     * @param spct spct
     * @param component component
     * @return putTopologyDebugActionSpct
     * @throws TException TException
     */
    public static Map<String, Object> putTopologyDebugActionSpct(
            Nimbus.Iface client, String id, String action, String spct, String component) throws TException {
        GetInfoOptions getInfoOptions = new GetInfoOptions();
        getInfoOptions.set_num_err_choice(NumErrorsChoice.NONE);
        TopologyInfo topologyInfo = client.getTopologyInfoWithOpts(id, getInfoOptions);
        client.debug(topologyInfo.get_name(), component, action.equals("enable"), Integer.parseInt(spct));
        return getTopologyOpResponse(id, "debug/" + action);
    }

    /**
     * putTopologyRebalance.
     * @param client client
     * @param id id
     * @param waitTime waitTime
     * @return putTopologyRebalance
     * @throws TException TException
     */
    public static Map<String, Object> putTopologyRebalance(
            Nimbus.Iface client, String id, String waitTime) throws TException {
        GetInfoOptions getInfoOptions = new GetInfoOptions();
        getInfoOptions.set_num_err_choice(NumErrorsChoice.NONE);
        TopologyInfo topologyInfo = client.getTopologyInfoWithOpts(id, getInfoOptions);
        RebalanceOptions rebalanceOptions = new RebalanceOptions();
        rebalanceOptions.set_wait_secs(Integer.parseInt(waitTime));
        client.rebalance(topologyInfo.get_name(), rebalanceOptions);
        return getTopologyOpResponse(id, "rebalance");
    }

    /**
     * putTopologyKill.
     * @param client client
     * @param id id
     * @param waitTime waitTime
     * @return putTopologyKill
     * @throws TException TException
     */
    public static Map<String, Object> putTopologyKill(Nimbus.Iface client, String id, String waitTime) throws TException {
        GetInfoOptions getInfoOptions = new GetInfoOptions();
        getInfoOptions.set_num_err_choice(NumErrorsChoice.NONE);
        TopologyInfo topologyInfo = client.getTopologyInfoWithOpts(id, getInfoOptions);
        KillOptions killOptions = new KillOptions();
        killOptions.set_wait_secs(Integer.parseInt(waitTime));
        client.killTopologyWithOpts(topologyInfo.get_name(), killOptions);
        return getTopologyOpResponse(id, "kill");
    }

    /**
     * setTopologyProfilingAction.
     * @param client client
     * @param id id
     * @param hostPort hostPort
     * @param timestamp timestamp
     * @param config config
     * @param profileAction profileAction
     * @throws TException TException
     */
    public static void setTopologyProfilingAction(
            Nimbus.Iface client, String id,
            String hostPort, Long timestamp, Map<String,
            Object> config, ProfileAction profileAction) throws TException {
        String host = hostPort.split(":")[0];
        Set<Long> ports = new HashSet();
        String port = hostPort.split(":")[1];
        ports.add(Long.valueOf(port));
        NodeInfo nodeInfo = new NodeInfo(host, ports);
        ProfileRequest profileRequest = new ProfileRequest(nodeInfo, profileAction);
        profileRequest.set_time_stamp(timestamp);
        client.setWorkerProfiler(id, profileRequest);
    }

    /**
     * getTopologyProfilingStart.
     * @param client client
     * @param id id
     * @param hostPort hostPort
     * @param timeout timeout
     * @param config config
     * @return getTopologyProfilingStart
     * @throws TException TException
     */
    public static Map<String, Object> getTopologyProfilingStart(Nimbus.Iface client, String id,
                                                                String hostPort, String timeout,
                                                                Map<String, Object> config) throws TException {
        setTopologyProfilingAction(
                client, id , hostPort, System.currentTimeMillis() + (Long.valueOf(timeout) * 60_000),
                config, ProfileAction.JPROFILE_STOP);
        Map<String, Object> result = new HashMap();
        String host = hostPort.split(":")[0];
        String port = hostPort.split(":")[1];
        result.put("status", "ok");
        result.put("id", hostPort);
        result.put("timeout", timeout);
        result.put("dumplink", getWorkerDumpLink(host, Long.valueOf(port), id, config));
        return result;
    }

    /**
     * getTopologyProfilingStop.
     * @param client client
     * @param id id
     * @param hostPort hostPort
     * @param config config
     * @return getTopologyProfilingStop
     * @throws TException TException
     */
    public static Map<String, Object> getTopologyProfilingStop(Nimbus.Iface client, String id,
                                                               String hostPort,
                                                               Map<String, Object> config) throws TException {
        setTopologyProfilingAction(client, id , hostPort, 0L, config, ProfileAction.JPROFILE_STOP);
        Map<String, Object> result = new HashMap();
        result.put("status", "ok");
        result.put("id", hostPort);
        return result;
    }

    /**
     * getProfilingDisabled.
     * @return getProfilingDisabled
     */
    public static Map<String, Object> getProfilingDisabled() {
        Map<String, Object> result = new HashMap();
        result.put("status", "disabled");
        result.put("message", "Profiling is not enabled on this server");
        return result;
    }

    /**
     * getTopologyProfilingDump.
     * @param client client
     * @param id id
     * @param hostPort hostPort
     * @param config config
     * @return getTopologyProfilingDump
     * @throws TException TException
     */
    public static Map<String, Object> getTopologyProfilingDump(Nimbus.Iface client, String id, String hostPort,
                                                               Map<String, Object> config) throws TException {
        setTopologyProfilingAction(
                client, id , hostPort, System.currentTimeMillis(),
                config, ProfileAction.JPROFILE_DUMP
        );
        Map<String, Object> result = new HashMap();
        result.put("status", "ok");
        result.put("id", hostPort);
        return result;
    }

    public static Map<String, Object> getTopologyProfilingDumpJstack(Nimbus.Iface client, String id,
                                                                     String hostPort, Map<String,
                                                                     Object> config) throws TException {
        setTopologyProfilingAction(
                client, id , hostPort, System.currentTimeMillis(), config, ProfileAction.JSTACK_DUMP
        );
        Map<String, Object> result = new HashMap();
        result.put("status", "ok");
        result.put("id", hostPort);
        return result;
    }

    /**
     * getTopologyProfilingRestartWorker.
     * @param client client
     * @param id id
     * @param hostPort hostPort
     * @param config config
     * @return getTopologyProfilingRestartWorker
     * @throws TException TException
     */
    public static Map<String, Object> getTopologyProfilingRestartWorker(Nimbus.Iface client,
                                                                        String id, String hostPort,
                                                                        Map<String, Object> config) throws TException {
        setTopologyProfilingAction(
                client, id , hostPort, System.currentTimeMillis(), config, ProfileAction.JVM_RESTART
        );
        Map<String, Object> result = new HashMap();
        result.put("status", "ok");
        result.put("id", hostPort);
        return result;
    }

    /**
     * getTopologyProfilingDumpHeap.
     * @param client client
     * @param id id
     * @param hostPort hostport
     * @param config config
     * @return getTopologyProfilingDumpHeap
     * @throws TException TException
     */
    public static Map<String, Object> getTopologyProfilingDumpHeap(Nimbus.Iface client, String id, String hostPort,
                                                                   Map<String, Object> config) throws TException {
        setTopologyProfilingAction(client, id , hostPort, System.currentTimeMillis(), config, ProfileAction.JMAP_DUMP);
        Map<String, Object> result = new HashMap();
        result.put("status", "ok");
        result.put("id", hostPort);
        return result;
    }

    /**
     * putTopologyLogLevel.
     * @param client client
     * @param namedLogLevel namedLogLevel
     * @param id id
     * @return putTopologyLogLevel.
     * @throws TException TException
     */
    public static Map<String, Object> putTopologyLogLevel(Nimbus.Iface client,
                                                          Map<String, Map> namedLogLevel, String id) throws TException {
        Map<String, Map> namedLoggerlevels = namedLogLevel;
        for (Map.Entry<String, Map> entry : namedLoggerlevels.entrySet()) {
            String loggerNMame = entry.getKey();
            String targetLevel = (String) entry.getValue().get("target_level");
            Long timeout = (Long) entry.getValue().get("timeout");
            LogLevel logLevel = new LogLevel();
            if (targetLevel == null) {
                logLevel.set_action(LogLevelAction.REMOVE);
                logLevel.unset_target_log_level();
            } else {
                logLevel.set_action(LogLevelAction.UPDATE);
                logLevel.set_target_log_level(org.apache.logging.log4j.Level.toLevel(targetLevel).name());
                logLevel.set_reset_log_level_timeout_secs(Math.toIntExact(timeout));
            }
            LogConfig logConfig = new LogConfig();
            logConfig.put_to_named_logger_level(loggerNMame, logLevel);
            client.setLogConfig(id, logConfig);
        }
        return UIHelpers.getTopolgoyLogConfig(client.getLogConfig(id));
    }

    /**
     * getNimbusSummary.
     * @param clusterInfo clusterInfo
     * @param config config
     * @return getNimbusSummary
     */
    public static Map<String, Object> getNimbusSummary(ClusterSummary clusterInfo, Map<String, Object> config) {
        List<NimbusSummary> nimbusSummaries = clusterInfo.get_nimbuses();
        List<String> nimbusSeeds = new ArrayList();
        for (String nimbusHost : (List<String>) config.get(Config.NIMBUS_SEEDS)) {
            if (!Utils.isLocalhostAddress(nimbusHost)) {
                nimbusSeeds.add(nimbusHost + ":" + config.get(Config.NIMBUS_THRIFT_PORT));
            }
        }

        List<Map> resultSummaryList = new ArrayList();

        for (NimbusSummary nimbusSummary : nimbusSummaries) {
            Map<String, Object> nimbusSummaryMap = new HashMap();
            nimbusSummaryMap.put("host", nimbusSummary.get_host());
            nimbusSummaryMap.put("port", nimbusSummary.get_port());
            String status = "Not a Leader";
            if (nimbusSummary.is_isLeader()) {
                status = "Leader";
            }
            nimbusSummaryMap.put("status", status);
            nimbusSummaryMap.put("version", nimbusSummary.get_version());
            nimbusSummaryMap.put("nimbusUpTimeSeconds", nimbusSummary.get_uptime_secs());
            nimbusSummaryMap.put("nimbusUpTime", prettyUptimeSec(nimbusSummary.get_uptime_secs()));
            nimbusSummaryMap.put("nimbusLogLink", getNimbusLogLink(nimbusSummary.get_host(), config));
            resultSummaryList.add(nimbusSummaryMap);
            nimbusSeeds.remove(nimbusSummary.get_host() + ":" + String.valueOf(nimbusSummary.get_port()));
        }

        for (String nimbusSeed : nimbusSeeds) {
            Map<String, Object> nimbusSummaryMap = new HashMap();
            nimbusSummaryMap.put("host", nimbusSeed.split(":")[0]);
            nimbusSummaryMap.put("port", nimbusSeed.split(":")[1]);
            nimbusSummaryMap.put("status", "Offline");
            nimbusSummaryMap.put("version", "Not applicable");
            nimbusSummaryMap.put("nimbusUpTimeSeconds", "Not applicable");
            nimbusSummaryMap.put("nimbusUpTime", "Not applicable");
            nimbusSummaryMap.put("nimbusLogLink", getNimbusLogLink(nimbusSeed.split(":")[0], config));
            resultSummaryList.add(nimbusSummaryMap);
        }
        Map<String, Object> result = new HashMap();
        result.put("nimbuses", resultSummaryList);
        return result;
    }
}
