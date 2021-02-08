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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.daemon.common.JsonResponseBuilder;
import org.apache.storm.daemon.logviewer.handler.LogviewerLogDownloadHandler;
import org.apache.storm.daemon.logviewer.handler.LogviewerLogPageHandler;
import org.apache.storm.daemon.logviewer.handler.LogviewerLogSearchHandler;
import org.apache.storm.daemon.logviewer.handler.LogviewerProfileHandler;
import org.apache.storm.daemon.logviewer.utils.ExceptionMeterNames;
import org.apache.storm.daemon.ui.InvalidRequestException;
import org.apache.storm.daemon.ui.UIHelpers;
import org.apache.storm.daemon.ui.resources.StormApiResource;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.IHttpCredentialsPlugin;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles HTTP requests for Logviewer.
 */
@Path("/")
public class LogviewerResource {
    private static final Logger LOG = LoggerFactory.getLogger(LogviewerResource.class);

    private final Meter meterLogPageHttpRequests;
    private final Meter meterDaemonLogPageHttpRequests;
    private final Meter meterDownloadLogFileHttpRequests;
    private final Meter meterDownloadLogDaemonFileHttpRequests;
    private final Meter meterListLogsHttpRequests;
    private final Meter numSearchLogRequests;
    private final Meter numDeepSearchArchived;
    private final Meter numDeepSearchNonArchived;
    private final Meter numReadLogExceptions;
    private final Meter numReadDaemonLogExceptions;
    private final Meter numListLogExceptions;
    private final Meter numListDumpExceptions;
    private final Meter numDownloadDumpExceptions;
    private final Meter numDownloadLogExceptions;
    private final Meter numDownloadDaemonLogExceptions;
    private final Meter numSearchExceptions;
    private final Timer searchLogRequestDuration;
    private final Timer deepSearchRequestDuration;

    private final LogviewerLogPageHandler logviewer;
    private final LogviewerProfileHandler profileHandler;
    private final LogviewerLogDownloadHandler logDownloadHandler;
    private final LogviewerLogSearchHandler logSearchHandler;
    private final IHttpCredentialsPlugin httpCredsHandler;

    /**
     * Constructor.
     *
     * @param logviewerParam {@link LogviewerLogPageHandler}
     * @param profileHandler {@link LogviewerProfileHandler}
     * @param logDownloadHandler {@link LogviewerLogDownloadHandler}
     * @param logSearchHandler {@link LogviewerLogSearchHandler}
     * @param httpCredsHandler {@link IHttpCredentialsPlugin}
     * @param metricsRegistry The metrics registry
     */
    public LogviewerResource(LogviewerLogPageHandler logviewerParam, LogviewerProfileHandler profileHandler,
                             LogviewerLogDownloadHandler logDownloadHandler, LogviewerLogSearchHandler logSearchHandler,
                             IHttpCredentialsPlugin httpCredsHandler, StormMetricsRegistry metricsRegistry) {
        this.meterLogPageHttpRequests = metricsRegistry.registerMeter("logviewer:num-log-page-http-requests");
        this.meterDaemonLogPageHttpRequests = metricsRegistry.registerMeter(
            "logviewer:num-daemonlog-page-http-requests");
        this.meterDownloadLogFileHttpRequests = metricsRegistry.registerMeter(
            "logviewer:num-download-log-file-http-requests");
        this.meterDownloadLogDaemonFileHttpRequests = metricsRegistry.registerMeter(
            "logviewer:num-download-log-daemon-file-http-requests");
        this.meterListLogsHttpRequests = metricsRegistry.registerMeter("logviewer:num-list-logs-http-requests");
        this.numSearchLogRequests = metricsRegistry.registerMeter("logviewer:num-search-logs-requests");
        this.numDeepSearchArchived = metricsRegistry.registerMeter("logviewer:num-deep-search-requests-with-archived");
        this.numDeepSearchNonArchived = metricsRegistry.registerMeter("logviewer:num-deep-search-requests-without-archived");
        this.numReadLogExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_READ_LOG_EXCEPTIONS);
        this.numReadDaemonLogExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_READ_DAEMON_LOG_EXCEPTIONS);
        this.numListLogExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_LIST_LOG_EXCEPTIONS);
        this.numListDumpExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_LIST_DUMP_EXCEPTIONS);
        this.numDownloadDumpExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_DOWNLOAD_DUMP_EXCEPTIONS);
        this.numDownloadLogExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_DOWNLOAD_LOG_EXCEPTIONS);
        this.numDownloadDaemonLogExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_DOWNLOAD_DAEMON_LOG_EXCEPTIONS);
        this.numSearchExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_SEARCH_EXCEPTIONS);
        this.searchLogRequestDuration = metricsRegistry.registerTimer("logviewer:search-requests-duration-ms");
        this.deepSearchRequestDuration = metricsRegistry.registerTimer("logviewer:deep-search-request-duration-ms");
        this.logviewer = logviewerParam;
        this.profileHandler = profileHandler;
        this.logDownloadHandler = logDownloadHandler;
        this.logSearchHandler = logSearchHandler;
        this.httpCredsHandler = httpCredsHandler;
    }

    /**
     * Handles '/log' request.
     */
    @GET
    @Path("/log")
    public Response log(@Context HttpServletRequest request) throws IOException {
        meterLogPageHttpRequests.mark();

        try {
            String user = httpCredsHandler.getUserName(request);
            Integer start = request.getParameter("start") != null ? parseIntegerFromMap(request.getParameterMap(), "start") : null;
            Integer length = request.getParameter("length") != null ? parseIntegerFromMap(request.getParameterMap(), "length") : null;
            String decodedFileName = Utils.urlDecodeUtf8(request.getParameter("file"));
            String grep = request.getParameter("grep");
            return logviewer.logPage(decodedFileName, start, length, grep, user);
        } catch (InvalidRequestException e) {
            LOG.error(e.getMessage(), e);
            return Response.status(400).entity(e.getMessage()).build();
        } catch (IOException e) {
            numReadLogExceptions.mark();
            throw e;
        }
    }

    /**
     * Handles '/daemonlog' request.
     */
    @GET
    @Path("/daemonlog")
    public Response daemonLog(@Context HttpServletRequest request) throws IOException {
        meterDaemonLogPageHttpRequests.mark();

        try {
            String user = httpCredsHandler.getUserName(request);
            Integer start = request.getParameter("start") != null ? parseIntegerFromMap(request.getParameterMap(), "start") : null;
            Integer length = request.getParameter("length") != null ? parseIntegerFromMap(request.getParameterMap(), "length") : null;
            String decodedFileName = Utils.urlDecodeUtf8(request.getParameter("file"));
            String grep = request.getParameter("grep");
            return logviewer.daemonLogPage(decodedFileName, start, length, grep, user);
        } catch (InvalidRequestException e) {
            LOG.error(e.getMessage(), e);
            return Response.status(400).entity(e.getMessage()).build();
        } catch (IOException e) {
            numReadDaemonLogExceptions.mark();
            throw e;
        }
    }

    /**
     * Handles '/searchLogs' request.
     */
    @GET
    @Path("/searchLogs")
    public Response searchLogs(@Context HttpServletRequest request) throws IOException {
        String user = httpCredsHandler.getUserName(request);
        String topologyId = request.getParameter("topoId");
        String portStr = request.getParameter("port");
        String callback = request.getParameter("callbackParameterName");
        String origin = request.getHeader("Origin");

        return logviewer.listLogFiles(user, portStr != null ? Integer.parseInt(portStr) : null, topologyId, callback, origin);
    }

    /**
     * Handles '/listLogs' request.
     */
    @GET
    @Path("/listLogs")
    public Response listLogs(@Context HttpServletRequest request) throws IOException {
        meterListLogsHttpRequests.mark();

        String user = httpCredsHandler.getUserName(request);
        String topologyId = request.getParameter("topoId");
        String portStr = request.getParameter("port");
        String callback = request.getParameter(StormApiResource.callbackParameterName);
        String origin = request.getHeader("Origin");

        try {
            return logviewer.listLogFiles(user, portStr != null ? Integer.parseInt(portStr) : null, topologyId, callback, origin);
        } catch (IOException e) {
            numListLogExceptions.mark();
            throw e;
        }
    }

    /**
     * Handles '/dumps' (listing dump files) request.
     */
    @GET
    @Path("/dumps/{topo-id}/{host-port}")
    public Response listDumpFiles(@PathParam("topo-id") String topologyId, @PathParam("host-port") String hostPort,
                                  @Context HttpServletRequest request) throws IOException {
        String user = httpCredsHandler.getUserName(request);
        try {
            return profileHandler.listDumpFiles(topologyId, hostPort, user);
        } catch (IOException e) {
            numListDumpExceptions.mark();
            throw e;
        }
    }

    /**
     * Handles '/dumps' (downloading specific dump file) request.
     */
    @GET
    @Path("/dumps/{topo-id}/{host-port}/{filename}")
    public Response downloadDumpFile(@PathParam("topo-id") String topologyId, @PathParam("host-port") String hostPort,
                                     @PathParam("filename") String fileName, @Context HttpServletRequest request) throws IOException {

        String user = httpCredsHandler.getUserName(request);
        try {
            return profileHandler.downloadDumpFile(topologyId, hostPort, fileName, user);
        } catch (IOException e) {
            numDownloadDumpExceptions.mark();
            throw e;
        }
    }

    /**
     * Handles '/download' (downloading specific log file) request.
     */
    @GET
    @Path("/download")
    public Response downloadLogFile(@Context HttpServletRequest request) throws IOException {
        meterDownloadLogFileHttpRequests.mark();
        String user = httpCredsHandler.getUserName(request);
        String file = request.getParameter("file");
        String decodedFileName = Utils.urlDecodeUtf8(file);
        try {
            String host = Utils.hostname();
            return logDownloadHandler.downloadLogFile(host, decodedFileName, user);
        } catch (IOException e) {
            numDownloadLogExceptions.mark();
            throw e;
        }
    }

    /**
     * Handles '/daemondownload' (downloading specific daemon log file) request.
     */
    @GET
    @Path("/daemondownload")
    public Response downloadDaemonLogFile(@Context HttpServletRequest request) throws IOException {
        meterDownloadLogDaemonFileHttpRequests.mark();
        String user = httpCredsHandler.getUserName(request);
        String file = request.getParameter("file");
        String decodedFileName = Utils.urlDecodeUtf8(file);
        try {
            String host = Utils.hostname();
            return logDownloadHandler.downloadDaemonLogFile(host, decodedFileName, user);
        } catch (IOException e) {
            numDownloadDaemonLogExceptions.mark();
            throw e;
        }
    }

    /**
     * Handles '/search' (searching from specific worker or daemon log file) request.
     */
    @GET
    @Path("/search")
    public Response search(@Context HttpServletRequest request) throws IOException {
        numSearchLogRequests.mark();

        String user = httpCredsHandler.getUserName(request);
        boolean isDaemon = StringUtils.equals(request.getParameter("is-daemon"), "yes");
        String file = request.getParameter("file");
        String decodedFileName = Utils.urlDecodeUtf8(file);
        String searchString = request.getParameter("search-string");
        String numMatchesStr = request.getParameter("num-matches");
        String startByteOffset = request.getParameter("start-byte-offset");
        String callback = request.getParameter(StormApiResource.callbackParameterName);
        String origin = request.getHeader("Origin");

        try (Timer.Context t = searchLogRequestDuration.time()) {
            return logSearchHandler.searchLogFile(decodedFileName, user, isDaemon,
                searchString, numMatchesStr, startByteOffset, callback, origin);
        } catch (InvalidRequestException e) {
            LOG.error(e.getMessage(), e);
            int statusCode = 400;
            return new JsonResponseBuilder().setData(UIHelpers.exceptionToJson(e, statusCode)).setCallback(callback)
                .setStatus(statusCode).build();
        } catch (IOException e) {
            numSearchExceptions.mark();
            throw e;
        }
    }

    /**
     * Handles '/deepSearch' request.
     */
    @GET
    @Path("/deepSearch/{topoId}")
    public Response deepSearch(@PathParam("topoId") String topologyId,
                               @Context HttpServletRequest request) throws IOException {
        String user = httpCredsHandler.getUserName(request);
        String searchString = request.getParameter("search-string");
        String numMatchesStr = request.getParameter("num-matches");
        String portStr = request.getParameter("port");
        String startFileOffset = request.getParameter("start-file-offset");
        String startByteOffset = request.getParameter("start-byte-offset");
        String searchArchived = request.getParameter("search-archived");
        String callback = request.getParameter(StormApiResource.callbackParameterName);
        String origin = request.getHeader("Origin");

        Boolean alsoSearchArchived = BooleanUtils.toBooleanObject(searchArchived);
        if (BooleanUtils.isTrue(alsoSearchArchived)) {
            numDeepSearchArchived.mark();
        } else {
            numDeepSearchNonArchived.mark();
        }
        try (Timer.Context t = deepSearchRequestDuration.time()) {
            return logSearchHandler.deepSearchLogsForTopology(topologyId, user, searchString, numMatchesStr, portStr, startFileOffset,
                startByteOffset, alsoSearchArchived, callback, origin);
        }
    }

    private int parseIntegerFromMap(Map<String, String[]> map, String parameterKey) throws InvalidRequestException {
        try {
            return Integer.parseInt(map.get(parameterKey)[0]);
        } catch (NumberFormatException ex) {
            throw new InvalidRequestException("Could not make an integer out of the query parameter '"
                + parameterKey + "'", ex);
        }
    }

}
