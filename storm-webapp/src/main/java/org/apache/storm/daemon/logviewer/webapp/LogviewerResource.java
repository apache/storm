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

import java.io.IOException;
import java.net.URLDecoder;
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
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.IHttpCredentialsPlugin;
import org.apache.storm.ui.InvalidRequestException;
import org.apache.storm.ui.UIHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles HTTP requests for Logviewer.
 */
@Path("/")
public class LogviewerResource {
    private static final Logger LOG = LoggerFactory.getLogger(LogviewerResource.class);

    private static final Meter meterLogPageHttpRequests = StormMetricsRegistry.registerMeter("logviewer:num-log-page-http-requests");
    private static final Meter meterDaemonLogPageHttpRequests = StormMetricsRegistry.registerMeter(
            "logviewer:num-daemonlog-page-http-requests");
    private static final Meter meterDownloadLogFileHttpRequests = StormMetricsRegistry.registerMeter(
            "logviewer:num-download-log-file-http-requests");
    private static final Meter meterDownloadLogDaemonFileHttpRequests = StormMetricsRegistry.registerMeter(
            "logviewer:num-download-log-daemon-file-http-requests");
    private static final Meter meterListLogsHttpRequests = StormMetricsRegistry.registerMeter("logviewer:num-list-logs-http-requests");

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
     */
    public LogviewerResource(LogviewerLogPageHandler logviewerParam, LogviewerProfileHandler profileHandler,
                             LogviewerLogDownloadHandler logDownloadHandler, LogviewerLogSearchHandler logSearchHandler,
                             IHttpCredentialsPlugin httpCredsHandler) {
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
            String decodedFileName = URLDecoder.decode(request.getParameter("file"));
            String grep = request.getParameter("grep");
            return logviewer.logPage(decodedFileName, start, length, grep, user);
        } catch (InvalidRequestException e) {
            LOG.error(e.getMessage(), e);
            return Response.status(400).entity(e.getMessage()).build();
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
            String decodedFileName = URLDecoder.decode(request.getParameter("file"));
            String grep = request.getParameter("grep");
            return logviewer.daemonLogPage(decodedFileName, start, length, grep, user);
        } catch (InvalidRequestException e) {
            LOG.error(e.getMessage(), e);
            return Response.status(400).entity(e.getMessage()).build();
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
        String callback = request.getParameter("callback");
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
        String callback = request.getParameter("callback");
        String origin = request.getHeader("Origin");

        return logviewer.listLogFiles(user, portStr != null ? Integer.parseInt(portStr) : null, topologyId, callback, origin);
    }

    /**
     * Handles '/dumps' (listing dump files) request.
     */
    @GET
    @Path("/dumps/{topo-id}/{host-port}")
    public Response listDumpFiles(@PathParam("topo-id") String topologyId, @PathParam("host-port") String hostPort,
                                  @Context HttpServletRequest request) throws IOException {
        String user = httpCredsHandler.getUserName(request);
        return profileHandler.listDumpFiles(topologyId, hostPort, user);
    }

    /**
     * Handles '/dumps' (downloading specific dump file) request.
     */
    @GET
    @Path("/dumps/{topo-id}/{host-port}/{filename}")
    public Response downloadDumpFile(@PathParam("topo-id") String topologyId, @PathParam("host-port") String hostPort,
                                     @PathParam("filename") String fileName, @Context HttpServletRequest request) throws IOException {
        String user = httpCredsHandler.getUserName(request);
        return profileHandler.downloadDumpFile(topologyId, hostPort, fileName, user);
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
        String decodedFileName = URLDecoder.decode(file);
        return logDownloadHandler.downloadLogFile(decodedFileName, user);
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
        String decodedFileName = URLDecoder.decode(file);
        return logDownloadHandler.downloadDaemonLogFile(decodedFileName, user);
    }

    /**
     * Handles '/search' (searching from specific worker or daemon log file) request.
     */
    @GET
    @Path("/search")
    public Response search(@Context HttpServletRequest request) throws IOException {
        String user = httpCredsHandler.getUserName(request);
        boolean isDaemon = StringUtils.equals(request.getParameter("is-daemon"), "yes");
        String file = request.getParameter("file");
        String decodedFileName = URLDecoder.decode(file);
        String searchString = request.getParameter("search-string");
        String numMatchesStr = request.getParameter("num-matches");
        String startByteOffset = request.getParameter("start-byte-offset");
        String callback = request.getParameter("callback");
        String origin = request.getHeader("Origin");

        try {
            return logSearchHandler.searchLogFile(decodedFileName, user, isDaemon, searchString, numMatchesStr,
                    startByteOffset, callback, origin);
        } catch (InvalidRequestException e) {
            LOG.error(e.getMessage(), e);
            return new JsonResponseBuilder().setData(UIHelpers.exceptionToJson(e)).setCallback(callback)
                    .setStatus(400).build();
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
        String callback = request.getParameter("callback");
        String origin = request.getHeader("Origin");

        return logSearchHandler.deepSearchLogsForTopology(topologyId, user, searchString, numMatchesStr, portStr,
                startFileOffset, startByteOffset, BooleanUtils.toBooleanObject(searchArchived), callback, origin);
    }

    private int parseIntegerFromMap(Map map, String parameterKey) throws InvalidRequestException {
        try {
            return Integer.parseInt(((String[]) map.get(parameterKey))[0]);
        } catch (NumberFormatException ex) {
            throw new InvalidRequestException("Could not make an integer out of the query parameter '"
                + parameterKey + "'", ex);
        }
    }

}
