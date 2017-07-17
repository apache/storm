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

package org.apache.storm.daemon.logviewer.utils;

import static j2html.TagCreator.body;
import static j2html.TagCreator.h2;
import static javax.ws.rs.core.Response.Status.OK;
import static org.apache.commons.lang.StringEscapeUtils.escapeHtml;

import com.google.common.io.ByteStreams;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.storm.daemon.common.JsonResponseBuilder;
import org.apache.storm.ui.UIHelpers;

public class LogviewerResponseBuilder {

    private LogviewerResponseBuilder() {
    }

    /**
     * Build a Response object representing success response with HTML entity.
     *
     * @param content HTML entity content, String type
     */
    public static Response buildSuccessHtmlResponse(String content) {
        return Response.status(OK).entity(content)
                .type(MediaType.TEXT_HTML_TYPE).build();
    }

    /**
     * Build a Response object representing success response with JSON entity.
     *
     * @param entity entity object to represent it as JSON
     * @param callback callback for JSONP
     * @param origin origin
     * @see {@link JsonResponseBuilder}
     */
    public static Response buildSuccessJsonResponse(Object entity, String callback, String origin) {
        return new JsonResponseBuilder().setData(entity).setCallback(callback)
                .setHeaders(LogviewerResponseBuilder.getHeadersForSuccessResponse(origin)).build();
    }

    /**
     * Build a Response object representing download a file.
     *
     * @param file file to download
     */
    public static Response buildDownloadFile(File file) throws IOException {
        // do not close this InputStream in method: it will be used from jetty server
        InputStream is = new FileInputStream(file);
        return Response.status(OK)
                .entity(wrapWithStreamingOutput(is))
                .type(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                .header("Content-Disposition", "attachment; filename=\"" + file.getName() + "\"")
                .build();
    }

    /**
     * Build a Response object representing unauthorized user, with HTML response.
     *
     * @param user username
     */
    public static Response buildResponseUnautohrizedUser(String user) {
        String entity = buildUnauthorizedUserHtml(user);
        return Response.status(OK)
                .entity(entity)
                .type(MediaType.TEXT_HTML_TYPE)
                .build();
    }

    /**
     * Build a Response object representing page not found.
     */
    public static Response buildResponsePageNotFound() {
        return Response.status(404)
                .entity("Page not found")
                .type(MediaType.TEXT_HTML_TYPE)
                .build();
    }

    /**
     * Build a Response object representing unauthorized user, with JSON response.
     *
     * @param user username
     * @param callback callback for JSONP
     */
    public static Response buildUnauthorizedUserJsonResponse(String user, String callback) {
        return new JsonResponseBuilder().setData(UIHelpers.unauthorizedUserJson(user))
                .setCallback(callback).setStatus(401).build();
    }

    /**
     * Build a Response object representing exception, with JSON response.
     *
     * @param ex Exception object
     * @param callback callback for JSONP
     */
    public static Response buildExceptionJsonResponse(Exception ex, String callback) {
        return new JsonResponseBuilder().setData(UIHelpers.exceptionToJson(ex))
                .setCallback(callback).setStatus(500).build();
    }

    private static Map<String, Object> getHeadersForSuccessResponse(String origin) {
        Map<String, Object> headers = new HashMap<>();
        headers.put("Access-Control-Allow-Origin", origin);
        headers.put("Access-Control-Allow-Credentials", "true");
        return headers;
    }

    private static String buildUnauthorizedUserHtml(String user) {
        String content = "User '" + escapeHtml(user) + "' is not authorized.";
        return body(h2(content)).render();
    }

    private static StreamingOutput wrapWithStreamingOutput(final InputStream inputStream) {
        return os -> {
            OutputStream wrappedOutputStream = os;
            if (!(os instanceof BufferedOutputStream)) {
                wrappedOutputStream = new BufferedOutputStream(os);
            }

            ByteStreams.copy(inputStream, wrappedOutputStream);

            wrappedOutputStream.flush();
        };
    }

}
