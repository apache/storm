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

package org.apache.storm.daemon.common;

import java.util.Collections;
import java.util.Map;
import javax.ws.rs.core.Response;

import org.apache.storm.daemon.ui.UIHelpers;

/**
 * Response builder for JSON. It utilizes {@link UIHelpers} to construct JSON body and headers.
 */
public class JsonResponseBuilder {
    private Object data;
    private String callback;
    private boolean needSerialize = true;
    private int status = 200;
    private Map<String, Object> headers = Collections.emptyMap();

    public JsonResponseBuilder setData(Object data) {
        this.data = data;
        return this;
    }

    public JsonResponseBuilder setCallback(String callback) {
        this.callback = callback;
        return this;
    }

    public JsonResponseBuilder setNeedSerialize(boolean needSerialize) {
        this.needSerialize = needSerialize;
        return this;
    }

    public JsonResponseBuilder setStatus(int status) {
        this.status = status;
        return this;
    }

    public JsonResponseBuilder setHeaders(Map<String, Object> headers) {
        this.headers = headers;
        return this;
    }

    /**
     * Build a Response based on given parameters.
     *
     * @return A Response.
     */
    public Response build() {
        String body = UIHelpers.getJsonResponseBody(data, callback, needSerialize);
        Map<String, Object> respHeaders = UIHelpers.getJsonResponseHeaders(callback, headers);
        Response.ResponseBuilder respBuilder = Response.status(status).entity(body);
        respHeaders.forEach(respBuilder::header);
        return respBuilder.build();
    }
}