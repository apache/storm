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

package org.apache.storm.daemon.ui.exceptionmappers;

import javax.inject.Provider;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import org.apache.storm.daemon.common.JsonResponseBuilder;
import org.apache.storm.daemon.ui.UIHelpers;
import org.apache.storm.daemon.ui.resources.StormApiResource;
import org.apache.storm.generated.AuthorizationException;


public class ExceptionMapperUtils {

    /**
     * getResponse.
     * @param ex ex
     * @param responseStatus responseStatus
     * @return getResponse
     */
    public static Response getResponse(Exception ex, Response.Status responseStatus,
                                       Provider<HttpServletRequest> request) {
        String callback = null;
        if (request.get().getParameterMap().containsKey(StormApiResource.callbackParameterName)) {
            callback = String.valueOf(request.get().getParameterMap().get(StormApiResource.callbackParameterName));
        }
        return new JsonResponseBuilder().setData(
                UIHelpers.exceptionToJson(ex, responseStatus.getStatusCode())).setCallback(callback)
                .setStatus(responseStatus.getStatusCode()).build();
    }

    /**
     * getResponse.
     * @param ex ex
     * @param request request
     * @return getResponse
     */
    public static Response getResponse(AuthorizationException ex, Provider<HttpServletRequest> request) {
        return getResponse(ex, Response.Status.UNAUTHORIZED, request);
    }

    /**
     * getResponse.
     * @param ex ex
     * @return getResponse
     */
    public static Response getResponse(Exception ex, Provider<HttpServletRequest> request) {
        return getResponse(ex, Response.Status.INTERNAL_SERVER_ERROR, request);
    }


}
