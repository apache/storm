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

import static org.apache.storm.daemon.ui.exceptionmappers.ExceptionMapperUtils.getResponse;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class DefaultExceptionMapper implements ExceptionMapper<Throwable> {

    @Inject
    public javax.inject.Provider<HttpServletRequest> request;

    /**
     * toResponse.
     * @param throwable throwable
     * @return response
     */
    @Override
    public Response toResponse(Throwable throwable) {
        if (throwable instanceof Exception) {
            return getResponse((Exception) throwable, request);
        } else {
            return getResponse(new Exception(throwable.getMessage(), throwable), request);
        }
    }
}
