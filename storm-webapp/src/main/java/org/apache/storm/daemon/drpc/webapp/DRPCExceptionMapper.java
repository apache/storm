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

package org.apache.storm.daemon.drpc.webapp;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.storm.generated.DRPCExecutionException;
import org.json.simple.JSONValue;

@Provider
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class DRPCExceptionMapper implements ExceptionMapper<DRPCExecutionException> {

    @Override
    public Response toResponse(DRPCExecutionException ex) {
        ResponseBuilder builder = Response.status(500);
        switch (ex.get_type()) {
            case FAILED_REQUEST:
                builder.status(400);
                break;
            case SERVER_SHUTDOWN:
                builder.status(503); //Not available
                break;
            case SERVER_TIMEOUT:
                builder.status(504); //proxy timeout
                break;
            case INTERNAL_ERROR:
                //fall throw on purpose
            default:
                //Empty (Still 500)
                break;
            
        }
        Map<String, String> body = new HashMap<>();
        //TODO I would love to standardize this...
        body.put("error", ex.is_set_type() ? ex.get_type().toString() : "Internal Error");
        body.put("errorMessage", ex.get_msg());
        return builder.entity(JSONValue.toJSONString(body)).type("application/json").build();
    }

}
