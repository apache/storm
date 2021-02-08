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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;

import org.apache.storm.daemon.drpc.DRPC;
import org.apache.storm.metric.StormMetricsRegistry;

@Path("/drpc/")
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class DRPCResource {
    private final Meter meterHttpRequests;
    private final Timer responseDuration;
    private final DRPC drpc;

    public DRPCResource(DRPC drpc, StormMetricsRegistry metricsRegistry) {
        this.drpc = drpc;
        this.meterHttpRequests = metricsRegistry.registerMeter("drpc:num-execute-http-requests");
        this.responseDuration = metricsRegistry.registerTimer("drpc:HTTP-request-response-duration");
    }
    
    //TODO put in some better exception mapping...
    //TODO move populateContext to a filter...
    @POST
    @Path("/{func}") 
    public String post(@PathParam("func") String func, String args, @Context HttpServletRequest request) throws Exception {
        meterHttpRequests.mark();
        return responseDuration.time(() -> drpc.executeBlocking(func, args));
    }
    
    @GET
    @Path("/{func}/{args}") 
    public String get(@PathParam("func") String func, @PathParam("args") String args,
                      @Context HttpServletRequest request) throws Exception {
        meterHttpRequests.mark();
        return responseDuration.time(() -> drpc.executeBlocking(func, args));
    }
    
    @GET
    @Path("/{func}") 
    public String get(@PathParam("func") String func, @Context HttpServletRequest request) throws Exception {
        meterHttpRequests.mark();
        return responseDuration.time(() -> drpc.executeBlocking(func, ""));
    }
}
