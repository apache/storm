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

package org.apache.storm.ui.resources;

import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.UriInfo;

import org.apache.http.HttpRequest;
import org.apache.storm.ui.UiHelpers;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.apache.thrift.TException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Root resource (exposed at "storm" path).
 */
@Path("/api/v1/")
public class StormApiResource {

    public static final Logger LOG = LoggerFactory.getLogger(StormApiResource.class);

    @Context
    private UriInfo info;

    @Context
    private HttpServletRequest servletRequest;


    @Context
    private ServletContext servletContext;

    public static NimbusClient nimbusClient =
            NimbusClient.getConfiguredClient(Utils.readStormConfig());

    public static Map<String, Object> config = Utils.readStormConfig();
    /**
     * /api/v1/cluster/configuration -> nimbus configuration.
     */

    @GET
    @Path("/cluster/configuration")
    @Produces("application/json")
    public String getClusterConfiguration() {
        try {
            return nimbusClient.getClient().getNimbusConf();
        } catch (TException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * /api/v1/cluster/configuration -> nimbus configuration.
     */
    @GET
    @Path("/cluster/summary")
    @Produces("application/json")
    public String getClusterSummary() {
        try {
            LOG.info("HELL");
            String user = servletRequest.getRemoteUser();
            // System.out.println("HELL");
            System.out.println(user);
            LOG.info("HELL");
            LOG.info(user);
            LOG.info(nimbusClient.getClient().getClusterInfo().toString());
            return JSONValue.toJSONString(
                    UiHelpers.getClusterSummary(
                            nimbusClient.getClient().getClusterInfo(), user, config)
            );
        } catch (TException e) {
            e.printStackTrace();
            return null;
        }
    }
}
