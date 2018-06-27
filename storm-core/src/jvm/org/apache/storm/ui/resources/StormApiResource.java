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
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

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
     * /api/v1/cluster/summary -> cluster summary.
     */
    @GET
    @Path("/cluster/summary")
    @Produces("application/json")
    public String getClusterSummary() {
        try {
            String user = servletRequest.getRemoteUser();
            return JSONValue.toJSONString(
                    UiHelpers.getClusterSummary(
                            nimbusClient.getClient().getClusterInfo(), user, config)
            );
        } catch (TException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * /api/v1/nimbus/summary -> nimbus summary.
     */
    @GET
    @Path("/nimbus/summary")
    @Produces("application/json")
    public String getNimbusSummary() {
        try {
            String user = servletRequest.getRemoteUser();
            return JSONValue.toJSONString(
                    UiHelpers.getClusterSummary(
                            nimbusClient.getClient().getClusterInfo(), user, config)
            );
        } catch (TException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * /api/v1/owner-resources -> owner resources.
     */
    @GET
    @Path("/owner-resources")
    @Produces("application/json")
    public String getOwnerResources() {
        try {
            return JSONValue.toJSONString(
                    UiHelpers.getOwnerResourceSummaries(
                            nimbusClient.getClient().getOwnerResourceSummaries(null), config)
            );
        } catch (TException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * /api/v1/owner-resources/:id -> owner resources.
     */
    @GET
    @Path("/owner-resources/{id}")
    @Produces("application/json")
    public String getOwnerResource(@PathParam("id") String id) {
        try {
            return JSONValue.toJSONString(
                    UiHelpers.getOwnerResourceSummary(
                            nimbusClient.getClient().getOwnerResourceSummaries(id),nimbusClient.getClient(),
                            id, config)
            );
        } catch (TException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * /api/v1/history/summary -> topo history.
     */
    @GET
    @Path("/history/summary")
    @Produces("application/json")
    public String getHistorySummary() {
        try {
            String user = servletRequest.getRemoteUser();
            return JSONValue.toJSONString(
                    UiHelpers.getTopologyHistoryInfo(
                            nimbusClient.getClient().getTopologyHistory(user)
            ));
        } catch (TException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * /api/v1/supervisor/summary -> topo history.
     */
    @GET
    @Path("/supervisor/summary")
    @Produces("application/json")
    public String getSupervisorSummary(@Context SecurityContext securityContext) {
        try {
            return JSONValue.toJSONString(
                    UiHelpers.getSupervisorSummary(
                            nimbusClient.getClient().getClusterInfo().get_supervisors(),
                            securityContext, config
                    ));
        } catch (TException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * /api/v1/supervisor -> topo history.
     */
    @GET
    @Path("/supervisor/{id}")
    @Produces("application/json")
    public String getSupervisor(@PathParam("id") String id, @QueryParam("host") String host,
                                @QueryParam("sys") boolean sys) {
        try {
            return JSONValue.toJSONString(
                    UiHelpers.getSupervisorPageInfo(
                            nimbusClient.getClient().getSupervisorPageInfo(id, host, sys),
                            config
                    ));
        } catch (TException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * /api/v1/topology/summary -> topo history.
     */
    @GET
    @Path("/topology/summary")
    @Produces("application/json")
    public String getTopologySummary(@Context SecurityContext securityContext) {
        try {
            return JSONValue.toJSONString(
                    UiHelpers.getAllTopologiesSummary(
                            nimbusClient.getClient().getClusterInfo().get_topologies(),
                            config
                    ));
        } catch (TException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * /api/v1/topology -> topo.
     */
    @GET
    @Path("/topology/{id}")
    @Produces("application/json")
    public String getTopology(@PathParam("id") String id,
                              @DefaultValue(":all-time") @QueryParam("sys") String window,
                              @QueryParam("sys") boolean sys) {
        try {
            return JSONValue.toJSONString(
                    UiHelpers.getTopologySummary(
                            nimbusClient.getClient().getTopologyPageInfo(
                                    id, window, sys),
                            window, config,
                            servletRequest.getRemoteUser()
                    ));
        } catch (TException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * /api/v1/topology-workers/:id -> topo workers.
     */
    @GET
    @Path("/topology-workers/:id")
    @Produces("application/json")
    public String getTopologyWorkers(@PathParam("id") String id) {
        try {
            return JSONValue.toJSONString(
                    UiHelpers.getTopologyWorkers(
                            nimbusClient.getClient().getTopologyInfo(id), id, config
                    ));
        } catch (TException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * /api/v1/topology/:id/lag -> lag
     */
    @GET
    @Path("/topology/:id/lag")
    @Produces("application/json")
    public String getTopologyMetrics(@PathParam("id") String id) {
        try {
            return JSONValue.toJSONString(
                    UiHelpers.getTopologyLag(
                            nimbusClient.getClient().getUserTopology(id), config
                    ));
        } catch (TException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * /api/v1/topology/:id/visualization-init -> visualization-init
     */
    @GET
    @Path("/topology/:id/visualization-init")
    @Produces("application/json")
    public String getTopologyVisializationInit(@PathParam("id") String id,
                                               @DefaultValue(":all-time") String window,
                                               @QueryParam("sys") boolean sys
    ) {
        try {
            return JSONValue.toJSONString(
                    UiHelpers.getBuildVisualization(
                            nimbusClient.getClient(), config, window, id, sys
                    ));
        } catch (TException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * /api/v1/topology/:id/visualization -> visualization
     */
    @GET
    @Path("/topology/:id/visualization")
    @Produces("application/json")
    public String getTopologyVisialization(@PathParam("id") String id,
                                               @DefaultValue(":all-time") String window,
                                               @QueryParam("sys") boolean sys
    ) {
        try {
            return JSONValue.toJSONString(
                    UiHelpers.getVisualizationData(nimbusClient.getClient(), id, window, sys)
            );
        } catch (TException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * /api/v1//topology/:id/component/:component -> component
     */
    @GET
    @Path("/topology/:id/component/:component")
    @Produces("application/json")
    public String getTopologyVisialization(@PathParam("id") String id,
                                           @PathParam("component") String component,
                                           @DefaultValue(":all-time") String window,
                                           @QueryParam("sys") boolean sys
    ) {
        try {
            String user = servletRequest.getRemoteUser();
            return JSONValue.toJSONString(
                    UiHelpers.getComponentPage(nimbusClient.getClient(), id, component,
                            window, sys, user, config)
            );
        } catch (TException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * /api/v1/topology/:id/logconfig -> component
     */
    @GET
    @Path("/topology/:id/logconfig")
    @Produces("application/json")
    public String getTopologyLogconfig(@PathParam("id") String id) {
        try {
            String user = servletRequest.getRemoteUser();
            return JSONValue.toJSONString(
                    UiHelpers.getTopolgoyLogConfig(nimbusClient.getClient().getLogConfig(id))
            );
        } catch (TException e) {
            e.printStackTrace();
            return null;
        }
    }
}
