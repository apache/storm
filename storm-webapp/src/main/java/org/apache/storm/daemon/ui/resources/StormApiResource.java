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

package org.apache.storm.daemon.ui.resources;

import com.codahale.metrics.Meter;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import org.apache.storm.daemon.ui.UIHelpers;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Root resource (exposed at "storm" path).
 */
@Path("/")
public class StormApiResource {

    public static final Logger LOG = LoggerFactory.getLogger(StormApiResource.class);

    public static final String callbackParameterName = "callback";

    @Context
    private HttpServletRequest servletRequest;

    public static Map<String, Object> config = ConfigUtils.readStormConfig();

    private final Meter clusterConfigurationRequestMeter;
    private final Meter clusterSummaryRequestMeter;
    private final Meter nimbusSummaryRequestMeter;
    private final Meter supervisorRequestMeter;
    private final Meter supervisorSummaryRequestMeter;
    private final Meter allTopologiesSummaryRequestMeter;
    private final Meter topologyPageRequestMeter;
    private final Meter topologyMetricRequestMeter;
    private final Meter buildVisualizationRequestMeter;
    private final Meter mkVisualizationDataRequestMeter;
    private final Meter componentPageRequestMeter;
    private final Meter logConfigRequestMeter;
    private final Meter activateTopologyRequestMeter;
    private final Meter deactivateTopologyRequestMeter;
    private final Meter debugTopologyRequestMeter;
    private final Meter componentOpResponseRequestMeter;
    private final Meter topologyOpResponseMeter;
    private final Meter topologyLagRequestMeter;
    private final Meter getOwnerResourceSummariesMeter;

    @Inject
    public StormApiResource(StormMetricsRegistry metricsRegistry) {
        this.clusterConfigurationRequestMeter = metricsRegistry.registerMeter("ui:num-cluster-configuration-http-requests");
        this.clusterSummaryRequestMeter = metricsRegistry.registerMeter("ui:num-cluster-summary-http-requests");
        this.nimbusSummaryRequestMeter = metricsRegistry.registerMeter("ui:num-nimbus-summary-http-requests");
        this.supervisorRequestMeter = metricsRegistry.registerMeter("ui:num-supervisor-http-requests");
        this.supervisorSummaryRequestMeter = metricsRegistry.registerMeter("ui:num-supervisor-summary-http-requests");
        this.allTopologiesSummaryRequestMeter = metricsRegistry.registerMeter("ui:num-all-topologies-summary-http-requests");
        this.topologyPageRequestMeter = metricsRegistry.registerMeter("ui:num-topology-page-http-requests");
        this.topologyMetricRequestMeter = metricsRegistry.registerMeter("ui:num-topology-metric-http-requests");
        this.buildVisualizationRequestMeter = metricsRegistry.registerMeter("ui:num-build-visualization-http-requests");
        this.mkVisualizationDataRequestMeter = metricsRegistry.registerMeter("ui:num-mk-visualization-data-http-requests");
        this.componentPageRequestMeter = metricsRegistry.registerMeter("ui:num-component-page-http-requests");
        this.logConfigRequestMeter = metricsRegistry.registerMeter("ui:num-log-config-http-requests");
        this.activateTopologyRequestMeter = metricsRegistry.registerMeter("ui:num-activate-topology-http-requests");
        this.deactivateTopologyRequestMeter = metricsRegistry.registerMeter("ui:num-deactivate-topology-http-requests");
        this.debugTopologyRequestMeter = metricsRegistry.registerMeter("ui:num-debug-topology-http-requests");
        this.componentOpResponseRequestMeter = metricsRegistry.registerMeter("ui:num-component-op-response-http-requests");
        this.topologyOpResponseMeter = metricsRegistry.registerMeter("ui:num-topology-op-response-http-requests");
        this.topologyLagRequestMeter = metricsRegistry.registerMeter("ui:num-topology-lag-http-requests");
        this.getOwnerResourceSummariesMeter = metricsRegistry.registerMeter("ui:num-get-owner-resource-summaries-http-request");
    }

    /**
     * /api/v1/cluster/configuration -> nimbus configuration.
     */

    @GET
    @Path("/cluster/configuration")
    @Produces("application/json")
    public Response getClusterConfiguration(@QueryParam(callbackParameterName) String callback) throws TException {
        clusterConfigurationRequestMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    nimbusClient.getClient().getNimbusConf(),
                    callback, false, Response.Status.OK
            );
        }
    }

    /**
     * /api/v1/cluster/summary -> cluster summary.
     */
    @GET
    @Path("/cluster/summary")
    @AuthNimbusOp("getClusterInfo")
    @Produces("application/json")
    public Response getClusterSummary(@QueryParam(callbackParameterName) String callback) throws TException {
        clusterSummaryRequestMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            String user = servletRequest.getRemoteUser();
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getClusterSummary(
                            nimbusClient.getClient().getClusterInfo(), user, config),
                    callback
            );
        }
    }

    /**
     * /api/v1/nimbus/summary -> nimbus summary.
     */
    @GET
    @Path("/nimbus/summary")
    @AuthNimbusOp("getClusterInfo")
    @Produces("application/json")
    public Response getNimbusSummary(@QueryParam(callbackParameterName) String callback) throws TException {
        nimbusSummaryRequestMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getNimbusSummary(
                            nimbusClient.getClient().getClusterInfo(), config),
                    callback
            );
        }
    }

    /**
     * /api/v1/owner-resources -> owner resources.
     */
    @GET
    @Path("/owner-resources")
    @AuthNimbusOp("getOwnerResourceSummaries")
    @Produces("application/json")
    public Response getOwnerResources(@QueryParam(callbackParameterName) String callback) throws TException {
        getOwnerResourceSummariesMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getOwnerResourceSummaries(
                            nimbusClient.getClient().getOwnerResourceSummaries(null), config),
                    callback
            );
        }
    }

    /**
     * /api/v1/owner-resources/:id -> owner resources.
     */
    @GET
    @Path("/owner-resources/{id}")
    @AuthNimbusOp(value = "getOwnerResourceSummaries")
    @Produces("application/json")
    public Response getOwnerResource(@PathParam("id") String id,
                                     @QueryParam(callbackParameterName) String callback) throws TException {
        getOwnerResourceSummariesMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getOwnerResourceSummary(
                            nimbusClient.getClient().getOwnerResourceSummaries(id), nimbusClient.getClient(),
                            id, config),
                    callback
            );
        }
    }

    /**
     * /api/v1/history/summary -> topo history.
     */
    @GET
    @Path("/history/summary")
    @Produces("application/json")
    public Response getHistorySummary(@QueryParam(callbackParameterName) String callback) throws TException {
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            String user = servletRequest.getRemoteUser();
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getTopologyHistoryInfo(
                            nimbusClient.getClient().getTopologyHistory(user)),
                    callback
            );
        }
    }

    /**
     * /api/v1/supervisor/summary -> supervisor summary.
     */
    @GET
    @Path("/supervisor/summary")
    @AuthNimbusOp("getClusterInfo")
    @Produces("application/json")
    public Response getSupervisorSummary(@Context SecurityContext securityContext,
                                         @QueryParam(callbackParameterName) String callback) throws TException {
        supervisorSummaryRequestMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getSupervisorSummary(
                            nimbusClient.getClient().getClusterInfo().get_supervisors(),
                            securityContext, config
                    ),
                    callback
            );
        }
    }

    /**
     * /api/v1/supervisor -> topo history.
     */
    @GET
    @Path("/supervisor")
    @AuthNimbusOp("getSupervisorPageInfo")
    @Produces("application/json")
    public Response getSupervisor(@QueryParam("id") String id,
                                  @QueryParam("host") String host,
                                  @QueryParam("sys") boolean sys,
                                  @QueryParam(callbackParameterName) String callback) throws TException {
        supervisorRequestMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getSupervisorPageInfo(
                            nimbusClient.getClient().getSupervisorPageInfo(id, host, sys),
                            config
                    ),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology/summary -> topo history.
     */
    @GET
    @Path("/topology/summary")
    @AuthNimbusOp("getClusterInfo")
    @Produces("application/json")
    public Response getTopologySummary(@Context SecurityContext securityContext,
                                       @QueryParam(callbackParameterName) String callback) throws TException {
        allTopologiesSummaryRequestMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getAllTopologiesSummary(
                            nimbusClient.getClient().getTopologySummaries(),
                            config
                    ),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology -> topo.
     */
    @GET
    @Path("/topology/{id}")
    @AuthNimbusOp(value = "getTopology", needsTopoId = true)
    @Produces("application/json")
    public Response getTopology(@PathParam("id") String id,
                                @DefaultValue(":all-time") @QueryParam("window") String window,
                                @QueryParam("sys") boolean sys,
                                @QueryParam(callbackParameterName) String callback) throws TException {
        topologyPageRequestMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getTopologySummary(
                            nimbusClient.getClient().getTopologyPageInfo(id, window, sys),
                            window, config,
                            servletRequest.getRemoteUser()
                    ),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology-workers/:id -> topo workers.
     */
    @GET
    @Path("/topology-workers/{id}")
    @AuthNimbusOp(value = "getTopology", needsTopoId = true)
    @Produces("application/json")
    public Response getTopologyWorkers(@PathParam("id") String id,
        @QueryParam(callbackParameterName) String callback) throws TException {
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            id = Utils.urlDecodeUtf8(id);
            return UIHelpers.makeStandardResponse(
                UIHelpers.getTopologyWorkers(
                    nimbusClient.getClient().getTopologyInfo(id), config
                ),
                callback
            );
        }
    }

    /**
     * /api/v1/topology/:id/metrics -> metrics.
     */
    @GET
    @Path("/topology/{id}/metrics")
    @AuthNimbusOp(value = "getTopology", needsTopoId = true)
    @Produces("application/json")
    public Response getTopologyMetrics(@PathParam("id") String id,
                                       @DefaultValue(":all-time") @QueryParam("window") String window,
                                       @QueryParam("sys") boolean sys,
                                       @QueryParam(callbackParameterName) String callback) throws TException {
        topologyMetricRequestMeter.mark();
        String user = servletRequest.getRemoteUser();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getTopologySummary(
                            nimbusClient.getClient().getTopologyPageInfo(id, window, sys), window, config, user
                    ),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology/:id/lag -> lag.
     */
    @GET
    @Path("/topology/{id}/lag")
    @AuthNimbusOp(value = "getTopology", needsTopoId = true)
    @Produces("application/json")
    public Response getTopologyLag(@PathParam("id") String id,
                                   @QueryParam(callbackParameterName) String callback) throws TException {
        topologyLagRequestMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getTopologyLag(nimbusClient.getClient().getTopology(id), config),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology/:id/visualization-init -> visualization-init.
     */
    @GET
    @Path("/topology/{id}/visualization-init")
    @AuthNimbusOp(value = "getTopology", needsTopoId = true)
    @Produces("application/json")
    public Response getTopologyVisializationInit(@PathParam("id") String id,
                                                 @QueryParam("sys") boolean sys,
                                                 @QueryParam(callbackParameterName) String callback,
                                                 @DefaultValue(":all-time") @QueryParam("window") String window
    ) throws TException {
        mkVisualizationDataRequestMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getBuildVisualization(nimbusClient.getClient(), config, window, id, sys),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology/:id/visualization -> visualization.
     */
    @GET
    @Path("/topology/{id}/visualization")
    @AuthNimbusOp(value = "getTopology", needsTopoId = true)
    @Produces("application/json")
    public Response getTopologyVisualization(@PathParam("id") String id,
                                             @QueryParam("sys") boolean sys,
                                             @QueryParam(callbackParameterName) String callback,
                                             @DefaultValue(":all-time") @QueryParam("window") String window
    ) throws TException {
        buildVisualizationRequestMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getVisualizationData(nimbusClient.getClient(), window, id, sys),
                    callback
            );
        } catch (RuntimeException e) {
            LOG.error("Failure getting topology visualization", e);
            throw e;
        }
    }

    /**
     * /api/v1/topology/:id/component/:component -> component.
     */
    @GET
    @Path("/topology/{id}/component/{component}")
    @AuthNimbusOp(value = "getTopology", needsTopoId = true)
    @Produces("application/json")
    public Response getTopologyComponent(@PathParam("id") String id,
                                         @PathParam("component") String component,
                                         @QueryParam("sys") boolean sys,
                                         @QueryParam(callbackParameterName) String callback,
                                         @DefaultValue(":all-time") @QueryParam("window") String window
                                         ) throws TException {
        componentPageRequestMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            String user = servletRequest.getRemoteUser();
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getComponentPage(
                            nimbusClient.getClient(), id, component,
                            window, sys, user, config
                    ),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology/:id/logconfig -> logconfig.
     */
    @GET
    @Path("/topology/{id}/logconfig")
    @AuthNimbusOp(value = "getTopology", needsTopoId = true)
    @Produces("application/json")
    public Response getTopologyLogconfig(@PathParam("id") String id,
                                         @QueryParam(callbackParameterName) String callback) throws TException {
        logConfigRequestMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getTopolgoyLogConfig(nimbusClient.getClient().getLogConfig(id)),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology/:id/logconfig -> logconfig.
     */
    @POST
    @Path("/topology/{id}/logconfig")
    @AuthNimbusOp(value = "setLogConfig", needsTopoId = true)
    @Produces("application/json")
    @Consumes("application/json")
    public Response putTopologyLogconfig(@PathParam("id") String id, String body,
                                         @QueryParam(callbackParameterName) String callback) throws TException {
        topologyOpResponseMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.putTopologyLogLevel(nimbusClient.getClient(),
                            ((Map<String, Map>) JSONValue.parse(body)).get("namedLoggerLevels"), id),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology/:id/activate -> topology activate.
     */
    @POST
    @Path("/topology/{id}/activate")
    @AuthNimbusOp(value = "activate", needsTopoId = true)
    @Produces("application/json")
    public Response putTopologyActivate(@PathParam("id") String id,
                                        @QueryParam(callbackParameterName) String callback) throws TException {
        activateTopologyRequestMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.putTopologyActivate(nimbusClient.getClient(), id),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology/:id/deactivate -> topology deactivate.
     */
    @POST
    @Path("/topology/{id}/deactivate")
    @AuthNimbusOp(value = "deactivate", needsTopoId = true)
    @Produces("application/json")
    public Response putTopologyDeactivate(@PathParam("id") String id,
                                          @QueryParam(callbackParameterName) String callback) throws TException {
        deactivateTopologyRequestMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.putTopologyDeactivate(nimbusClient.getClient(), id),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology/:id/debug/:action/:spct -> debug action.
     */
    @POST
    @Path("/topology/{id}/debug/{action}/{spct}")
    @AuthNimbusOp(value = "debug", needsTopoId = true)
    @Produces("application/json")
    public Response putTopologyDebugActionSpct(@PathParam("id") String id,
                                               @PathParam("action") String action,
                                               @PathParam("spct") String spct,
                                               @QueryParam(callbackParameterName) String callback) throws TException {
        debugTopologyRequestMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.putTopologyDebugActionSpct(
                            nimbusClient.getClient(), id, action, spct, ""
                    ),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology/:id/component/:component/debug/:action/:spct -> debug component action.
     */
    @POST
    @Path("/topology/{id}/component/{component}/debug/{action}/{spct}")
    @AuthNimbusOp(value = "debug", needsTopoId = true)
    @Produces("application/json")
    public Response putTopologyComponentDebugActionSpct(
            @PathParam("id") String id,
            @PathParam("component") String component,
            @PathParam("action") String action,
            @PathParam("spct") String spct,
            @QueryParam(callbackParameterName) String callback) throws TException {
        componentOpResponseRequestMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.putTopologyDebugActionSpct(
                            nimbusClient.getClient(), id, component, action, spct
                    ),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology/:id/rebalance/:wait-time -> topology rebalance.
     */
    @POST
    @Path("/topology/{id}/rebalance/{wait-time}")
    @AuthNimbusOp(value = "rebalance", needsTopoId = true)
    @Produces("application/json")
    public Response putTopologyRebalance(@PathParam("id") String id,
                                         @PathParam("wait-time") String waitTime,
                                         @QueryParam(callbackParameterName) String callback) throws TException {
        topologyOpResponseMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.putTopologyRebalance(
                            nimbusClient.getClient(), id, waitTime
                    ),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology/:id/kill/:wait-time -> topology kill.
     */
    @POST
    @Path("/topology/{id}/kill/{wait-time}")
    @AuthNimbusOp(value = "killTopology", needsTopoId = true)
    @Produces("application/json")
    public Response putTopologyKill(
        @PathParam("id") String id,
        @PathParam("wait-time") String waitTime,
        @QueryParam(callbackParameterName) String callback) throws TException {
        topologyOpResponseMeter.mark();
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                UIHelpers.putTopologyKill(nimbusClient.getClient(), id, waitTime),
                callback
            );
        }
    }

    /**
     * /api/v1/topology/:id/profiling/start/:host-port/:timeout -> profiling start.
     */
    @GET
    @Path("/topology/{id}/profiling/start/{host-port}/{timeout}")
    @AuthNimbusOp(value = "setWorkerProfiler", needsTopoId = true)
    @Produces("application/json")
    public Response getTopologyProfilingStart(@PathParam("id") String id,
                                              @PathParam("host-port") String hostPort,
                                              @PathParam("timeout") String timeout,
                                              @QueryParam(callbackParameterName) String callback) throws TException {
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getTopologyProfilingStart(nimbusClient.getClient(), id, hostPort, timeout, config),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology/:id/profiling/stop/:host-port -> profiling stop.
     */
    @GET
    @Path("/topology/{id}/profiling/stop/{host-port}")
    @AuthNimbusOp(value = "setWorkerProfiler", needsTopoId = true)
    @Produces("application/json")
    public Response getTopologyProfilingStop(@PathParam("id") String id,
                                             @PathParam("host-port") String hostPort,
                                             @QueryParam(callbackParameterName) String callback) throws TException {
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getTopologyProfilingStop(nimbusClient.getClient(), id, hostPort, config),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology/:id/profiling/dumpprofile/:host-port -> dump profile.
     */
    @GET
    @Path("/topology/{id}/profiling/dumpprofile/{host-port}")
    @AuthNimbusOp(value = "setWorkerProfiler", needsTopoId = true)
    @Produces("application/json")
    public Response getTopologyProfilingDumpProfile(@PathParam("id") String id,
                                                    @PathParam("host-port") String hostPort,
                                                    @QueryParam(callbackParameterName) String callback) throws TException {
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getTopologyProfilingDump(nimbusClient.getClient(), id, hostPort, config),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology/:id/profiling/dumpjstack/:host-port -> dump jstack.
     */
    @GET
    @Path("/topology/{id}/profiling/dumpjstack/{host-port}")
    @AuthNimbusOp(value = "setWorkerProfiler", needsTopoId = true)
    @Produces("application/json")
    public Response getTopologyProfilingDumpJstack(@PathParam("id") String id,
                                                   @PathParam("host-port") String hostPort,
                                                   @QueryParam(callbackParameterName) String callback) throws TException {
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getTopologyProfilingDumpJstack(
                            nimbusClient.getClient(), id, hostPort, config
                    ),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology/:id/profiling/restartworker/:host-port -> restart worker.
     */
    @GET
    @Path("/topology/{id}/profiling/restartworker/{host-port}")
    @AuthNimbusOp(value = "setWorkerProfiler", needsTopoId = true)
    @Produces("application/json")
    public Response getTopologyProfilingRestartWorker(@PathParam("id") String id,
                                                      @PathParam("host-port") String hostPort,
                                                      @QueryParam(callbackParameterName) String callback) throws TException {
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getTopologyProfilingRestartWorker(
                            nimbusClient.getClient(), id, hostPort, config
                    ),
                    callback
            );
        }
    }

    /**
     * /api/v1/topology/:id/profiling/dumpheap/:host-port -> dump heap.
     */
    @GET
    @Path("/topology/{id}/profiling/dumpheap/{host-port}")
    @AuthNimbusOp(value = "setWorkerProfiler", needsTopoId = true)
    @Produces("application/json")
    public Response getTopologyProfilingDumpheap(@PathParam("id") String id,
                                                 @PathParam("host-port") String hostPort,
                                                 @QueryParam(callbackParameterName) String callback) throws TException {
        try (NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config)) {
            return UIHelpers.makeStandardResponse(
                    UIHelpers.getTopologyProfilingDumpHeap(
                            nimbusClient.getClient(), id, hostPort, config
                    ),
                    callback
            );
        }
    }

}
