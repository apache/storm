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

package org.apache.storm.daemon.nimbus;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.DerivativeGauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Timer;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.BindException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import javax.security.auth.Subject;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.DaemonConfig;
import org.apache.storm.StormTimer;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.blobstore.KeySequenceNumber;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.callback.DefaultWatcherCallBack;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.container.oci.OciUtils;
import org.apache.storm.daemon.DaemonCommon;
import org.apache.storm.daemon.Shutdownable;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.BeginDownloadResult;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.BoltAggregateStats;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.CommonAggregateStats;
import org.apache.storm.generated.ComponentAggregateStats;
import org.apache.storm.generated.ComponentPageInfo;
import org.apache.storm.generated.ComponentType;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.DebugOptions;
import org.apache.storm.generated.ErrorInfo;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GetInfoOptions;
import org.apache.storm.generated.IllegalStateException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.LSTopoHistory;
import org.apache.storm.generated.ListBlobsResult;
import org.apache.storm.generated.LogConfig;
import org.apache.storm.generated.LogLevel;
import org.apache.storm.generated.LogLevelAction;
import org.apache.storm.generated.Nimbus.Iface;
import org.apache.storm.generated.Nimbus.Processor;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.NumErrorsChoice;
import org.apache.storm.generated.OwnerResourceSummary;
import org.apache.storm.generated.ProfileAction;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.generated.SpecificAggregateStats;
import org.apache.storm.generated.SpoutAggregateStats;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.generated.SupervisorAssignments;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.generated.SupervisorPageInfo;
import org.apache.storm.generated.SupervisorSummary;
import org.apache.storm.generated.SupervisorWorkerHeartbeat;
import org.apache.storm.generated.SupervisorWorkerHeartbeats;
import org.apache.storm.generated.TopologyActionOptions;
import org.apache.storm.generated.TopologyHistoryInfo;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologyInitialStatus;
import org.apache.storm.generated.TopologyPageInfo;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.generated.WorkerMetricPoint;
import org.apache.storm.generated.WorkerMetrics;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.generated.WorkerSummary;
import org.apache.storm.logging.ThriftAccessLogger;
import org.apache.storm.metric.ClusterMetricsConsumerExecutor;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.metric.api.DataPoint;
import org.apache.storm.metric.api.IClusterMetricsConsumer;
import org.apache.storm.metric.api.IClusterMetricsConsumer.ClusterInfo;
import org.apache.storm.metricstore.AggLevel;
import org.apache.storm.metricstore.Metric;
import org.apache.storm.metricstore.MetricStore;
import org.apache.storm.metricstore.MetricStoreConfig;
import org.apache.storm.nimbus.AssignmentDistributionService;
import org.apache.storm.nimbus.DefaultTopologyValidator;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.ITopologyActionNotifierPlugin;
import org.apache.storm.nimbus.ITopologyValidator;
import org.apache.storm.nimbus.IWorkerHeartbeatsRecoveryStrategy;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.nimbus.WorkerHeartbeatsRecoveryStrategyFactory;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.DefaultScheduler;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.SupervisorResources;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.blacklist.BlacklistScheduler;
import org.apache.storm.scheduler.multitenant.MultitenantScheduler;
import org.apache.storm.scheduler.resource.ResourceAwareScheduler;
import org.apache.storm.scheduler.resource.ResourceUtils;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceRequest;
import org.apache.storm.scheduler.resource.normalization.ResourceMetrics;
import org.apache.storm.security.INimbusCredentialPlugin;
import org.apache.storm.security.auth.ClientAuthUtils;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ICredentialsRenewer;
import org.apache.storm.security.auth.IGroupMappingServiceProvider;
import org.apache.storm.security.auth.IPrincipalToLocal;
import org.apache.storm.security.auth.NimbusPrincipal;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.storm.security.auth.ThriftServer;
import org.apache.storm.security.auth.workertoken.WorkerTokenManager;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.shade.com.google.common.base.Strings;
import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.apache.storm.shade.com.google.common.collect.MapDifference;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.stats.ClientStatsUtil;
import org.apache.storm.stats.StatsUtil;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.BufferInputStream;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ReflectionUtils;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.ServerConfigUtils;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.SimpleVersion;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.TimeCacheMap;
import org.apache.storm.utils.TupleUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.Utils.UptimeComputer;
import org.apache.storm.utils.VersionInfo;
import org.apache.storm.utils.WrappedAlreadyAliveException;
import org.apache.storm.utils.WrappedAuthorizationException;
import org.apache.storm.utils.WrappedIllegalStateException;
import org.apache.storm.utils.WrappedInvalidTopologyException;
import org.apache.storm.utils.WrappedNotAliveException;
import org.apache.storm.validation.ConfigValidation;
import org.apache.storm.zookeeper.AclEnforcement;
import org.apache.storm.zookeeper.ClientZookeeper;
import org.apache.storm.zookeeper.Zookeeper;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Nimbus implements Iface, Shutdownable, DaemonCommon {
    @VisibleForTesting
    public static final List<ACL> ZK_ACLS = Arrays.asList(ZooDefs.Ids.CREATOR_ALL_ACL.get(0));
    public static final SimpleVersion MIN_VERSION_SUPPORT_RPC_HEARTBEAT = new SimpleVersion("2.0.0");
    private static final Logger LOG = LoggerFactory.getLogger(Nimbus.class);
    //    Metrics
    private final Meter submitTopologyWithOptsCalls;
    private final Meter submitTopologyCalls;
    private final Meter killTopologyWithOptsCalls;
    private final Meter killTopologyCalls;
    private final Meter rebalanceCalls;
    private final Meter activateCalls;
    private final Meter deactivateCalls;
    private final Meter debugCalls;
    private final Meter setWorkerProfilerCalls;
    private final Meter getComponentPendingProfileActionsCalls;
    private final Meter setLogConfigCalls;
    private final Meter uploadNewCredentialsCalls;
    private final Meter beginFileUploadCalls;
    private final Meter uploadChunkCalls;
    private final Meter finishFileUploadCalls;
    private final Meter downloadChunkCalls;
    private final Meter getNimbusConfCalls;
    private final Meter getLogConfigCalls;
    private final Meter getTopologyConfCalls;
    private final Meter getTopologyCalls;
    private final Meter getUserTopologyCalls;
    private final Meter getClusterInfoCalls;
    private final Meter getTopologySummariesCalls;
    private final Meter getTopologySummaryCalls;
    private final Meter getTopologySummaryByNameCalls;
    private final Meter getLeaderCalls;
    private final Meter isTopologyNameAllowedCalls;
    private final Meter getTopologyInfoWithOptsCalls;
    private final Meter getTopologyInfoCalls;
    private final Meter getTopologyInfoByNameCalls;
    private final Meter getTopologyInfoByNameWithOptsCalls;
    private final Meter getTopologyPageInfoCalls;
    private final Meter getSupervisorPageInfoCalls;
    private final Meter getComponentPageInfoCalls;
    private final Meter getOwnerResourceSummariesCalls;
    private final Meter shutdownCalls;
    private final Meter processWorkerMetricsCalls;
    private final Meter mkAssignmentsErrors;
    private final Meter sendAssignmentExceptions;   // used in AssignmentDistributionService.java

    //Timer
    private final Timer fileUploadDuration;
    private final Timer schedulingDuration;
    //Scheduler histogram
    private final Histogram numAddedExecPerScheduling;
    private final Histogram numAddedSlotPerScheduling;
    private final Histogram numRemovedExecPerScheduling;
    private final Histogram numRemovedSlotPerScheduling;
    private final Histogram numNetExecIncreasePerScheduling;
    private final Histogram numNetSlotIncreasePerScheduling;
    // END Metrics
    private static final String STORM_VERSION = VersionInfo.getVersion();

    public static List<ACL> getNimbusAcls(Map<String, Object> conf) {
        List<ACL> acls = null;
        if (Utils.isZkAuthenticationConfiguredStormServer(conf)) {
            acls = ZK_ACLS;
        }
        return acls;
    }

    public static final Subject NIMBUS_SUBJECT = new Subject();

    static {
        NIMBUS_SUBJECT.getPrincipals().add(new NimbusPrincipal());
        NIMBUS_SUBJECT.setReadOnly();
    }
    
    private static final TopologyStateTransition NOOP_TRANSITION = (arg, nimbus, topoId, base) -> null;
    private static final TopologyStateTransition INACTIVE_TRANSITION = (arg, nimbus, topoId, base) -> Nimbus.make(TopologyStatus.INACTIVE);
    private static final TopologyStateTransition ACTIVE_TRANSITION = (arg, nimbus, topoId, base) -> Nimbus.make(TopologyStatus.ACTIVE);
    private static final TopologyStateTransition REMOVE_TRANSITION = (args, nimbus, topoId, base) -> {
        LOG.info("Killing topology: {}", topoId);
        IStormClusterState state = nimbus.getStormClusterState();
        Assignment oldAssignment = state.assignmentInfo(topoId, null);
        state.removeStorm(topoId);
        notifySupervisorsAsKilled(state, oldAssignment, nimbus.getAssignmentsDistributer(), nimbus.getMetricsRegistry());
        nimbus.heartbeatsCache.removeTopo(topoId);
        nimbus.getIdToExecutors().getAndUpdate(new Dissoc<>(topoId));
        return null;
    };
    private static final TopologyStateTransition DO_REBALANCE_TRANSITION = (args, nimbus, topoId, base) -> {
        nimbus.doRebalance(topoId, base);
        return Nimbus.make(base.get_prev_status());
    };
    private static final TopologyStateTransition KILL_TRANSITION = (killTime, nimbus, topoId, base) -> {
        int delay = 0;
        if (killTime != null) {
            delay = ((Number) killTime).intValue();
        } else {
            delay = ObjectReader.getInt(Nimbus.readTopoConf(topoId, nimbus.getTopoCache()).get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
        }
        nimbus.delayEvent(topoId, delay, TopologyActions.REMOVE, null);
        StormBase sb = new StormBase();
        sb.set_status(TopologyStatus.KILLED);
        TopologyActionOptions tao = new TopologyActionOptions();
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(delay);
        tao.set_kill_options(opts);
        sb.set_topology_action_options(tao);
        sb.set_component_executors(Collections.emptyMap());
        sb.set_component_debug(Collections.emptyMap());
        return sb;
    };

    private static final TopologyStateTransition REBALANCE_TRANSITION = (args, nimbus, topoId, base) -> {
        RebalanceOptions rbo = ((RebalanceOptions) args).deepCopy();
        int delay = 0;
        if (rbo.is_set_wait_secs()) {
            delay = rbo.get_wait_secs();
        } else {
            delay = ObjectReader.getInt(Nimbus.readTopoConf(topoId, nimbus.getTopoCache()).get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
        }
        nimbus.delayEvent(topoId, delay, TopologyActions.DO_REBALANCE, null);

        rbo.set_wait_secs(delay);
        if (!rbo.is_set_num_executors()) {
            rbo.set_num_executors(Collections.emptyMap());
        }

        StormBase sb = new StormBase();
        sb.set_status(TopologyStatus.REBALANCING);
        sb.set_prev_status(base.get_status());
        TopologyActionOptions tao = new TopologyActionOptions();
        tao.set_rebalance_options(rbo);
        sb.set_topology_action_options(tao);
        sb.set_component_executors(Collections.emptyMap());
        sb.set_component_debug(Collections.emptyMap());

        return sb;
    };
    private static final TopologyStateTransition GAIN_LEADERSHIP_WHEN_KILLED_TRANSITION = (args, nimbus, topoId, base) -> {
        int delay = base.get_topology_action_options().get_kill_options().get_wait_secs();
        nimbus.delayEvent(topoId, delay, TopologyActions.REMOVE, null);
        return null;
    };
    private static final TopologyStateTransition GAIN_LEADERSHIP_WHEN_REBALANCING_TRANSITION = (args, nimbus, topoId, base) -> {
        int delay = base.get_topology_action_options().get_rebalance_options().get_wait_secs();
        nimbus.delayEvent(topoId, delay, TopologyActions.DO_REBALANCE, null);
        return null;
    };
    private static final Map<TopologyStatus, Map<TopologyActions, TopologyStateTransition>> TOPO_STATE_TRANSITIONS =
        new ImmutableMap.Builder<TopologyStatus, Map<TopologyActions, TopologyStateTransition>>()
            .put(TopologyStatus.ACTIVE, new ImmutableMap.Builder<TopologyActions, TopologyStateTransition>()
                .put(TopologyActions.INACTIVATE, INACTIVE_TRANSITION)
                .put(TopologyActions.ACTIVATE, NOOP_TRANSITION)
                .put(TopologyActions.REBALANCE, REBALANCE_TRANSITION)
                .put(TopologyActions.KILL, KILL_TRANSITION)
                .build())
            .put(TopologyStatus.INACTIVE, new ImmutableMap.Builder<TopologyActions, TopologyStateTransition>()
                .put(TopologyActions.ACTIVATE, ACTIVE_TRANSITION)
                .put(TopologyActions.INACTIVATE, NOOP_TRANSITION)
                .put(TopologyActions.REBALANCE, REBALANCE_TRANSITION)
                .put(TopologyActions.KILL, KILL_TRANSITION)
                .build())
            .put(TopologyStatus.KILLED, new ImmutableMap.Builder<TopologyActions, TopologyStateTransition>()
                .put(TopologyActions.GAIN_LEADERSHIP, GAIN_LEADERSHIP_WHEN_KILLED_TRANSITION)
                .put(TopologyActions.KILL, KILL_TRANSITION)
                .put(TopologyActions.REMOVE, REMOVE_TRANSITION)
                .build())
            .put(TopologyStatus.REBALANCING, new ImmutableMap.Builder<TopologyActions, TopologyStateTransition>()
                .put(TopologyActions.GAIN_LEADERSHIP, GAIN_LEADERSHIP_WHEN_REBALANCING_TRANSITION)
                .put(TopologyActions.KILL, KILL_TRANSITION)
                .put(TopologyActions.DO_REBALANCE, DO_REBALANCE_TRANSITION)
                .build())
            .build();
    private static final List<String> EMPTY_STRING_LIST = Collections.unmodifiableList(Collections.emptyList());
    private static final Set<String> EMPTY_STRING_SET = Collections.unmodifiableSet(Collections.emptySet());
    private static final RotatingMap<String, Long> topologyCleanupDetected = new RotatingMap<>(2);
    private static long topologyCleanupRotationTime = 0L;

    // END TOPOLOGY STATE TRANSITIONS

    private final Map<String, Object> conf;
    private final NavigableMap<SimpleVersion, List<String>> supervisorClasspaths;
    private final NimbusInfo nimbusHostPortInfo;
    private final INimbus inimbus;
    private final IAuthorizer impersonationAuthorizationHandler;
    private final AtomicLong submittedCount;
    private final IStormClusterState stormClusterState;
    private final Object submitLock = new Object();
    private final Object schedLock = new Object();
    private final Object credUpdateLock = new Object();
    private final HeartbeatCache heartbeatsCache;
    private final AtomicBoolean heartbeatsReadyFlag;
    private final IWorkerHeartbeatsRecoveryStrategy heartbeatsRecoveryStrategy;
    @SuppressWarnings("deprecation")
    private final TimeCacheMap<String, BufferInputStream> downloaders;
    @SuppressWarnings("deprecation")
    private final TimeCacheMap<String, WritableByteChannel> uploaders;
    private final BlobStore blobStore;
    private final TopoCache topoCache;
    @SuppressWarnings("deprecation")
    private final TimeCacheMap<String, BufferInputStream> blobDownloaders;
    @SuppressWarnings("deprecation")
    private final TimeCacheMap<String, OutputStream> blobUploaders;
    @SuppressWarnings("deprecation")
    private final TimeCacheMap<String, Iterator<String>> blobListers;
    private final UptimeComputer uptime;
    private final ITopologyValidator validator;
    private final StormTimer timer;
    private final IScheduler scheduler;
    private final IScheduler underlyingScheduler;
    //Metrics related
    private final AtomicReference<Long> schedulingStartTimeNs = new AtomicReference<>(null);
    private final AtomicLong longestSchedulingTime = new AtomicLong();

    private final ILeaderElector leaderElector;
    private final AssignmentDistributionService assignmentsDistributer;
    private final AtomicReference<Map<String, String>> idToSchedStatus;
    private final AtomicReference<Map<String, SupervisorResources>> nodeIdToResources;
    private final AtomicReference<Map<String, TopologyResources>> idToResources;
    private final AtomicReference<Map<String, Map<WorkerSlot, WorkerResources>>> idToWorkerResources;
    private final Collection<ICredentialsRenewer> credRenewers;
    private final Object topologyHistoryLock;
    private final LocalState topologyHistoryState;
    private final Collection<INimbusCredentialPlugin> nimbusAutocredPlugins;
    private final ITopologyActionNotifierPlugin nimbusTopologyActionNotifier;
    private final List<ClusterMetricsConsumerExecutor> clusterConsumerExceutors;
    private final IGroupMappingServiceProvider groupMapper;
    private final IPrincipalToLocal principalToLocal;
    private final StormMetricsRegistry metricsRegistry;
    private final ResourceMetrics resourceMetrics;
    private final ClusterSummaryMetricSet clusterMetricSet;
    private MetricStore metricsStore;
    private IAuthorizer authorizationHandler;
    //Cached CuratorFramework, mainly used for BlobStore.
    private final CuratorFramework zkClient;
    //Cached topology -> executor ids, used for deciding timeout workers of heartbeatsCache.
    private AtomicReference<Map<String, Set<List<Integer>>>> idToExecutors;
    //May be null if worker tokens are not supported by the thrift transport.
    private WorkerTokenManager workerTokenManager;
    private boolean wasLeader = false;

    public Nimbus(Map<String, Object> conf, INimbus inimbus, StormMetricsRegistry metricsRegistry) throws Exception {
        this(conf, inimbus, null, null, null, null, null, metricsRegistry);
    }

    public Nimbus(Map<String, Object> conf, INimbus inimbus, IStormClusterState stormClusterState, NimbusInfo hostPortInfo,
                  BlobStore blobStore, ILeaderElector leaderElector, IGroupMappingServiceProvider groupMapper,
                  StormMetricsRegistry metricsRegistry) throws Exception {
        this(conf, inimbus, stormClusterState, hostPortInfo, blobStore, null, leaderElector, groupMapper, metricsRegistry);
    }

    public Nimbus(Map<String, Object> conf, INimbus inimbus, IStormClusterState stormClusterState, NimbusInfo hostPortInfo,
                  BlobStore blobStore, TopoCache topoCache, ILeaderElector leaderElector, IGroupMappingServiceProvider groupMapper,
                  StormMetricsRegistry metricsRegistry)
        throws Exception {
        this.conf = conf;
        this.metricsRegistry = metricsRegistry;
        this.resourceMetrics = new ResourceMetrics(metricsRegistry);
        this.submitTopologyWithOptsCalls = metricsRegistry.registerMeter("nimbus:num-submitTopologyWithOpts-calls");
        this.submitTopologyCalls = metricsRegistry.registerMeter("nimbus:num-submitTopology-calls");
        this.killTopologyWithOptsCalls = metricsRegistry.registerMeter("nimbus:num-killTopologyWithOpts-calls");
        this.killTopologyCalls = metricsRegistry.registerMeter("nimbus:num-killTopology-calls");
        this.rebalanceCalls = metricsRegistry.registerMeter("nimbus:num-rebalance-calls");
        this.activateCalls = metricsRegistry.registerMeter("nimbus:num-activate-calls");
        this.deactivateCalls = metricsRegistry.registerMeter("nimbus:num-deactivate-calls");
        this.debugCalls = metricsRegistry.registerMeter("nimbus:num-debug-calls");
        this.setWorkerProfilerCalls = metricsRegistry.registerMeter("nimbus:num-setWorkerProfiler-calls");
        this.getComponentPendingProfileActionsCalls = metricsRegistry.registerMeter(
            "nimbus:num-getComponentPendingProfileActions-calls");
        this.setLogConfigCalls = metricsRegistry.registerMeter("nimbus:num-setLogConfig-calls");
        this.uploadNewCredentialsCalls = metricsRegistry.registerMeter("nimbus:num-uploadNewCredentials-calls");
        this.beginFileUploadCalls = metricsRegistry.registerMeter("nimbus:num-beginFileUpload-calls");
        this.uploadChunkCalls = metricsRegistry.registerMeter("nimbus:num-uploadChunk-calls");
        this.finishFileUploadCalls = metricsRegistry.registerMeter("nimbus:num-finishFileUpload-calls");
        this.downloadChunkCalls = metricsRegistry.registerMeter("nimbus:num-downloadChunk-calls");
        this.getNimbusConfCalls = metricsRegistry.registerMeter("nimbus:num-getNimbusConf-calls");
        this.getLogConfigCalls = metricsRegistry.registerMeter("nimbus:num-getLogConfig-calls");
        this.getTopologyConfCalls = metricsRegistry.registerMeter("nimbus:num-getTopologyConf-calls");
        this.getTopologyCalls = metricsRegistry.registerMeter("nimbus:num-getTopology-calls");
        this.getUserTopologyCalls = metricsRegistry.registerMeter("nimbus:num-getUserTopology-calls");
        this.getClusterInfoCalls = metricsRegistry.registerMeter("nimbus:num-getClusterInfo-calls");
        this.getTopologySummariesCalls = metricsRegistry.registerMeter("nimbus:num-getTopologySummaries-calls");
        this.getTopologySummaryCalls = metricsRegistry.registerMeter("nimbus:num-getTopologySummary-calls");
        this.getTopologySummaryByNameCalls = metricsRegistry.registerMeter("nimbus:num-getTopologySummaryByName-calls");
        this.getLeaderCalls = metricsRegistry.registerMeter("nimbus:num-getLeader-calls");
        this.isTopologyNameAllowedCalls = metricsRegistry.registerMeter("nimbus:num-isTopologyNameAllowed-calls");
        this.getTopologyInfoWithOptsCalls = metricsRegistry.registerMeter(
            "nimbus:num-getTopologyInfoWithOpts-calls");
        this.getTopologyInfoCalls = metricsRegistry.registerMeter("nimbus:num-getTopologyInfo-calls");
        this.getTopologyInfoByNameCalls = metricsRegistry.registerMeter("nimbus:num-getTopologyInfoByName-calls");
        this.getTopologyInfoByNameWithOptsCalls = metricsRegistry.registerMeter("nimbus:num-getTopologyInfoByNameWithOpts-calls");
        this.getTopologyPageInfoCalls = metricsRegistry.registerMeter("nimbus:num-getTopologyPageInfo-calls");
        this.getSupervisorPageInfoCalls = metricsRegistry.registerMeter("nimbus:num-getSupervisorPageInfo-calls");
        this.getComponentPageInfoCalls = metricsRegistry.registerMeter("nimbus:num-getComponentPageInfo-calls");
        this.getOwnerResourceSummariesCalls = metricsRegistry.registerMeter(
            "nimbus:num-getOwnerResourceSummaries-calls");
        this.shutdownCalls = metricsRegistry.registerMeter("nimbus:num-shutdown-calls");
        this.processWorkerMetricsCalls = metricsRegistry.registerMeter("nimbus:process-worker-metric-calls");
        this.mkAssignmentsErrors = metricsRegistry.registerMeter("nimbus:mkAssignments-Errors");
        this.sendAssignmentExceptions = metricsRegistry.registerMeter(Constants.NIMBUS_SEND_ASSIGNMENT_EXCEPTIONS);
        this.fileUploadDuration = metricsRegistry.registerTimer("nimbus:files-upload-duration-ms");
        this.schedulingDuration = metricsRegistry.registerTimer("nimbus:topology-scheduling-duration-ms");
        this.numAddedExecPerScheduling = metricsRegistry.registerHistogram("nimbus:num-added-executors-per-scheduling");
        this.numAddedSlotPerScheduling = metricsRegistry.registerHistogram("nimbus:num-added-slots-per-scheduling");
        this.numRemovedExecPerScheduling = metricsRegistry.registerHistogram("nimbus:num-removed-executors-per-scheduling");
        this.numRemovedSlotPerScheduling = metricsRegistry.registerHistogram("nimbus:num-removed-slots-per-scheduling");
        this.numNetExecIncreasePerScheduling = metricsRegistry.registerHistogram("nimbus:num-net-executors-increase-per-scheduling");
        this.numNetSlotIncreasePerScheduling = metricsRegistry.registerHistogram("nimbus:num-net-slots-increase-per-scheduling");

        this.metricsStore = null;
        try {
            this.metricsStore = MetricStoreConfig.configure(conf, metricsRegistry);
        } catch (Exception e) {
            // the metrics store is not critical to the operation of the cluster, allow Nimbus to come up
            LOG.error("Failed to initialize metric store", e);
        }

        if (hostPortInfo == null) {
            hostPortInfo = NimbusInfo.fromConf(conf);
        }
        this.nimbusHostPortInfo = hostPortInfo;
        if (inimbus != null) {
            inimbus.prepare(conf, ServerConfigUtils.masterInimbusDir(conf));
        }

        this.inimbus = inimbus;
        this.authorizationHandler = StormCommon.mkAuthorizationHandler((String) conf.get(DaemonConfig.NIMBUS_AUTHORIZER), conf);
        this.impersonationAuthorizationHandler =
            StormCommon.mkAuthorizationHandler((String) conf.get(DaemonConfig.NIMBUS_IMPERSONATION_AUTHORIZER), conf);
        this.submittedCount = new AtomicLong(0);
        if (stormClusterState == null) {
            stormClusterState = makeStormClusterState(conf);
        }
        this.stormClusterState = stormClusterState;
        this.heartbeatsCache = new HeartbeatCache();
        this.heartbeatsReadyFlag = new AtomicBoolean(false);
        this.heartbeatsRecoveryStrategy = WorkerHeartbeatsRecoveryStrategyFactory.getStrategy(conf);
        this.downloaders = fileCacheMap(conf);
        this.uploaders = fileCacheMap(conf);
        this.blobDownloaders = makeBlobCacheMap(conf);
        this.blobUploaders = makeBlobCacheMap(conf);
        this.blobListers = makeBlobListCacheMap(conf);
        this.uptime = Utils.makeUptimeComputer();
        this.validator = ReflectionUtils
            .newInstance((String) conf.getOrDefault(DaemonConfig.NIMBUS_TOPOLOGY_VALIDATOR, DefaultTopologyValidator.class.getName()));
        this.timer = new StormTimer(null, (t, e) -> {
            LOG.error("Error while processing event", e);
            Utils.exitProcess(20, "Error while processing event");
        });
        this.underlyingScheduler = makeScheduler(conf, inimbus);
        this.scheduler = wrapAsBlacklistScheduler(conf, underlyingScheduler, metricsRegistry);
        this.zkClient = makeZKClient(conf);
        this.idToExecutors = new AtomicReference<>(new HashMap<>());

        if (blobStore == null) {
            blobStore = ServerUtils.getNimbusBlobStore(conf, this.nimbusHostPortInfo, null);
        }
        this.blobStore = blobStore;

        if (topoCache == null) {
            topoCache = new TopoCache(blobStore, conf);
        }
        if (leaderElector == null) {
            leaderElector = Zookeeper.zkLeaderElector(conf, zkClient, blobStore, topoCache, stormClusterState, getNimbusAcls(conf),
                metricsRegistry);
        }
        this.leaderElector = leaderElector;
        this.blobStore.setLeaderElector(this.leaderElector);

        this.topoCache = topoCache;
        this.assignmentsDistributer = AssignmentDistributionService.getInstance(conf, this.scheduler);
        this.idToSchedStatus = new AtomicReference<>(new HashMap<>());
        this.nodeIdToResources = new AtomicReference<>(new HashMap<>());
        this.idToResources = new AtomicReference<>(new HashMap<>());
        this.idToWorkerResources = new AtomicReference<>(new HashMap<>());
        this.credRenewers = ClientAuthUtils.getCredentialRenewers(conf);
        this.topologyHistoryLock = new Object();
        this.topologyHistoryState = ServerConfigUtils.nimbusTopoHistoryState(conf);
        this.nimbusAutocredPlugins = ClientAuthUtils.getNimbusAutoCredPlugins(conf);
        this.nimbusTopologyActionNotifier = createTopologyActionNotifier(conf);
        this.clusterConsumerExceutors = makeClusterMetricsConsumerExecutors(conf);
        if (groupMapper == null) {
            groupMapper = ClientAuthUtils.getGroupMappingServiceProviderPlugin(conf);
        }
        this.groupMapper = groupMapper;
        this.principalToLocal = ClientAuthUtils.getPrincipalToLocalPlugin(conf);
        // We don't use the classpath part of this, so just an empty list
        this.supervisorClasspaths = Collections.unmodifiableNavigableMap(Utils.getConfiguredClasspathVersions(conf, EMPTY_STRING_LIST));
        clusterMetricSet = new ClusterSummaryMetricSet(metricsRegistry);
    }

    // TOPOLOGY STATE TRANSITIONS
    private static StormBase make(TopologyStatus status) {
        StormBase ret = new StormBase();
        ret.set_status(status);
        //The following are required for backwards compatibility with clojure code
        ret.set_component_executors(Collections.emptyMap());
        ret.set_component_debug(Collections.emptyMap());
        return ret;
    }

    @SuppressWarnings("deprecation")
    private static <T extends AutoCloseable> TimeCacheMap<String, T> fileCacheMap(Map<String, Object> conf) {
        return new TimeCacheMap<>(ObjectReader.getInt(conf.get(DaemonConfig.NIMBUS_FILE_COPY_EXPIRATION_SECS), 600),
            (id, stream) -> {
                try {
                    stream.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
    }

    //Not symmetric difference. Performing A.entrySet() - B.entrySet()
    private static <K, V> Map<K, V> mapDiff(Map<? extends K, ? extends V> first, Map<? extends K, ? extends V> second) {
        Map<K, V> ret = new HashMap<>();
        for (Entry<? extends K, ? extends V> entry : second.entrySet()) {
            if (!entry.getValue().equals(first.get(entry.getKey()))) {
                ret.put(entry.getKey(), entry.getValue());
            }
        }
        return ret;
    }

    private static IScheduler wrapAsBlacklistScheduler(Map<String, Object> conf, IScheduler scheduler,
        StormMetricsRegistry metricsRegistry) {
        BlacklistScheduler blacklistWrappedScheduler = new BlacklistScheduler(scheduler);
        blacklistWrappedScheduler.prepare(conf, metricsRegistry);
        return blacklistWrappedScheduler;
    }

    private static IScheduler makeScheduler(Map<String, Object> conf, INimbus inimbus) {
        String schedClass = (String) conf.get(DaemonConfig.STORM_SCHEDULER);
        IScheduler scheduler = inimbus == null ? null : inimbus.getForcedScheduler();
        if (scheduler != null) {
            LOG.info("Using forced scheduler from INimbus {} {}", scheduler.getClass(), scheduler);
        } else if (schedClass != null) {
            LOG.info("Using custom scheduler: {}", schedClass);
            scheduler = ReflectionUtils.newInstance(schedClass);
        } else {
            LOG.info("Using default scheduler");
            scheduler = new DefaultScheduler();
        }
        return scheduler;
    }

    /**
     * Constructs a TimeCacheMap instance with a blob store timeout whose expiration callback invokes cancel on the value held by an expired
     * entry when that value is an AtomicOutputStream and calls close otherwise.
     *
     * @param conf the config to use
     * @return the newly created map
     */
    @SuppressWarnings("deprecation")
    private static <T extends AutoCloseable> TimeCacheMap<String, T> makeBlobCacheMap(Map<String, Object> conf) {
        return new TimeCacheMap<>(ObjectReader.getInt(conf.get(DaemonConfig.NIMBUS_BLOBSTORE_EXPIRATION_SECS), 600),
            (id, stream) -> {
                try {
                    if (stream instanceof AtomicOutputStream) {
                        ((AtomicOutputStream) stream).cancel();
                    } else {
                        stream.close();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
    }

    /**
     * Constructs a TimeCacheMap instance with a blobstore timeout and no callback function.
     *
     * @param conf the config to use
     * @return the newly created TimeCacheMap
     */
    @SuppressWarnings("deprecation")
    private static TimeCacheMap<String, Iterator<String>> makeBlobListCacheMap(Map<String, Object> conf) {
        return new TimeCacheMap<>(ObjectReader.getInt(conf.get(DaemonConfig.NIMBUS_BLOBSTORE_EXPIRATION_SECS), 600));
    }

    private static ITopologyActionNotifierPlugin createTopologyActionNotifier(Map<String, Object> conf) {
        String clazz = (String) conf.get(DaemonConfig.NIMBUS_TOPOLOGY_ACTION_NOTIFIER_PLUGIN);
        ITopologyActionNotifierPlugin ret = null;
        if (clazz != null && !clazz.isEmpty()) {
            ret = ReflectionUtils.newInstance(clazz);
            try {
                ret.prepare(conf);
            } catch (Exception e) {
                LOG.warn("Ignoring exception, Could not initialize {}", clazz, e);
                ret = null;
            }
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    private static List<ClusterMetricsConsumerExecutor> makeClusterMetricsConsumerExecutors(Map<String, Object> conf) {
        Collection<Map<String, Object>> consumers = (Collection<Map<String, Object>>) conf.get(
            DaemonConfig.STORM_CLUSTER_METRICS_CONSUMER_REGISTER);
        List<ClusterMetricsConsumerExecutor> ret = new ArrayList<>();
        if (consumers != null) {
            for (Map<String, Object> consumer : consumers) {
                ret.add(new ClusterMetricsConsumerExecutor((String) consumer.get("class"), consumer.get("argument")));
            }
        }
        return ret;
    }

    private static Subject getSubject() {
        return ReqContext.context().subject();
    }

    static Map<String, Object> readTopoConf(String topoId, TopoCache tc) throws KeyNotFoundException,
        AuthorizationException, IOException {
        return tc.readTopoConf(topoId, getSubject());
    }

    static List<String> getKeyListFromId(Map<String, Object> conf, String id) {
        List<String> ret = new ArrayList<>(3);
        ret.add(ConfigUtils.masterStormCodeKey(id));
        ret.add(ConfigUtils.masterStormConfKey(id));
        if (!ConfigUtils.isLocalMode(conf)) {
            ret.add(ConfigUtils.masterStormJarKey(id));
        }
        return ret;
    }

    public static int getVersionForKey(String key, NimbusInfo nimbusInfo,
        CuratorFramework zkClient) throws KeyNotFoundException {
        KeySequenceNumber kseq = new KeySequenceNumber(key, nimbusInfo);
        return kseq.getKeySequenceNumber(zkClient);
    }

    private static StormTopology readStormTopology(String topoId, TopoCache tc) throws KeyNotFoundException, AuthorizationException,
        IOException {
        return tc.readTopology(topoId, getSubject());
    }

    private static Map<String, Object> readTopoConfAsNimbus(String topoId, TopoCache tc) throws KeyNotFoundException,
        AuthorizationException, IOException {
        return tc.readTopoConf(topoId, NIMBUS_SUBJECT);
    }

    private static StormTopology readStormTopologyAsNimbus(String topoId, TopoCache tc) throws KeyNotFoundException,
        AuthorizationException, IOException {
        return tc.readTopology(topoId, NIMBUS_SUBJECT);
    }

    /**
     * convert {topology-id -> SchedulerAssignment} to {topology-id -> {executor [node port]}}.
     *
     * @return {topology-id -> {executor [node port]}} mapping
     */
    private static Map<String, Map<List<Long>, List<Object>>> computeTopoToExecToNodePort(
        Map<String, SchedulerAssignment> schedAssignments, List<String> assignedTopologyIds) {
        Map<String, Map<List<Long>, List<Object>>> ret = new HashMap<>();
        for (Entry<String, SchedulerAssignment> schedEntry : schedAssignments.entrySet()) {
            Map<List<Long>, List<Object>> execToNodePort = new HashMap<>();
            for (Entry<ExecutorDetails, WorkerSlot> execAndNodePort : schedEntry.getValue().getExecutorToSlot().entrySet()) {
                ExecutorDetails exec = execAndNodePort.getKey();
                WorkerSlot slot = execAndNodePort.getValue();
                execToNodePort.put(exec.toList(), slot.toList());
            }
            ret.put(schedEntry.getKey(), execToNodePort);
        }
        for (String id : assignedTopologyIds) {
            ret.putIfAbsent(id, null);
        }
        return ret;
    }

    private static int numUsedWorkers(SchedulerAssignment assignment) {
        if (assignment == null) {
            return 0;
        }
        return assignment.getSlots().size();
    }

    /**
     * Convert {topology-id -> SchedulerAssignment} to {topology-id -> {WorkerSlot WorkerResources}}. Make sure this can deal with other
     * non-RAS schedulers later we may further support map-for-any-resources.
     *
     * @param schedAssignments the assignments
     * @return The resources used per slot
     */
    private static Map<String, Map<WorkerSlot, WorkerResources>> computeTopoToNodePortToResources(
        Map<String, SchedulerAssignment> schedAssignments) {
        Map<String, Map<WorkerSlot, WorkerResources>> ret = new HashMap<>();
        for (Entry<String, SchedulerAssignment> schedEntry : schedAssignments.entrySet()) {
            ret.put(schedEntry.getKey(), schedEntry.getValue().getScheduledResources());
        }
        return ret;
    }

    private boolean auditAssignmentChanges(Map<String, Assignment> existingAssignments,
                                           Map<String, Assignment> newAssignments) {
        assert existingAssignments != null && newAssignments != null;
        boolean anyChanged = existingAssignments.isEmpty() ^ newAssignments.isEmpty();
        long numRemovedExec = 0;
        long numRemovedSlot = 0;
        long numAddedExec   = 0;
        long numAddedSlot   = 0;
        if (existingAssignments.isEmpty()) {
            for (Entry<String, Assignment> entry : newAssignments.entrySet()) {
                final Map<List<Long>, NodeInfo> execToPort = entry.getValue().get_executor_node_port();
                final long count = new HashSet<>(execToPort.values()).size();
                LOG.info("Assigning {} to {} slots", entry.getKey(), count);
                LOG.info("Assign executors: {}", execToPort.keySet());
                numAddedSlot += count;
                numAddedExec += execToPort.size();
            }
        } else if (newAssignments.isEmpty()) {
            for (Entry<String, Assignment> entry : existingAssignments.entrySet()) {
                final Map<List<Long>, NodeInfo> execToPort = entry.getValue().get_executor_node_port();
                final long count = new HashSet<>(execToPort.values()).size();
                LOG.info("Removing {} from {} slots", entry.getKey(), count);
                LOG.info("Remove executors: {}", execToPort.keySet());
                numRemovedSlot += count;
                numRemovedExec += execToPort.size();
            }
        } else {
            MapDifference<String, Assignment> difference = Maps.difference(existingAssignments, newAssignments);
            if (anyChanged = !difference.areEqual()) {
                for (Entry<String, Assignment> entry : difference.entriesOnlyOnLeft().entrySet()) {
                    final Map<List<Long>, NodeInfo> execToPort = entry.getValue().get_executor_node_port();
                    final long count = new HashSet<>(execToPort.values()).size();
                    LOG.info("Removing {} from {} slots", entry.getKey(), count);
                    LOG.info("Remove executors: {}", execToPort.keySet());
                    numRemovedSlot += count;
                    numRemovedExec += execToPort.size();
                }
                for (Entry<String, Assignment> entry : difference.entriesOnlyOnRight().entrySet()) {
                    final Map<List<Long>, NodeInfo> execToPort = entry.getValue().get_executor_node_port();
                    final long count = new HashSet<>(execToPort.values()).size();
                    LOG.info("Assigning {} to {} slots", entry.getKey(), count);
                    LOG.info("Assign executors: {}", execToPort.keySet());
                    numAddedSlot += count;
                    numAddedExec += execToPort.size();
                }
                for (Entry<String, MapDifference.ValueDifference<Assignment>> entry : difference.entriesDiffering().entrySet()) {
                    final Map<List<Long>, NodeInfo> execToSlot = entry.getValue().rightValue().get_executor_node_port();
                    final Set<NodeInfo> slots = new HashSet<>(execToSlot.values());
                    LOG.info("Reassigning {} to {} slots", entry.getKey(), slots.size());
                    LOG.info("Reassign executors: {}", execToSlot.keySet());

                    final Map<List<Long>, NodeInfo> oldExecToSlot = entry.getValue().leftValue().get_executor_node_port();

                    long commonExecCount = 0;
                    Set<NodeInfo> commonSlots = new HashSet<>(execToSlot.size());
                    for (Entry<List<Long>, NodeInfo> execEntry : execToSlot.entrySet()) {
                        if (execEntry.getValue().equals(oldExecToSlot.get(execEntry.getKey()))) {
                            commonExecCount++;
                            commonSlots.add(execEntry.getValue());
                        }
                    }
                    long commonSlotCount = commonSlots.size();

                    //Treat reassign as remove and add
                    numRemovedSlot += new HashSet<>(oldExecToSlot.values()).size() - commonSlotCount;
                    numRemovedExec += oldExecToSlot.size() - commonExecCount;
                    numAddedSlot += slots.size() - commonSlotCount;
                    numAddedExec += execToSlot.size() - commonExecCount;
                }
            }
            LOG.debug("{} assignments unchanged: {}", difference.entriesInCommon().size(), difference.entriesInCommon().keySet());
        }
        numAddedExecPerScheduling.update(numAddedExec);
        numAddedSlotPerScheduling.update(numAddedSlot);
        numRemovedExecPerScheduling.update(numRemovedExec);
        numRemovedSlotPerScheduling.update(numRemovedSlot);
        numNetExecIncreasePerScheduling.update(numAddedExec - numRemovedExec);
        numNetSlotIncreasePerScheduling.update(numAddedSlot - numRemovedSlot);

        if (anyChanged) {
            LOG.info("Fragmentation after scheduling is: {} MB, {} PCore CPUs", fragmentedMemory(), fragmentedCpu());
            nodeIdToResources.get().forEach((id, node) -> {
                final double availableMem = node.getAvailableMem();
                if (availableMem < 0) {
                    LOG.warn("Memory over-scheduled on {}", id, availableMem);
                }
                final double availableCpu = node.getAvailableCpu();
                if (availableCpu < 0) {
                    LOG.warn("CPU over-scheduled on {}", id, availableCpu);
                }
                LOG.info(
                    "Node Id: {} Total Mem: {}, Used Mem: {}, Available Mem: {}, Total CPU: {}, Used "
                        + "CPU: {}, Available CPU: {}, fragmented: {}",
                        id, node.getTotalMem(), node.getUsedMem(), availableMem,
                        node.getTotalCpu(), node.getUsedCpu(), availableCpu, isFragmented(node));
            });
        }
        return anyChanged;
    }

    private static List<List<Long>> changedExecutors(Map<List<Long>, NodeInfo> map, Map<List<Long>,
        List<Object>> newExecToNodePort) {
        HashMap<NodeInfo, List<List<Long>>> tmpSlotAssigned = map == null ? new HashMap<>() : Utils.reverseMap(map);
        HashMap<List<Object>, List<List<Long>>> slotAssigned = new HashMap<>();
        for (Entry<NodeInfo, List<List<Long>>> entry : tmpSlotAssigned.entrySet()) {
            NodeInfo ni = entry.getKey();
            List<Object> key = new ArrayList<>(2);
            key.add(ni.get_node());
            key.add(ni.get_port_iterator().next());
            List<List<Long>> value = new ArrayList<>(entry.getValue());
            value.sort(Comparator.comparing(a -> a.get(0)));
            slotAssigned.put(key, value);
        }
        HashMap<List<Object>, List<List<Long>>> tmpNewSlotAssigned = newExecToNodePort == null ? new HashMap<>() :
            Utils.reverseMap(newExecToNodePort);
        HashMap<List<Object>, List<List<Long>>> newSlotAssigned = new HashMap<>();
        for (Entry<List<Object>, List<List<Long>>> entry : tmpNewSlotAssigned.entrySet()) {
            List<List<Long>> value = new ArrayList<>(entry.getValue());
            value.sort(Comparator.comparing(a -> a.get(0)));
            newSlotAssigned.put(entry.getKey(), value);
        }
        Map<List<Object>, List<List<Long>>> diff = mapDiff(slotAssigned, newSlotAssigned);
        List<List<Long>> ret = new ArrayList<>();
        for (List<List<Long>> val : diff.values()) {
            ret.addAll(val);
        }
        return ret;
    }

    private static Set<WorkerSlot> newlyAddedSlots(Assignment old, Assignment current) {
        Set<NodeInfo> oldSlots = new HashSet<>(old.get_executor_node_port().values());
        Set<NodeInfo> niRet = new HashSet<>(current.get_executor_node_port().values());
        niRet.removeAll(oldSlots);
        Set<WorkerSlot> ret = new HashSet<>();
        for (NodeInfo ni : niRet) {
            ret.add(new WorkerSlot(ni.get_node(), ni.get_port_iterator().next()));
        }
        return ret;
    }

    private static Map<String, SupervisorDetails> basicSupervisorDetailsMap(IStormClusterState state) {
        Map<String, SupervisorDetails> ret = new HashMap<>();
        for (Entry<String, SupervisorInfo> entry : state.allSupervisorInfo().entrySet()) {
            String id = entry.getKey();
            SupervisorInfo info = entry.getValue();
            ret.put(id, new SupervisorDetails(id, info.get_server_port(), info.get_hostname(),
                                              info.get_scheduler_meta(), null, info.get_resources_map()));
        }
        return ret;
    }

    /**
     * NOTE: this can return false when a topology has just been activated.  The topology may still be
     * in the STORMS_SUBTREE.
     */
    private static boolean isTopologyActive(IStormClusterState state, String topoName) {
        return state.getTopoId(topoName).isPresent();
    }

    private static boolean isTopologyActiveOrActivating(IStormClusterState state, String topoName) {
        return isTopologyActive(state, topoName) || state.activeStorms().contains(topoName);
    }

    /**
     * Returns the topologyId of the topology using this blob.
     * @param state the cluster state
     * @param topoCache the topology cache
     * @param key the blob key
     * @return null or id
     */
    private static String topologyUsingThisBlob(IStormClusterState state, TopoCache topoCache, String key) {
        for (String topologyId : state.activeStorms()) {
            Map<String, Object> topoConf = null;
            try {
                topoConf = readTopoConfAsNimbus(topologyId, topoCache);
            } catch (KeyNotFoundException | AuthorizationException | IOException e) {
                continue;
            }
            if (topoConf == null) {
                continue;
            }
            @SuppressWarnings("unchecked")
            Map<String, Map<String, Object>> blobstoreMap = (Map<String, Map<String, Object>>) topoConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
            if (blobstoreMap != null && blobstoreMap.containsKey(key)) {
                return topologyId;
            }
        }
        return null;
    }

    private static Map<String, Object> tryReadTopoConf(String topoId, TopoCache tc)
        throws NotAliveException, AuthorizationException, IOException {
        try {
            return readTopoConfAsNimbus(topoId, tc);
            //Was a try-cause but I looked at the code around this and key not found is not wrapped in runtime,
            // so it is not needed
        } catch (KeyNotFoundException e) {
            if (topoId == null) {
                throw new NullPointerException();
            }
            throw new WrappedNotAliveException(topoId);
        }
    }

    private static void rotateTopologyCleanupMap(long deletionDelay) {
        if (Time.currentTimeMillis() - topologyCleanupRotationTime > deletionDelay) {
            topologyCleanupDetected.rotate();
            topologyCleanupRotationTime = Time.currentTimeMillis();
        }
    }

    private static long getTopologyCleanupDetectedTime(String topologyId) {
        Long firstDetectedForDeletion = topologyCleanupDetected.get(topologyId);
        if (firstDetectedForDeletion == null) {
            firstDetectedForDeletion = Time.currentTimeMillis();
            topologyCleanupDetected.put(topologyId, firstDetectedForDeletion);
        }
        return firstDetectedForDeletion;
    }

    /**
     * From a set of topologies that have been found to cleanup, return a set that has been detected for a minimum
     * amount of time. Topology entries first detected less than NIMBUS_TOPOLOGY_BLOBSTORE_DELETION_DELAY_MS ago are
     * ignored. The delay is to prevent a race conditions such as when a blobstore is created and when the topology
     * is submitted. It is possible the Nimbus cleanup timer task will find entries to delete between these two events.
     *
     * <p>Tracked topology entries are rotated out of the stored map periodically.
     *
     * @param toposToClean topologies considered for cleanup
     * @param conf the nimbus conf
     * @return the set of topologies that have been detected for cleanup past the expiration time
     */
    static Set<String> getExpiredTopologyIds(Set<String> toposToClean, Map<String, Object> conf) {
        Set<String> idleTopologies = new HashSet<>();
        long topologyDeletionDelay = ObjectReader.getInt(
                conf.get(DaemonConfig.NIMBUS_TOPOLOGY_BLOBSTORE_DELETION_DELAY_MS), 5 * 60 * 1000);
        for (String topologyId : toposToClean) {
            if (Math.max(0, Time.currentTimeMillis() - getTopologyCleanupDetectedTime(topologyId)) >= topologyDeletionDelay) {
                idleTopologies.add(topologyId);
            }
        }

        rotateTopologyCleanupMap(topologyDeletionDelay);

        return idleTopologies;
    }

    @VisibleForTesting
    public static Set<String> topoIdsToClean(IStormClusterState state, BlobStore store, Map<String, Object> conf) {
        Set<String> ret = new HashSet<>();
        ret.addAll(Utils.OR(state.heartbeatStorms(), EMPTY_STRING_LIST));
        ret.addAll(Utils.OR(state.errorTopologies(), EMPTY_STRING_LIST));
        ret.addAll(Utils.OR(store.storedTopoIds(), EMPTY_STRING_SET));
        ret.addAll(Utils.OR(state.backpressureTopologies(), EMPTY_STRING_LIST));
        ret.addAll(Utils.OR(state.idsOfTopologiesWithPrivateWorkerKeys(), EMPTY_STRING_SET));
        ret = getExpiredTopologyIds(ret, conf);
        ret.removeAll(Utils.OR(state.activeStorms(), EMPTY_STRING_LIST));
        return ret;
    }

    private static String extractStatusStr(StormBase base) {
        String ret = null;
        if (base != null) {
            TopologyStatus status = base.get_status();
            if (status != null) {
                ret = status.name().toUpperCase();
            }
        }
        return ret;
    }

    private static StormTopology normalizeTopology(Map<String, Object> topoConf, StormTopology topology)
        throws InvalidTopologyException {
        StormTopology ret = topology.deepCopy();
        for (Object comp : StormCommon.allComponents(ret).values()) {
            Map<String, Object> mergedConf = StormCommon.componentConf(comp);
            mergedConf.put(Config.TOPOLOGY_TASKS, ServerUtils.getComponentParallelism(topoConf, comp));
            String jsonConf = JSONValue.toJSONString(mergedConf);
            StormCommon.getComponentCommon(comp).set_json_conf(jsonConf);
        }
        return ret;
    }

    private static void addToDecorators(Set<String> decorators, List<String> conf) {
        if (conf != null) {
            decorators.addAll(conf);
        }
    }

    @SuppressWarnings("unchecked")
    private static void addToSerializers(Map<String, String> ser, List<Object> conf) {
        if (conf != null) {
            for (Object o : conf) {
                if (o instanceof Map) {
                    ser.putAll((Map<String, String>) o);
                } else {
                    ser.put((String) o, null);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    /**
     * Create a normalized topology conf.
     *
     * @param conf  the nimbus conf
     * @param topoConf initial topology conf
     * @param topology  the Storm topology
     */
    private static Map<String, Object> normalizeConf(Map<String, Object> conf, Map<String, Object> topoConf, StormTopology topology) {
        //ensure that serializations are same for all tasks no matter what's on
        // the supervisors. this also allows you to declare the serializations as a sequence
        List<Map<String, Object>> allConfs = new ArrayList<>();
        for (Object comp : StormCommon.allComponents(topology).values()) {
            allConfs.add(StormCommon.componentConf(comp));
        }

        Set<String> decorators = new HashSet<>();
        //Yes we are putting in a config that is not the same type we pulled out.
        Map<String, String> serializers = new HashMap<>();
        for (Map<String, Object> c : allConfs) {
            addToDecorators(decorators, (List<String>) c.get(Config.TOPOLOGY_KRYO_DECORATORS));
            addToSerializers(serializers, (List<Object>) c.get(Config.TOPOLOGY_KRYO_REGISTER));
        }
        addToDecorators(decorators, (List<String>) topoConf.getOrDefault(Config.TOPOLOGY_KRYO_DECORATORS,
                                                                         conf.get(Config.TOPOLOGY_KRYO_DECORATORS)));
        addToSerializers(serializers, (List<Object>) topoConf.getOrDefault(Config.TOPOLOGY_KRYO_REGISTER,
                                                                           conf.get(Config.TOPOLOGY_KRYO_REGISTER)));

        Map<String, Object> mergedConf = Utils.merge(conf, topoConf);
        Map<String, Object> ret = new HashMap<>(topoConf);
        ret.put(Config.TOPOLOGY_KRYO_REGISTER, serializers);
        ret.put(Config.TOPOLOGY_KRYO_DECORATORS, new ArrayList<>(decorators));
        ret.put(Config.TOPOLOGY_ACKER_EXECUTORS, mergedConf.get(Config.TOPOLOGY_ACKER_EXECUTORS));
        ret.put(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, mergedConf.get(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS));
        ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, mergedConf.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM));

        // storm.messaging.netty.authentication is about inter-worker communication
        // enforce netty authentication when either topo or daemon set it to true
        boolean enforceNettyAuth = false;
        if (!topoConf.containsKey(Config.STORM_MESSAGING_NETTY_AUTHENTICATION)) {
            enforceNettyAuth = (Boolean) conf.get(Config.STORM_MESSAGING_NETTY_AUTHENTICATION);
        } else {
            enforceNettyAuth = (Boolean) topoConf.get(Config.STORM_MESSAGING_NETTY_AUTHENTICATION)
                                || (Boolean) conf.get(Config.STORM_MESSAGING_NETTY_AUTHENTICATION);
        }
        LOG.debug("For netty authentication, topo conf is: {}, cluster conf is: {}, Enforce netty auth: {}",
            topoConf.get(Config.STORM_MESSAGING_NETTY_AUTHENTICATION),
            conf.get(Config.STORM_MESSAGING_NETTY_AUTHENTICATION),
            enforceNettyAuth);
        ret.put(Config.STORM_MESSAGING_NETTY_AUTHENTICATION, enforceNettyAuth);

        if (!mergedConf.containsKey(Config.TOPOLOGY_METRICS_REPORTERS) && mergedConf.containsKey(Config.STORM_METRICS_REPORTERS)) {
            ret.put(Config.TOPOLOGY_METRICS_REPORTERS, mergedConf.get(Config.STORM_METRICS_REPORTERS));
        }

        // add any system metrics reporters to the topology metrics reporters
        if (conf.containsKey(Config.STORM_TOPOLOGY_METRICS_SYSTEM_REPORTERS)) {
            List<Map<String, Object>> reporters = (List<Map<String, Object>>)
                    ret.computeIfAbsent(Config.TOPOLOGY_METRICS_REPORTERS, (key) -> new ArrayList<>());
            List<Map<String, Object>> systemReporters = (List<Map<String, Object>>)
                    conf.get(Config.STORM_TOPOLOGY_METRICS_SYSTEM_REPORTERS);
            reporters.addAll(systemReporters);
        }

        // Don't allow topoConf to override various cluster-specific properties.
        // Specifically adding the cluster settings to the topoConf here will make sure these settings
        // also override the subsequently generated conf picked up locally on the classpath.
        //
        // We will be dealing with 3 confs:
        // 1) the submitted topoConf created here
        // 2) the combined classpath conf with the topoConf added on top
        // 3) the nimbus conf with conf 2 above added on top.
        //
        // By first forcing the topology conf to contain the nimbus settings, we guarantee all three confs
        // will have the correct settings that cannot be overriden by the submitter.
        ret.put(Config.STORM_CGROUP_HIERARCHY_DIR, conf.get(Config.STORM_CGROUP_HIERARCHY_DIR));
        ret.put(Config.WORKER_METRICS, conf.get(Config.WORKER_METRICS));

        if (mergedConf.containsKey(Config.TOPOLOGY_WORKER_TIMEOUT_SECS)) {
            int workerTimeoutSecs = (Integer) ObjectReader.getInt(mergedConf.get(Config.TOPOLOGY_WORKER_TIMEOUT_SECS));
            int workerMaxTimeoutSecs = (Integer) ObjectReader.getInt(mergedConf.get(Config.WORKER_MAX_TIMEOUT_SECS));
            if (workerTimeoutSecs > workerMaxTimeoutSecs) {
                ret.put(Config.TOPOLOGY_WORKER_TIMEOUT_SECS, workerMaxTimeoutSecs);
                String topoId = (String) mergedConf.get(Config.STORM_ID);
                LOG.warn("Topology {} topology.worker.timeout.secs is too large. Reducing from {} to {}",
                    topoId, workerTimeoutSecs, workerMaxTimeoutSecs);
            }
        }
        return ret;
    }

    private static void rmBlobKey(BlobStore store, String key, IStormClusterState state) {
        try {
            store.deleteBlob(key, NIMBUS_SUBJECT);
        } catch (Exception e) {
            //Yes eat the exception
            LOG.info("Exception {}", e);
        }
    }

    /**
     * Deletes jar files in dirLoc older than seconds.
     *
     * @param dirLoc  the location to look in for file
     * @param seconds how old is too old and should be deleted
     */
    @VisibleForTesting
    public static void cleanInbox(String dirLoc, int seconds) {
        final long now = Time.currentTimeMillis();
        final long ms = Time.secsToMillis(seconds);
        File dir = new File(dirLoc);
        for (File f : dir.listFiles((file) -> file.isFile() && ((file.lastModified() + ms) <= now))) {
            if (f.delete()) {
                LOG.info("Cleaning inbox ... deleted: {}", f.getName());
            } else {
                LOG.error("Cleaning inbox ... error deleting: {}", f.getName());
            }
        }
    }

    private static ExecutorInfo toExecInfo(List<Long> exec) {
        return new ExecutorInfo(exec.get(0).intValue(), exec.get(1).intValue());
    }

    private static void validateTopologyName(String name) throws InvalidTopologyException {
        try {
            Utils.validateTopologyName(name);
        } catch (IllegalArgumentException e) {
            throw new WrappedInvalidTopologyException(e.getMessage());
        }
    }

    private static StormTopology tryReadTopology(String topoId, TopoCache tc)
        throws NotAliveException, AuthorizationException, IOException {
        try {
            return readStormTopologyAsNimbus(topoId, tc);
        } catch (KeyNotFoundException e) {
            throw new WrappedNotAliveException(topoId);
        }
    }

    private static void validateTopologySize(Map<String, Object> topoConf, Map<String, Object> nimbusConf,
        StormTopology topology) throws InvalidTopologyException {
        // check allowedWorkers only if the scheduler is not the Resource Aware Scheduler
        if (!ServerUtils.isRas(nimbusConf)) {
            int workerCount = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_WORKERS), 1);
            Integer allowedWorkers = ObjectReader.getInt(nimbusConf.get(DaemonConfig.NIMBUS_SLOTS_PER_TOPOLOGY), null);
            if (allowedWorkers != null && workerCount > allowedWorkers) {
                throw new WrappedInvalidTopologyException("Failed to submit topology. Topology requests more than "
                        + allowedWorkers + " workers.");
            }
        }
        int executorsCount = 0;
        for (Object comp : StormCommon.allComponents(topology).values()) {
            executorsCount += StormCommon.numStartExecutors(comp);
        }
        Integer allowedExecutors = ObjectReader.getInt(nimbusConf.get(DaemonConfig.NIMBUS_EXECUTORS_PER_TOPOLOGY), null);
        if (allowedExecutors != null && executorsCount > allowedExecutors) {
            throw new WrappedInvalidTopologyException("Failed to submit topology. Topology requests more than "
                    + allowedExecutors + " executors.");
        }
    }

    private static void setLoggerTimeouts(LogLevel level) {
        int timeoutSecs = level.get_reset_log_level_timeout_secs();
        if (timeoutSecs > 0) {
            level.set_reset_log_level_timeout_epoch(Time.currentTimeMillis() + Time.secsToMillis(timeoutSecs));
        } else {
            level.unset_reset_log_level_timeout_epoch();
        }
    }

    @VisibleForTesting
    public static List<String> topologiesOnSupervisor(Map<String, Assignment> assignments, String supervisorId) {
        Set<String> ret = new HashSet<>();
        for (Entry<String, Assignment> entry : assignments.entrySet()) {
            Assignment assignment = entry.getValue();
            for (NodeInfo nodeInfo : assignment.get_executor_node_port().values()) {
                if (supervisorId.equals(nodeInfo.get_node())) {
                    ret.add(entry.getKey());
                    break;
                }
            }
        }

        return new ArrayList<>(ret);
    }

    private static IClusterMetricsConsumer.ClusterInfo mkClusterInfo() {
        return new IClusterMetricsConsumer.ClusterInfo(Time.currentTimeSecs());
    }

    private static List<DataPoint> extractClusterMetrics(ClusterSummary summ) {
        List<DataPoint> ret = new ArrayList<>();
        ret.add(new DataPoint("supervisors", summ.get_supervisors_size()));
        ret.add(new DataPoint("topologies", summ.get_topologies_size()));

        int totalSlots = 0;
        int usedSlots = 0;
        for (SupervisorSummary sup : summ.get_supervisors()) {
            usedSlots += sup.get_num_used_workers();
            totalSlots += sup.get_num_workers();
        }
        ret.add(new DataPoint("slotsTotal", totalSlots));
        ret.add(new DataPoint("slotsUsed", usedSlots));
        ret.add(new DataPoint("slotsFree", totalSlots - usedSlots));

        int totalExecutors = 0;
        int totalTasks = 0;
        for (TopologySummary topo : summ.get_topologies()) {
            totalExecutors += topo.get_num_executors();
            totalTasks += topo.get_num_tasks();
        }
        ret.add(new DataPoint("executorsTotal", totalExecutors));
        ret.add(new DataPoint("tasksTotal", totalTasks));
        return ret;
    }

    private static Map<IClusterMetricsConsumer.SupervisorInfo, List<DataPoint>> extractSupervisorMetrics(ClusterSummary summ) {
        Map<IClusterMetricsConsumer.SupervisorInfo, List<DataPoint>> ret = new HashMap<>();
        for (SupervisorSummary sup : summ.get_supervisors()) {
            List<DataPoint> metrics = new ArrayList<>();
            metrics.add(new DataPoint("slotsTotal", sup.get_num_workers()));
            metrics.add(new DataPoint("slotsUsed", sup.get_num_used_workers()));
            metrics.add(new DataPoint("totalMem", sup.get_total_resources().get(Constants.COMMON_TOTAL_MEMORY_RESOURCE_NAME)));
            metrics.add(new DataPoint("totalCpu", sup.get_total_resources().get(Constants.COMMON_CPU_RESOURCE_NAME)));
            metrics.add(new DataPoint("usedMem", sup.get_used_mem()));
            metrics.add(new DataPoint("usedCpu", sup.get_used_cpu()));
            IClusterMetricsConsumer.SupervisorInfo info =
                    new IClusterMetricsConsumer.SupervisorInfo(sup.get_host(), sup.get_supervisor_id(), Time.currentTimeSecs());
            ret.put(info, metrics);
        }
        return ret;
    }

    private static void setResourcesDefaultIfNotSet(Map<String, NormalizedResourceRequest> compResourcesMap, String compId,
                                                    Map<String, Object> topoConf) {
        NormalizedResourceRequest resources = compResourcesMap.get(compId);
        if (resources == null) {
            compResourcesMap.put(compId, new NormalizedResourceRequest(topoConf, compId));
        }
    }

    private static void validatePortAvailable(Map<String, Object> conf) throws IOException {
        int port = ObjectReader.getInt(conf.get(Config.NIMBUS_THRIFT_PORT));
        try (ServerSocket socket = new ServerSocket(port)) {
            //Nothing
        } catch (BindException e) {
            LOG.error("{} is not available. Check if another process is already listening on {}", port, port);
            System.exit(0);
        }
    }

    @VisibleForTesting
    public void launchServer() throws Exception {
        try {
            IStormClusterState state = stormClusterState;
            NimbusInfo hpi = nimbusHostPortInfo;

            LOG.info("Starting Nimbus with conf {}", ConfigUtils.maskPasswords(conf));
            validator.prepare(conf);

            //add to nimbuses
            state.addNimbusHost(hpi.getHost(),
                    new NimbusSummary(hpi.getHost(), hpi.getPort(), Time.currentTimeSecs(), false, STORM_VERSION));
            leaderElector.addToLeaderLockQueue();
            this.blobStore.startSyncBlobs();

            for (ClusterMetricsConsumerExecutor exec: clusterConsumerExceutors) {
                exec.prepare();
            }

            // Leadership coordination may be incomplete when launchServer is called. Previous behavior did a one time check
            // which could cause Nimbus to not process TopologyActions.GAIN_LEADERSHIP transitions. Similar problem exists for
            // HA Nimbus on being newly elected as leader. Change to a recurring pattern addresses these problems.
            timer.scheduleRecurring(3, 5,
                () -> {
                    try {
                        boolean isLeader = isLeader();
                        if (isLeader && !wasLeader) {
                            for (String topoId : state.activeStorms()) {
                                transition(topoId, TopologyActions.GAIN_LEADERSHIP, null);
                            }
                            clusterMetricSet.setActive(true);
                        }
                        wasLeader = isLeader;
                    } catch (Exception e) {
                        throw  new RuntimeException(e);
                    }
                });

            final boolean doNotReassign = (Boolean) conf.getOrDefault(ServerConfigUtils.NIMBUS_DO_NOT_REASSIGN, false);
            timer.scheduleRecurring(0, ObjectReader.getInt(conf.get(DaemonConfig.NIMBUS_MONITOR_FREQ_SECS)),
                () -> {
                    try {
                        if (!doNotReassign) {
                            mkAssignments();
                        }
                        doCleanup();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

            // Schedule Nimbus inbox cleaner
            final int jarExpSecs = ObjectReader.getInt(conf.get(DaemonConfig.NIMBUS_INBOX_JAR_EXPIRATION_SECS));
            timer.scheduleRecurring(0, ObjectReader.getInt(conf.get(DaemonConfig.NIMBUS_CLEANUP_INBOX_FREQ_SECS)),
                () -> {
                    try {
                        cleanInbox(getInbox(), jarExpSecs);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });


            // Schedule topology history cleaner
            Integer interval = ObjectReader.getInt(conf.get(DaemonConfig.LOGVIEWER_CLEANUP_INTERVAL_SECS), null);
            if (interval != null) {
                final int lvCleanupAgeMins = ObjectReader.getInt(conf.get(DaemonConfig.LOGVIEWER_CLEANUP_AGE_MINS));
                timer.scheduleRecurring(0, interval,
                    () -> {
                        try {
                            cleanTopologyHistory(lvCleanupAgeMins);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            }

            timer.scheduleRecurring(0, ObjectReader.getInt(conf.get(DaemonConfig.NIMBUS_CREDENTIAL_RENEW_FREQ_SECS)),
                () -> {
                    try {
                        renewCredentials();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

            // Periodically make sure the blobstore update time is up to date.  This could have failed if Nimbus encountered
            // an exception updating the update time, or due to bugs causing a missed update of the blobstore mod time on a blob
            // update.
            timer.scheduleRecurring(30, ServerConfigUtils.getLocalizerUpdateBlobInterval(conf) * 5,
                () -> {
                    try {
                        blobStore.validateBlobUpdateTime();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

            metricsRegistry.registerGauge("nimbus:total-available-memory-non-negative", () -> nodeIdToResources.get().values()
                    .parallelStream()
                    .mapToDouble(supervisorResources -> Math.max(supervisorResources.getAvailableMem(), 0))
                    .sum());
            metricsRegistry.registerGauge("nimbus:available-cpu-non-negative", () -> nodeIdToResources.get().values()
                    .parallelStream()
                    .mapToDouble(supervisorResources -> Math.max(supervisorResources.getAvailableCpu(), 0))
                    .sum());
            metricsRegistry.registerGauge("nimbus:total-memory", () -> nodeIdToResources.get().values()
                    .parallelStream()
                    .mapToDouble(SupervisorResources::getTotalMem)
                    .sum());
            metricsRegistry.registerGauge("nimbus:total-cpu", () -> nodeIdToResources.get().values()
                    .parallelStream()
                    .mapToDouble(SupervisorResources::getTotalCpu)
                    .sum());
            metricsRegistry.registerGauge("nimbus:longest-scheduling-time-ms", () -> {
                //We want to update longest scheduling time in real time in case scheduler get stuck
                // Get current time before startTime to avoid potential race with scheduler's Timer
                Long currTime = Time.nanoTime();
                Long startTime = schedulingStartTimeNs.get();
                return TimeUnit.NANOSECONDS.toMillis(startTime == null
                        ? longestSchedulingTime.get()
                        : Math.max(currTime - startTime, longestSchedulingTime.get()));
            });
            metricsRegistry.registerMeter("nimbus:num-launched").mark();

            timer.scheduleRecurring(0, ObjectReader.getInt(conf.get(DaemonConfig.STORM_CLUSTER_METRICS_CONSUMER_PUBLISH_INTERVAL_SECS)),
                () -> {
                    try {
                        if (isLeader()) {
                            sendClusterMetricsToExecutors();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

            timer.scheduleRecurring(5, 5, clusterMetricSet);
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(InterruptedException.class, e)) {
                throw e;
            }

            if (Utils.exceptionCauseIsInstanceOf(InterruptedIOException.class, e)) {
                throw e;
            }
            LOG.error("Error on initialization of nimbus", e);
            Utils.exitProcess(13, "Error on initialization of nimbus");
        }
    }

    private static Nimbus launchServer(Map<String, Object> conf, INimbus inimbus) throws Exception {
        StormCommon.validateDistributedMode(conf);
        validatePortAvailable(conf);
        OciUtils.validateImageInDaemonConf(conf);
        StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
        final Nimbus nimbus = new Nimbus(conf, inimbus, metricsRegistry);
        nimbus.launchServer();
        final ThriftServer server = new ThriftServer(conf, new Processor<>(nimbus), ThriftConnectionType.NIMBUS);
        metricsRegistry.startMetricsReporters(conf);
        Utils.addShutdownHookWithDelayedForceKill(() -> {
            metricsRegistry.stopMetricsReporters();
            nimbus.shutdown();
            server.stop();
        }, 10);
        if (ClientAuthUtils.areWorkerTokensEnabledServer(server, conf)) {
            nimbus.initWorkerTokenManager();
        }
        LOG.info("Starting nimbus server for storm version '{}'", STORM_VERSION);
        server.serve();
        return nimbus;
    }

    public static Nimbus launch(INimbus inimbus) throws Exception {
        Map<String, Object> conf = Utils.merge(ConfigUtils.readStormConfig(),
                                               ConfigUtils.readYamlConfig("storm-cluster-auth.yaml", false));
        boolean fixupAcl = (boolean) conf.get(DaemonConfig.STORM_NIMBUS_ZOOKEEPER_ACLS_FIXUP);
        boolean checkAcl = fixupAcl || (boolean) conf.get(DaemonConfig.STORM_NIMBUS_ZOOKEEPER_ACLS_CHECK);
        if (checkAcl) {
            AclEnforcement.verifyAcls(conf, fixupAcl);
        }
        return launchServer(conf, inimbus);
    }

    public static void main(String[] args) throws Exception {
        Utils.setupDefaultUncaughtExceptionHandler();
        launch(new StandaloneINimbus());
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    private static CuratorFramework makeZKClient(Map<String, Object> conf) {
        List<String> servers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);
        String root = (String) conf.get(Config.STORM_ZOOKEEPER_ROOT);
        CuratorFramework ret = null;
        if (servers != null && port != null) {
            ret = ClientZookeeper.mkClient(conf, servers, port, root, new DefaultWatcherCallBack(), conf, DaemonType.NIMBUS);
        }
        return ret;
    }

    private static IStormClusterState makeStormClusterState(Map<String, Object> conf) throws Exception {
        return ClusterUtils.mkStormClusterState(conf, new ClusterStateContext(DaemonType.NIMBUS, conf));
    }

    private static List<Integer> asIntExec(List<Long> exec) {
        List<Integer> ret = new ArrayList<>(2);
        ret.add(exec.get(0).intValue());
        ret.add(exec.get(1).intValue());
        return ret;
    }

    /**
     * Diff old/new assignment to find nodes which assigned assignments has changed.
     *
     * @param oldAss old assigned assignment
     * @param newAss new assigned assignment
     * @return nodeId -> host map of assignments changed nodes
     */
    private static Map<String, String> assignmentChangedNodes(Assignment oldAss, Assignment newAss) {
        Map<List<Long>, NodeInfo> oldExecutorNodePort = null;
        Map<List<Long>, NodeInfo> newExecutorNodePort = null;
        Map<String, String> allNodeHost = new HashMap<>();
        if (oldAss != null) {
            oldExecutorNodePort = oldAss.get_executor_node_port();
            allNodeHost.putAll(oldAss.get_node_host());
        }
        if (newAss != null) {
            newExecutorNodePort = newAss.get_executor_node_port();
            allNodeHost.putAll(newAss.get_node_host());
        }
        //kill or newly submit
        if (oldAss == null || newAss == null) {
            return allNodeHost;
        } else {
            // rebalance
            Map<String, String> ret = new HashMap<>();
            for (Map.Entry<List<Long>, NodeInfo> entry : newExecutorNodePort.entrySet()) {
                NodeInfo newNodeInfo = entry.getValue();
                NodeInfo oldNodeInfo = oldExecutorNodePort.get(entry.getKey());
                if (null != oldNodeInfo) {
                    if (!oldNodeInfo.equals(newNodeInfo)) {
                        ret.put(oldNodeInfo.get_node(), allNodeHost.get(oldNodeInfo.get_node()));
                        ret.put(newNodeInfo.get_node(), allNodeHost.get(newNodeInfo.get_node()));
                    }
                } else {
                    ret.put(newNodeInfo.get_node(), allNodeHost.get(newNodeInfo.get_node()));
                }
            }

            return ret;
        }
    }

    /**
     * Pick out assignments for a specific host from all assignments.  This could include multiple NUMA
     * supervisors on an individual host.
     * @param assignmentMap stormId -> assignment map
     * @param hostname        hostname
     * @return stormId -> assignment map for the node
     */
    private static Map<String, Assignment> assignmentsForHost(Map<String, Assignment> assignmentMap, String hostname) {
        Map<String, Assignment> ret = new HashMap<>();

        assignmentMap.entrySet().stream().filter(assignmentEntry -> assignmentEntry.getValue().get_node_host().values()
                .contains(hostname))
                .forEach(assignmentEntry -> {
                    ret.put(assignmentEntry.getKey(), assignmentEntry.getValue());
                });

        return ret;
    }

    /**
     * Pick out assignments for specific NodeId from all assignments.
     *
     * @param assignmentMap stormId -> assignment map
     * @param nodeId        supervisor node id
     * @return stormId -> assignment map for the node
     */
    private static Map<String, Assignment> assignmentsForNodeId(Map<String, Assignment> assignmentMap, String nodeId) {
        Map<String, Assignment> ret = new HashMap<>();

        assignmentMap.entrySet().stream().filter(assignmentEntry -> assignmentEntry.getValue().get_node_host().keySet()

                .contains(nodeId))
                .forEach(assignmentEntry -> {
                    ret.put(assignmentEntry.getKey(), assignmentEntry.getValue());
                });

        return ret;
    }


    /**
     * Notify supervisors/nodes assigned assignments.
     *
     * @param assignments       assignments map for nodes
     * @param service           {@link AssignmentDistributionService} for distributing assignments asynchronous
     * @param nodeHost          node -> host map
     * @param supervisorDetails nodeId -> {@link SupervisorDetails} map
     */
    private static void notifySupervisorsAssignments(Map<String, Assignment> assignments,
                                                     AssignmentDistributionService service, Map<String, String> nodeHost,
                                                     Map<String, SupervisorDetails> supervisorDetails,
                                                     StormMetricsRegistry metricsRegistry) {
        for (Map.Entry<String, String> nodeEntry : nodeHost.entrySet()) {
            try {
                String nodeId = nodeEntry.getKey();
                String hostname = nodeEntry.getValue();
                SupervisorAssignments supervisorAssignments = new SupervisorAssignments();
                supervisorAssignments.set_storm_assignment(assignmentsForHost(assignments, hostname));
                SupervisorDetails details = supervisorDetails.get(nodeId);
                Integer serverPort = details != null ? details.getServerPort() : null;
                service.addAssignmentsForNode(nodeId, nodeEntry.getValue(), serverPort, supervisorAssignments, metricsRegistry);
            } catch (Throwable tr1) {
                //just skip when any error happens wait for next round assignments reassign
                LOG.error("Exception when add assignments distribution task for node {}", nodeEntry.getKey());
            }
        }
    }

    private static void notifySupervisorsAsKilled(IStormClusterState clusterState, Assignment oldAss,
                                                  AssignmentDistributionService service, StormMetricsRegistry metricsRegistry) {
        Map<String, String> nodeHost = assignmentChangedNodes(oldAss, null);
        notifySupervisorsAssignments(clusterState.assignmentsInfo(), service, nodeHost,
                                     basicSupervisorDetailsMap(clusterState), metricsRegistry);
    }

    Map<String, Object> getConf() {
        return conf;
    }

    @VisibleForTesting
    public void setAuthorizationHandler(IAuthorizer authorizationHandler) {
        this.authorizationHandler = authorizationHandler;
    }

    private IStormClusterState getStormClusterState() {
        return stormClusterState;
    }

    private AssignmentDistributionService getAssignmentsDistributer() {
        return assignmentsDistributer;
    }

    private StormMetricsRegistry getMetricsRegistry() {
        return metricsRegistry;
    }

    @VisibleForTesting
    public HeartbeatCache getHeartbeatsCache() {
        return heartbeatsCache;
    }

    public AtomicReference<Map<String, Set<List<Integer>>>> getIdToExecutors() {
        return idToExecutors;
    }

    private Set<List<Integer>> getOrUpdateExecutors(String topoId, StormBase base, Map<String, Object> topoConf,
                                                    StormTopology topology)
        throws InvalidTopologyException {
        Set<List<Integer>> executors = idToExecutors.get().get(topoId);
        if (null == executors) {
            executors = new HashSet<>(computeExecutors(base, topoConf, topology));
            idToExecutors.getAndUpdate(new Assoc<>(topoId, executors));
        }
        return executors;
    }

    private BlobStore getBlobStore() {
        return blobStore;
    }

    private TopoCache getTopoCache() {
        return topoCache;
    }

    @VisibleForTesting
    void initWorkerTokenManager() {
        if (workerTokenManager == null) {
            workerTokenManager = new WorkerTokenManager(conf, getStormClusterState());
        }
    }

    private boolean isLeader() throws Exception {
        return leaderElector.isLeader();
    }

    private void assertIsLeader() throws Exception {
        if (!isLeader()) {
            NimbusInfo leaderAddress = leaderElector.getLeader();
            throw new RuntimeException("not a leader, current leader is " + leaderAddress);
        }
    }

    private String getInbox() throws IOException {
        return ServerConfigUtils.masterInbox(conf);
    }

    /**
     * Used for local cluster.
     *
     * @param supervisor {@link org.apache.storm.daemon.supervisor.Supervisor}
     */
    public void addSupervisor(org.apache.storm.daemon.supervisor.Supervisor supervisor) {
        assignmentsDistributer.addLocalSupervisor(supervisor);
    }

    void delayEvent(String topoId, int delaySecs, TopologyActions event, Object args) {
        LOG.info("Delaying event {} for {} secs for {}", event, delaySecs, topoId);
        timer.schedule(delaySecs, () -> {
            try {
                transition(topoId, event, args, false);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    void doRebalance(String topoId, StormBase stormBase) throws Exception {
        RebalanceOptions rbo = stormBase.get_topology_action_options().get_rebalance_options();
        StormBase updated = new StormBase();
        updated.set_topology_action_options(null);
        updated.set_component_debug(Collections.emptyMap());

        if (rbo.is_set_num_executors()) {
            updated.set_component_executors(rbo.get_num_executors());
        }

        if (rbo.is_set_num_workers()) {
            updated.set_num_workers(rbo.get_num_workers());
        }
        stormClusterState.updateStorm(topoId, updated);
        updateBlobStore(topoId, rbo, ServerUtils.principalNameToSubject(rbo.get_principal()));
        idToExecutors.getAndUpdate(new Dissoc<>(topoId)); // remove the executors cache to let it recompute.
        mkAssignments(topoId);
    }

    private String toTopoId(String topoName) throws NotAliveException {
        return stormClusterState.getTopoId(topoName)
                                .orElseThrow(() -> new WrappedNotAliveException(topoName + " is not alive"));
    }

    private void transitionName(String topoName, TopologyActions event, Object eventArg, boolean errorOnNoTransition) throws Exception {
        transition(toTopoId(topoName), event, eventArg, errorOnNoTransition);
    }

    private void transition(String topoId, TopologyActions event, Object eventArg) throws Exception {
        transition(topoId, event, eventArg, false);
    }

    private void transition(String topoId, TopologyActions event, Object eventArg, boolean errorOnNoTransition)
        throws Exception {
        LOG.info("TRANSITION: {} {} {} {}", topoId, event, eventArg, errorOnNoTransition);
        assertIsLeader();
        synchronized (submitLock) {
            IStormClusterState clusterState = stormClusterState;
            StormBase base = clusterState.stormBase(topoId, null);
            if (base == null || base.get_status() == null) {
                LOG.info("Cannot apply event {} to {} because topology no longer exists", event, topoId);
            } else {
                TopologyStatus status = base.get_status();
                TopologyStateTransition transition = TOPO_STATE_TRANSITIONS.get(status).get(event);
                if (transition == null) {
                    String message = "No transition for event: " + event + ", status: " + status + " storm-id: " + topoId;
                    if (errorOnNoTransition) {
                        throw new RuntimeException(message);
                    }

                    if (TopologyActions.GAIN_LEADERSHIP != event) {
                        //GAIN_LEADERSHIP is a system event so don't log an issue
                        LOG.info(message);
                    }
                    transition = NOOP_TRANSITION;
                }
                StormBase updates = transition.transition(eventArg, this, topoId, base);
                if (updates != null) {
                    clusterState.updateStorm(topoId, updates);
                }
            }
        }
    }

    private void setupStormCode(Map<String, Object> conf, String topoId, String tmpJarLocation,
                                Map<String, Object> topoConf, StormTopology topology) throws Exception {
        Subject subject = getSubject();
        IStormClusterState clusterState = stormClusterState;
        BlobStore store = blobStore;
        String jarKey = ConfigUtils.masterStormJarKey(topoId);
        if (tmpJarLocation != null) {
            //in local mode there is no jar
            try (FileInputStream fin = new FileInputStream(tmpJarLocation)) {
                store.createBlob(jarKey, fin, new SettableBlobMeta(BlobStoreAclHandler.DEFAULT), subject);
            }
        }

        topoCache.addTopoConf(topoId, subject, topoConf);
        topoCache.addTopology(topoId, subject, topology);
    }

    private void updateTopologyResources(String topoId, Map<String, Map<String, Double>> resourceOverrides, Subject subject)
        throws AuthorizationException, IOException, KeyNotFoundException {
        StormTopology topo = topoCache.readTopology(topoId, subject);
        topo = topo.deepCopy();
        ResourceUtils.updateStormTopologyResources(topo, resourceOverrides);
        topoCache.updateTopology(topoId, subject, topo);
    }

    private void updateTopologyConf(String topoId, Map<String, Object> configOverride, Subject subject)
        throws AuthorizationException, IOException, KeyNotFoundException {
        Map<String, Object> topoConf = new HashMap<>(topoCache.readTopoConf(topoId, subject)); //Copy the data
        topoConf.putAll(configOverride);
        topoCache.updateTopoConf(topoId, subject, topoConf);
    }

    private void updateBlobStore(String topoId, RebalanceOptions rbo, Subject subject)
        throws AuthorizationException, IOException, KeyNotFoundException {
        Map<String, Map<String, Double>> resourceOverrides = rbo.get_topology_resources_overrides();
        if (resourceOverrides != null && !resourceOverrides.isEmpty()) {
            updateTopologyResources(topoId, resourceOverrides, subject);
        }
        String confOverride = rbo.get_topology_conf_overrides();
        if (confOverride != null && !confOverride.isEmpty()) {
            updateTopologyConf(topoId, Utils.parseJson(confOverride), subject);
        }
    }

    private Integer getBlobReplicationCount(String key) throws Exception {
        BlobStore store = blobStore;
        if (store != null) {
            return store.getBlobReplication(key, NIMBUS_SUBJECT);
        }
        return null;
    }

    private void waitForDesiredCodeReplication(Map<String, Object> topoConf, String topoId) throws Exception {
        int minReplicationCount = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_MIN_REPLICATION_COUNT));
        int maxWaitTime = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_MAX_REPLICATION_WAIT_TIME_SEC));
        int jarCount = minReplicationCount;
        if (!ConfigUtils.isLocalMode(topoConf)) {
            jarCount = getBlobReplicationCount(ConfigUtils.masterStormJarKey(topoId));
        }
        int codeCount = getBlobReplicationCount(ConfigUtils.masterStormCodeKey(topoId));
        int confCount = getBlobReplicationCount(ConfigUtils.masterStormConfKey(topoId));
        long totalWaitTime = 0;
        //When is this ever null?
        if (blobStore != null) {
            while (jarCount < minReplicationCount
                   && codeCount < minReplicationCount
                   && confCount < minReplicationCount) {
                if (maxWaitTime > 0 && totalWaitTime > maxWaitTime) {
                    LOG.info("desired replication count of {} not achieved for {} but we have hit the max wait time {}"
                             + " so moving on with replication count for conf key = {} for code key = {} for jar key = ",
                             minReplicationCount, topoId, maxWaitTime, confCount, codeCount, jarCount);
                    return;
                }
                LOG.debug("Checking if I am still the leader");
                assertIsLeader();
                LOG.info("WAITING... storm-id {}, {} <? {} {} {}", topoId, minReplicationCount, jarCount, codeCount, confCount);
                LOG.info("WAITING... {} <? {}", totalWaitTime, maxWaitTime);
                Time.sleepSecs(1);
                totalWaitTime++;
                if (!ConfigUtils.isLocalMode(topoConf)) {
                    jarCount = getBlobReplicationCount(ConfigUtils.masterStormJarKey(topoId));
                }
                codeCount = getBlobReplicationCount(ConfigUtils.masterStormCodeKey(topoId));
                confCount = getBlobReplicationCount(ConfigUtils.masterStormConfKey(topoId));
            }
        }
        LOG.info("desired replication count {} achieved for topology {}, current-replication-count for conf key = {},"
                 + " current-replication-count for code key = {}, current-replication-count for jar key = {}",
                 minReplicationCount, topoId, confCount, codeCount, jarCount);
    }

    private TopologyDetails readTopologyDetails(String topoId, StormBase base) throws KeyNotFoundException,
        AuthorizationException, IOException, InvalidTopologyException {
        assert (base != null);
        assert (topoId != null);

        Map<String, Object> topoConf = readTopoConfAsNimbus(topoId, topoCache);
        StormTopology topo = readStormTopologyAsNimbus(topoId, topoCache);
        if (!base.is_set_principal()) {
            fixupBase(base, topoConf);
            stormClusterState.updateStorm(topoId, base);
        }
        Map<List<Integer>, String> rawExecToComponent = computeExecutorToComponent(topoId, base, topoConf, topo);
        Map<ExecutorDetails, String> executorsToComponent = new HashMap<>();
        for (Entry<List<Integer>, String> entry : rawExecToComponent.entrySet()) {
            List<Integer> execs = entry.getKey();
            ExecutorDetails execDetails = new ExecutorDetails(execs.get(0), execs.get(1));
            executorsToComponent.put(execDetails, entry.getValue());
        }

        return new TopologyDetails(topoId, topoConf, topo, base.get_num_workers(), executorsToComponent,
            base.get_launch_time_secs(), base.get_owner());
    }

    private void updateHeartbeatsFromZkHeartbeat(String topoId, Set<List<Integer>> allExecutors, Assignment existingAssignment) {
        LOG.debug("Updating heartbeats for {} {} (from ZK heartbeat)", topoId, allExecutors);
        IStormClusterState state = stormClusterState;
        Map<List<Integer>, Map<String, Object>> executorBeats =
            StatsUtil.convertExecutorBeats(state.executorBeats(topoId, existingAssignment.get_executor_node_port()));
        heartbeatsCache.updateFromZkHeartbeat(topoId, executorBeats, allExecutors, getTopologyHeartbeatTimeoutSecs(topoId));
    }

    /**
     * Update all the heartbeats for all the topologies' executors.
     *
     * @param existingAssignments current assignments (thrift)
     * @param topologyToExecutors topology ID to executors.
     */
    private void updateAllHeartbeats(Map<String, Assignment> existingAssignments,
                                     Map<String, Set<List<Integer>>> topologyToExecutors, Set<String> zkHeartbeatTopologies) {
        for (Entry<String, Assignment> entry : existingAssignments.entrySet()) {
            String topoId = entry.getKey();
            if (zkHeartbeatTopologies.contains(topoId)) {
                updateHeartbeatsFromZkHeartbeat(topoId, topologyToExecutors.get(topoId), entry.getValue());
            } else {
                LOG.debug("Timing out old heartbeats for {}", topoId);
                heartbeatsCache.timeoutOldHeartbeats(topoId, getTopologyHeartbeatTimeoutSecs(topoId));
            }
        }
    }

    private void updateCachedHeartbeatsFromWorker(SupervisorWorkerHeartbeat workerHeartbeat, int heartbeatTimeoutSecs) {
        heartbeatsCache.updateHeartbeat(workerHeartbeat, heartbeatTimeoutSecs);
    }

    private void updateCachedHeartbeatsFromSupervisor(SupervisorWorkerHeartbeats workerHeartbeats) {
        for (SupervisorWorkerHeartbeat hb : workerHeartbeats.get_worker_heartbeats()) {
            String topoId = hb.get_storm_id();
            int heartbeatTimeoutSecs = getTopologyHeartbeatTimeoutSecs(topoId);
            updateCachedHeartbeatsFromWorker(hb, heartbeatTimeoutSecs);
        }
        if (!heartbeatsReadyFlag.get() && !Strings.isNullOrEmpty(workerHeartbeats.get_supervisor_id())) {
            heartbeatsRecoveryStrategy.reportNodeId(workerHeartbeats.get_supervisor_id());
        }
    }

    /**
     * Decide if the heartbeats is recovered for a master, will wait for all the assignments nodes to recovery, every node will take care
     * its node heartbeats reporting.
     *
     * @return true if all nodes have reported heartbeats or exceeds max-time-out
     */
    private boolean isHeartbeatsRecovered() {
        if (heartbeatsReadyFlag.get()) {
            return true;
        }
        Set<String> allNodes = new HashSet<>();
        for (Map.Entry<String, Assignment> assignmentEntry : stormClusterState.assignmentsInfo().entrySet()) {
            allNodes.addAll(assignmentEntry.getValue().get_node_host().keySet());
        }
        boolean isReady = heartbeatsRecoveryStrategy.isReady(allNodes);
        if (isReady) {
            heartbeatsReadyFlag.getAndSet(true);
        }
        return isReady;
    }

    /**
     * Decide if the assignments is synchronized.
     *
     * @return true if assignments have been synchronized from remote state store
     */
    private boolean isAssignmentsRecovered() {
        return stormClusterState.isAssignmentsBackendSynchronized();
    }

    private Set<List<Integer>> aliveExecutors(String topoId, Set<List<Integer>> allExecutors, Assignment assignment) {
        return heartbeatsCache.getAliveExecutors(topoId, allExecutors, assignment, getTopologyLaunchHeartbeatTimeoutSec(topoId));
    }

    private List<List<Integer>> computeExecutors(StormBase base, Map<String, Object> topoConf,
                                                 StormTopology topology)
        throws InvalidTopologyException {

        assert (base != null);

        Map<String, Integer> compToExecutors = base.get_component_executors();
        List<List<Integer>> ret = new ArrayList<>();
        if (compToExecutors != null) {
            Map<Integer, String> taskInfo = StormCommon.stormTaskInfo(topology, topoConf);
            Map<String, List<Integer>> compToTaskList = Utils.reverseMap(taskInfo);
            for (Entry<String, List<Integer>> entry : compToTaskList.entrySet()) {
                String compId = entry.getKey();
                List<Integer> tasks = entry.getValue();
                tasks.sort(null);
                Integer numExecutors = compToExecutors.get(compId);
                if (numExecutors != null) {
                    List<List<Integer>> partitioned = Utils.partitionFixed(numExecutors, tasks);
                    for (List<Integer> partition : partitioned) {
                        ret.add(Arrays.asList(partition.get(0), partition.get(partition.size() - 1)));
                    }
                }
            }
        }
        return ret;
    }

    private Map<List<Integer>, String> computeExecutorToComponent(String topoId, StormBase base,
                                                                  Map<String, Object> topoConf, StormTopology topology)
        throws InvalidTopologyException {
        List<List<Integer>> executors = new ArrayList<>(getOrUpdateExecutors(topoId, base, topoConf, topology));
        Map<Integer, String> taskToComponent = StormCommon.stormTaskInfo(topology, topoConf);
        Map<List<Integer>, String> ret = new HashMap<>();
        for (List<Integer> executor : executors) {
            ret.put(executor, taskToComponent.get(executor.get(0)));
        }
        return ret;
    }

    private Map<String, Set<List<Integer>>> computeTopologyToExecutors(Map<String, StormBase> bases)
        throws KeyNotFoundException, AuthorizationException, InvalidTopologyException, IOException {
        Map<String, Set<List<Integer>>> ret = new HashMap<>();
        if (bases != null) {
            for (Entry<String, StormBase> entry : bases.entrySet()) {
                String topoId = entry.getKey();
                Set<List<Integer>> executors = idToExecutors.get().get(topoId);
                if (executors == null) {
                    Map<String, Object> topoConf = readTopoConfAsNimbus(topoId, topoCache);
                    StormTopology topology = readStormTopologyAsNimbus(topoId, topoCache);
                    executors = getOrUpdateExecutors(topoId, entry.getValue(), topoConf, topology);
                }
                ret.put(topoId, executors);
            }
        }
        return ret;
    }

    /**
     * compute a topology-id -> alive executors map.
     *
     * @param existingAssignment  the current assignments
     * @param topologyToExecutors the executors for the current topologies
     * @param scratchTopologyId   the topology being rebalanced and should be excluded
     * @return the map of topology id to alive executors
     */
    private Map<String, Set<List<Integer>>> computeTopologyToAliveExecutors(Map<String, Assignment> existingAssignment,
                                                                            Map<String, Set<List<Integer>>> topologyToExecutors,
                                                                            String scratchTopologyId) {
        Map<String, Set<List<Integer>>> ret = new HashMap<>();
        for (Entry<String, Assignment> entry : existingAssignment.entrySet()) {
            String topoId = entry.getKey();
            Assignment assignment = entry.getValue();
            Set<List<Integer>> allExecutors = topologyToExecutors.get(topoId);
            Set<List<Integer>> aliveExecutors;
            if (topoId.equals(scratchTopologyId)) {
                aliveExecutors = allExecutors;
            } else {
                aliveExecutors = new HashSet<>(aliveExecutors(topoId, allExecutors, assignment));
            }
            ret.put(topoId, aliveExecutors);
        }
        return ret;
    }

    private Map<String, Set<Long>> computeSupervisorToDeadPorts(Map<String, Assignment> existingAssignments,
                                                                Map<String, Set<List<Integer>>> topologyToExecutors,
                                                                Map<String, Set<List<Integer>>> topologyToAliveExecutors) {
        Map<String, Set<Long>> ret = new HashMap<>();
        for (Entry<String, Assignment> entry : existingAssignments.entrySet()) {
            String topoId = entry.getKey();
            Assignment assignment = entry.getValue();
            Set<List<Integer>> allExecutors = topologyToExecutors.get(topoId);
            Set<List<Integer>> aliveExecutors = topologyToAliveExecutors.get(topoId);
            Set<List<Integer>> deadExecutors = new HashSet<>(allExecutors);
            deadExecutors.removeAll(aliveExecutors);
            Map<List<Long>, NodeInfo> execToNodePort = assignment.get_executor_node_port();
            for (Entry<List<Long>, NodeInfo> assigned : execToNodePort.entrySet()) {
                if (deadExecutors.contains(asIntExec(assigned.getKey()))) {
                    NodeInfo info = assigned.getValue();
                    String superId = info.get_node();
                    Set<Long> ports = ret.get(superId);
                    if (ports == null) {
                        ports = new HashSet<>();
                        ret.put(superId, ports);
                    }
                    ports.addAll(info.get_port());
                }
            }
        }
        return ret;
    }

    /**
     * Convert assignment information in zk to SchedulerAssignment, so it can be used by scheduler api.
     *
     * @param existingAssignments      current assignments
     * @param topologyToAliveExecutors executors that are alive
     * @return topo ID to schedulerAssignment
     */
    private Map<String, SchedulerAssignmentImpl> computeTopologyToSchedulerAssignment(Map<String, Assignment> existingAssignments,
                                                                                      Map<String, Set<List<Integer>>>
                                                                                          topologyToAliveExecutors) {
        Map<String, SchedulerAssignmentImpl> ret = new HashMap<>();
        for (Entry<String, Assignment> entry : existingAssignments.entrySet()) {
            String topoId = entry.getKey();
            Assignment assignment = entry.getValue();
            Set<List<Integer>> aliveExecutors = topologyToAliveExecutors.get(topoId);
            Map<List<Long>, NodeInfo> execToNodePort = assignment.get_executor_node_port();
            Map<NodeInfo, WorkerResources> workerToResources = assignment.get_worker_resources();
            Map<NodeInfo, WorkerSlot> nodePortToSlot = new HashMap<>();
            Map<WorkerSlot, WorkerResources> slotToResources = new HashMap<>();
            for (Entry<NodeInfo, WorkerResources> nodeAndResources : workerToResources.entrySet()) {
                NodeInfo info = nodeAndResources.getKey();
                WorkerResources resources = nodeAndResources.getValue();
                WorkerSlot slot = new WorkerSlot(info.get_node(), info.get_port_iterator().next());
                nodePortToSlot.put(info, slot);
                slotToResources.put(slot, resources);
            }
            Map<ExecutorDetails, WorkerSlot> execToSlot = new HashMap<>();
            for (Entry<List<Long>, NodeInfo> execAndNodePort : execToNodePort.entrySet()) {
                List<Integer> exec = asIntExec(execAndNodePort.getKey());
                NodeInfo info = execAndNodePort.getValue();
                if (aliveExecutors.contains(exec)) {
                    execToSlot.put(new ExecutorDetails(exec.get(0), exec.get(1)), nodePortToSlot.get(info));
                }
            }
            ret.put(topoId, new SchedulerAssignmentImpl(topoId, execToSlot, slotToResources, null));
        }
        return ret;
    }

    /**
     * Read supervisor details/exclude the dead slots.
     *
     * @param superToDeadPorts            dead ports on the supervisor
     * @param topologies                  all of the topologies
     * @param missingAssignmentTopologies topologies that need assignments
     * @return a map: {supervisor-id SupervisorDetails}
     */
    private Map<String, SupervisorDetails> readAllSupervisorDetails(Map<String, Set<Long>> superToDeadPorts,
                                                                    Topologies topologies, Collection<String> missingAssignmentTopologies) {
        Map<String, SupervisorDetails> ret = new HashMap<>();
        IStormClusterState state = stormClusterState;
        Map<String, SupervisorInfo> superInfos = state.allSupervisorInfo();
        List<SupervisorDetails> superDetails = new ArrayList<>();
        for (Entry<String, SupervisorInfo> entry : superInfos.entrySet()) {
            SupervisorInfo info = entry.getValue();
            superDetails.add(new SupervisorDetails(entry.getKey(), info.get_meta(), info.get_resources_map()));
        }
        // Note that allSlotsAvailableForScheduling
        // only uses the supervisor-details. The rest of the arguments
        // are there to satisfy the INimbus interface.
        Map<String, Set<Long>> superToPorts = new HashMap<>();
        for (WorkerSlot slot : inimbus.allSlotsAvailableForScheduling(superDetails, topologies,
                                                                      new HashSet<>(missingAssignmentTopologies))) {
            String superId = slot.getNodeId();
            Set<Long> ports = superToPorts.get(superId);
            if (ports == null) {
                ports = new HashSet<>();
                superToPorts.put(superId, ports);
            }
            ports.add((long) slot.getPort());
        }
        for (Entry<String, SupervisorInfo> entry : superInfos.entrySet()) {
            String superId = entry.getKey();
            SupervisorInfo info = entry.getValue();
            String hostname = info.get_hostname();
            // Hide the dead-ports from the all-ports
            // these dead-ports can be reused in next round of assignments
            Set<Long> deadPorts = superToDeadPorts.get(superId);
            Set<Long> allPorts = superToPorts.get(superId);
            if (allPorts == null) {
                allPorts = new HashSet<>();
            } else {
                allPorts = new HashSet<>(allPorts);
            }
            if (deadPorts != null) {
                allPorts.removeAll(deadPorts);
            }
            ret.put(superId, new SupervisorDetails(superId, hostname, info.get_scheduler_meta(),
                                                   allPorts, info.get_resources_map()));
        }
        return ret;
    }

    private boolean isFragmented(SupervisorResources supervisorResources) {
        double minMemory = ObjectReader.getDouble(conf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB), 256.0)
                           + ObjectReader.getDouble(conf.get(Config.TOPOLOGY_ACKER_RESOURCES_ONHEAP_MEMORY_MB), 128.0);
        double minCpu = ObjectReader.getDouble(conf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT), 50.0)
                        + ObjectReader.getDouble(conf.get(Config.TOPOLOGY_ACKER_CPU_PCORE_PERCENT), 50.0);

        return minMemory > supervisorResources.getAvailableMem() || minCpu > supervisorResources.getAvailableCpu();
    }

    private double fragmentedMemory() {
        Double res = nodeIdToResources.get().values().parallelStream().filter(this::isFragmented)
                                      .mapToDouble(SupervisorResources::getAvailableMem).filter(x -> x > 0).sum();
        return res.intValue();
    }

    private int fragmentedCpu() {
        Double res = nodeIdToResources.get().values().parallelStream().filter(this::isFragmented)
                                      .mapToDouble(SupervisorResources::getAvailableCpu).filter(x -> x > 0).sum();
        return res.intValue();
    }

    private Map<String, SchedulerAssignment> computeNewSchedulerAssignments(Map<String, Assignment> existingAssignments,
                                                                            Topologies topologies, Map<String, StormBase> bases,
                                                                            String scratchTopologyId)
        throws KeyNotFoundException, AuthorizationException, InvalidTopologyException, IOException {

        Map<String, Set<List<Integer>>> topoToExec = computeTopologyToExecutors(bases);

        Set<String> zkHeartbeatTopologies = topologies.getTopologies().stream()
                                                      .filter(topo -> !supportRpcHeartbeat(topo))
                                                      .map(TopologyDetails::getId)
                                                      .collect(Collectors.toSet());

        updateAllHeartbeats(existingAssignments, topoToExec, zkHeartbeatTopologies);

        Map<String, Set<List<Integer>>> topoToAliveExecutors = computeTopologyToAliveExecutors(existingAssignments, topoToExec,
                                                                                                scratchTopologyId);
        Map<String, Set<Long>> supervisorToDeadPorts = computeSupervisorToDeadPorts(existingAssignments, topoToExec,
                                                                                    topoToAliveExecutors);
        Map<String, SchedulerAssignmentImpl> topoToSchedAssignment = computeTopologyToSchedulerAssignment(existingAssignments,
                                                                                                          topoToAliveExecutors);
        Set<String> missingAssignmentTopologies = new HashSet<>();
        for (TopologyDetails topo : topologies.getTopologies()) {
            String id = topo.getId();
            Set<List<Integer>> allExecs = topoToExec.get(id);
            Set<List<Integer>> aliveExecs = topoToAliveExecutors.get(id);
            int numDesiredWorkers = topo.getNumWorkers();
            int numAssignedWorkers = numUsedWorkers(topoToSchedAssignment.get(id));
            if (allExecs == null || allExecs.isEmpty() || !allExecs.equals(aliveExecs) || numDesiredWorkers > numAssignedWorkers) {
                //We have something to schedule...
                missingAssignmentTopologies.add(id);
            }
        }
        Map<String, SupervisorDetails> supervisors =
            readAllSupervisorDetails(supervisorToDeadPorts, topologies, missingAssignmentTopologies);
        Cluster cluster = new Cluster(inimbus, resourceMetrics, supervisors, topoToSchedAssignment, topologies, conf);
        cluster.setStatusMap(idToSchedStatus.get());

        schedulingStartTimeNs.set(Time.nanoTime());
        scheduler.schedule(topologies, cluster);
        //Get and set the start time before getting current time in order to avoid potential race with the longest-scheduling-time-ms gauge
        final Long startTime = schedulingStartTimeNs.getAndSet(null);
        long elapsedNs = Time.nanoTime() - startTime;
        longestSchedulingTime.accumulateAndGet(elapsedNs, Math::max);
        schedulingDuration.update(elapsedNs, TimeUnit.NANOSECONDS);
        LOG.debug("Scheduling took {} ms for {} topologies", TimeUnit.NANOSECONDS.toMillis(elapsedNs), topologies.getTopologies().size());

        //merge with existing statuses
        idToSchedStatus.set(Utils.merge(idToSchedStatus.get(), cluster.getStatusMap()));
        nodeIdToResources.set(cluster.getSupervisorsResourcesMap());

        // This is a hack for non-ras scheduler topology and worker resources
        Map<String, TopologyResources> resources = cluster.getTopologyResourcesMap();
        idToResources.getAndAccumulate(resources, (orig, update) -> Utils.merge(orig, update));

        Map<String, Map<WorkerSlot, WorkerResources>> workerResources = new HashMap<>();
        for (Entry<String, Map<WorkerSlot, WorkerResources>> uglyWorkerResources : cluster.getWorkerResourcesMap().entrySet()) {
            Map<WorkerSlot, WorkerResources> slotToResources = new HashMap<>();
            for (Entry<WorkerSlot, WorkerResources> uglySlotToResources : uglyWorkerResources.getValue().entrySet()) {
                WorkerResources wr = uglySlotToResources.getValue();
                slotToResources.put(uglySlotToResources.getKey(), wr);
            }
            workerResources.put(uglyWorkerResources.getKey(), slotToResources);
        }
        idToWorkerResources.getAndAccumulate(workerResources, (orig, update) -> Utils.merge(orig, update));

        return cluster.getAssignments();
    }

    private boolean supportRpcHeartbeat(TopologyDetails topo) {
        if (stormClusterState.isPacemakerStateStore()) {
            // While using PacemakerStateStorage, ignore RPC heartbeat.
            return false;
        }

        if (!topo.getTopology().is_set_storm_version()) {
            // current version supports RPC heartbeat
            return true;
        }

        String stormVersionStr = topo.getTopology().get_storm_version();

        SimpleVersion stormVersion = new SimpleVersion(stormVersionStr);
        return stormVersion.compareTo(MIN_VERSION_SUPPORT_RPC_HEARTBEAT) >= 0;
    }

    private TopologyResources getResourcesForTopology(String topoId, StormBase base)
        throws NotAliveException, AuthorizationException, InvalidTopologyException, IOException {
        TopologyResources ret = idToResources.get().get(topoId);
        if (ret == null) {
            try {
                IStormClusterState state = stormClusterState;
                TopologyDetails details = readTopologyDetails(topoId, base);
                Assignment assignment = state.assignmentInfo(topoId, null);
                ret = new TopologyResources(details, assignment);
            } catch (KeyNotFoundException e) {
                //This can happen when a topology is first coming up
                // It's thrown by the blobstore code
                LOG.error("Failed to get topology details", e);
                ret = new TopologyResources();
            }
        }
        return ret;
    }

    private Map<WorkerSlot, WorkerResources> getWorkerResourcesForTopology(String topoId) {
        Map<WorkerSlot, WorkerResources> ret = idToWorkerResources.get().get(topoId);
        if (ret == null) {
            IStormClusterState state = stormClusterState;
            ret = new HashMap<>();
            Assignment assignment = state.assignmentInfo(topoId, null);
            if (assignment != null && assignment.is_set_worker_resources()) {
                for (Entry<NodeInfo, WorkerResources> entry : assignment.get_worker_resources().entrySet()) {
                    NodeInfo ni = entry.getKey();
                    WorkerSlot slot = new WorkerSlot(ni.get_node(), ni.get_port_iterator().next());
                    ret.put(slot, entry.getValue());
                }
                idToWorkerResources.getAndUpdate(new Assoc<>(topoId, ret));
            }
        }
        return ret;
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    private boolean isReadyForMKAssignments() throws Exception {
        if (isLeader()) {
            if (isHeartbeatsRecovered()) {
                if (isAssignmentsRecovered()) {
                    return true;
                }
                LOG.warn("waiting for assignments recovery, skipping assignments");
            }
            LOG.warn("waiting for worker heartbeats recovery, skipping assignments");
        } else {
            LOG.info("not a leader, skipping assignments");
        }
        return false;
    }

    private void mkAssignments() throws Exception {
        mkAssignments(null);
    }

    private void mkAssignments(String scratchTopoId) throws Exception {
        try {
            if (!isReadyForMKAssignments()) {
                return;
            }
            // get existing assignment (just the topologyToExecutorToNodePort map) -> default to {}
            // filter out ones which have a executor timeout
            // figure out available slots on cluster. add to that the used valid slots to get total slots. figure out how many executors
            // should be in each slot (e.g., 4, 4, 4, 5)
            // only keep existing slots that satisfy one of those slots. for rest, reassign them across remaining slots
            // edge case for slots with no executor timeout but with supervisor timeout... just treat these as valid slots that can be
            // reassigned to. worst comes to worse the executor will timeout and won't assign here next time around

            IStormClusterState state = stormClusterState;
            //read all the topologies
            Map<String, StormBase> bases;
            Map<String, TopologyDetails> tds = new HashMap<>();
            synchronized (submitLock) {
                // should promote: only fetch storm bases of topologies that need scheduling.
                bases = state.topologyBases();

                for (Iterator<Entry<String, StormBase>> it = bases.entrySet().iterator(); it.hasNext(); ) {
                    Entry<String, StormBase> entry = it.next();
                    String id = entry.getKey();
                    StormBase base = entry.getValue();
                    try {
                        tds.put(id, readTopologyDetails(id, base));
                    } catch (KeyNotFoundException e) {
                        //A race happened and it is probably not running
                        it.remove();
                    }
                }
            }
            List<String> assignedTopologyIds = state.assignments(null);
            Map<String, Assignment> existingAssignments = new HashMap<>();
            for (String id : assignedTopologyIds) {
                //for the topology which wants rebalance (specified by the scratchTopoId)
                // we exclude its assignment, meaning that all the slots occupied by its assignment
                // will be treated as free slot in the scheduler code.
                if (!id.equals(scratchTopoId)) {
                    Assignment currentAssignment = state.assignmentInfo(id, null);
                    if (!currentAssignment.is_set_owner()) {
                        TopologyDetails td = tds.get(id);
                        if (td != null) {
                            currentAssignment.set_owner(td.getTopologySubmitter());
                            state.setAssignment(id, currentAssignment, td.getConf());
                        }
                    }
                    existingAssignments.put(id, currentAssignment);
                }
            }

            // make the new assignments for topologies
            lockingMkAssignments(existingAssignments, bases, scratchTopoId, assignedTopologyIds, state, tds);
        } catch (Exception e) {
            this.mkAssignmentsErrors.mark();
            throw e;
        }
    }

    private void lockingMkAssignments(Map<String, Assignment> existingAssignments, Map<String, StormBase> bases,
                                      String scratchTopoId, List<String> assignedTopologyIds, IStormClusterState state,
                                      Map<String, TopologyDetails> tds) throws Exception {
        Topologies topologies = new Topologies(tds);

        synchronized (schedLock) {
            Map<String, SchedulerAssignment> newSchedulerAssignments =
                    computeNewSchedulerAssignments(existingAssignments, topologies, bases, scratchTopoId);

            Map<String, Map<List<Long>, List<Object>>> topologyToExecutorToNodePort =
                    computeTopoToExecToNodePort(newSchedulerAssignments, assignedTopologyIds);
            Map<String, Map<WorkerSlot, WorkerResources>> newAssignedWorkerToResources =
                    computeTopoToNodePortToResources(newSchedulerAssignments);
            int nowSecs = Time.currentTimeSecs();
            Map<String, SupervisorDetails> basicSupervisorDetailsMap = basicSupervisorDetailsMap(state);
            //construct the final Assignments by adding start-times etc into it
            Map<String, Assignment> newAssignments = new HashMap<>();
            for (Entry<String, Map<List<Long>, List<Object>>> entry : topologyToExecutorToNodePort.entrySet()) {
                String topoId = entry.getKey();
                Map<List<Long>, List<Object>> execToNodePort = entry.getValue();
                if (execToNodePort == null) {
                    execToNodePort = new HashMap<>();
                }
                Set<String> allNodes = new HashSet<>();
                for (List<Object> nodePort : execToNodePort.values()) {
                    allNodes.add((String) nodePort.get(0));
                }
                Map<String, String> allNodeHost = new HashMap<>();
                Assignment existingAssignment = existingAssignments.get(topoId);
                if (existingAssignment != null) {
                    allNodeHost.putAll(existingAssignment.get_node_host());
                }
                for (String node : allNodes) {
                    String host = inimbus.getHostName(basicSupervisorDetailsMap, node);
                    if (host != null) {
                        allNodeHost.put(node, host);
                    }
                }
                Map<List<Long>, NodeInfo> execNodeInfo = null;
                if (existingAssignment != null) {
                    execNodeInfo = existingAssignment.get_executor_node_port();
                }
                List<List<Long>> reassignExecutors = changedExecutors(execNodeInfo, execToNodePort);
                Map<List<Long>, Long> startTimes = new HashMap<>();
                if (existingAssignment != null) {
                    startTimes.putAll(existingAssignment.get_executor_start_time_secs());
                }
                for (List<Long> id : reassignExecutors) {
                    startTimes.put(id, (long) nowSecs);
                }
                Map<WorkerSlot, WorkerResources> workerToResources = newAssignedWorkerToResources.get(topoId);
                if (workerToResources == null) {
                    workerToResources = new HashMap<>();
                }
                Assignment newAssignment = new Assignment((String) conf.get(Config.STORM_LOCAL_DIR));
                Map<String, String> justAssignedKeys = new HashMap<>(allNodeHost);
                //Modifies justAssignedKeys
                justAssignedKeys.keySet().retainAll(allNodes);
                newAssignment.set_node_host(justAssignedKeys);
                //convert NodePort to NodeInfo (again!!!).
                Map<List<Long>, NodeInfo> execToNodeInfo = new HashMap<>();
                for (Entry<List<Long>, List<Object>> execAndNodePort : execToNodePort.entrySet()) {
                    List<Object> nodePort = execAndNodePort.getValue();
                    NodeInfo ni = new NodeInfo();
                    ni.set_node((String) nodePort.get(0));
                    ni.add_to_port((Long) nodePort.get(1));
                    execToNodeInfo.put(execAndNodePort.getKey(), ni);
                }
                newAssignment.set_executor_node_port(execToNodeInfo);
                newAssignment.set_executor_start_time_secs(startTimes);
                //do another conversion (lets just make this all common)
                Map<NodeInfo, WorkerResources> workerResources = new HashMap<>();
                for (Entry<WorkerSlot, WorkerResources> wr : workerToResources.entrySet()) {
                    WorkerSlot nodePort = wr.getKey();
                    NodeInfo ni = new NodeInfo();
                    ni.set_node(nodePort.getNodeId());
                    ni.add_to_port(nodePort.getPort());
                    WorkerResources resources = wr.getValue();
                    workerResources.put(ni, resources);
                }
                newAssignment.set_worker_resources(workerResources);
                TopologyDetails td = tds.get(topoId);
                newAssignment.set_owner(td.getTopologySubmitter());
                newAssignments.put(topoId, newAssignment);
            }

            boolean assignmentChanged = auditAssignmentChanges(existingAssignments, newAssignments);
            if (assignmentChanged) {
                LOG.debug("RESETTING id->resources and id->worker-resources cache!");
                idToResources.set(new HashMap<>());
                idToWorkerResources.set(new HashMap<>());
            }

            //tasks figure out what tasks to talk to by looking at topology at runtime
            // only log/set when there's been a change to the assignment
            for (Entry<String, Assignment> entry : newAssignments.entrySet()) {
                String topoId = entry.getKey();
                Assignment assignment = entry.getValue();
                Assignment existingAssignment = existingAssignments.get(topoId);
                TopologyDetails td = topologies.getById(topoId);
                if (assignment.equals(existingAssignment)) {
                    LOG.debug("Assignment for {} hasn't changed", topoId);
                } else {
                    LOG.info("Setting new assignment for topology id {}: {}", topoId, assignment);
                    state.setAssignment(topoId, assignment, td.getConf());
                }
            }

            //grouping assignment by node to see the nodes diff, then notify nodes/supervisors to synchronize its owned assignment
            //because the number of existing assignments is small for every scheduling round,
            //we expect to notify supervisors at almost the same time
            Map<String, String> totalAssignmentsChangedNodes = new HashMap<>();
            for (Entry<String, Assignment> entry : newAssignments.entrySet()) {
                String topoId = entry.getKey();
                Assignment assignment = entry.getValue();
                Assignment existingAssignment = existingAssignments.get(topoId);
                totalAssignmentsChangedNodes.putAll(assignmentChangedNodes(existingAssignment, assignment));
            }
            notifySupervisorsAssignments(newAssignments, assignmentsDistributer, totalAssignmentsChangedNodes,
                                        basicSupervisorDetailsMap, getMetricsRegistry());

            Map<String, Collection<WorkerSlot>> addedSlots = new HashMap<>();
            for (Entry<String, Assignment> entry : newAssignments.entrySet()) {
                String topoId = entry.getKey();
                Assignment assignment = entry.getValue();
                Assignment existingAssignment = existingAssignments.get(topoId);
                if (existingAssignment == null) {
                    existingAssignment = new Assignment();
                    existingAssignment.set_executor_node_port(new HashMap<>());
                    existingAssignment.set_executor_start_time_secs(new HashMap<>());
                }
                Set<WorkerSlot> newSlots = newlyAddedSlots(existingAssignment, assignment);
                addedSlots.put(topoId, newSlots);
            }
            inimbus.assignSlots(topologies, addedSlots);
        }
    }

    private void notifyTopologyActionListener(String topoId, String action) {
        ITopologyActionNotifierPlugin notifier = nimbusTopologyActionNotifier;
        if (notifier != null) {
            try {
                notifier.notify(topoId, action);
            } catch (Exception e) {
                LOG.warn("Ignoring exception from Topology action notifier for storm-Id {}", topoId, e);
            }
        }
    }

    private void fixupBase(StormBase base, Map<String, Object> topoConf) {
        base.set_owner((String) topoConf.get(Config.TOPOLOGY_SUBMITTER_USER));
        base.set_principal((String) topoConf.get(Config.TOPOLOGY_SUBMITTER_PRINCIPAL));
    }

    // Topology may set custom heartbeat timeout.
    private int getTopologyHeartbeatTimeoutSecs(Map<String, Object> topoConf) {
        int defaultNimbusTimeout = ObjectReader.getInt(conf.get(DaemonConfig.NIMBUS_TASK_TIMEOUT_SECS));
        if (topoConf.containsKey(Config.TOPOLOGY_WORKER_TIMEOUT_SECS)) {
            int topoTimeout = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_WORKER_TIMEOUT_SECS));
            topoTimeout = Math.max(topoTimeout, defaultNimbusTimeout);
            return topoTimeout;
        }

        return defaultNimbusTimeout;
    }

    private int getTopologyHeartbeatTimeoutSecs(String topoId) {
        try {
            Map<String, Object> topoConf = tryReadTopoConf(topoId, topoCache);
            return getTopologyHeartbeatTimeoutSecs(topoConf);
        } catch (Exception e) {
            // contain any exception
            LOG.warn("Exception when getting heartbeat timeout.", e.getMessage());
            return ObjectReader.getInt(conf.get(DaemonConfig.NIMBUS_TASK_TIMEOUT_SECS));
        }
    }

    private int getTopologyLaunchHeartbeatTimeoutSec(String topoId) {
        int nimbusLaunchTimeout = ObjectReader.getInt(conf.get(DaemonConfig.NIMBUS_TASK_LAUNCH_SECS));
        int topoHeartbeatTimeoutSecs = getTopologyHeartbeatTimeoutSecs(topoId);
        return Math.max(nimbusLaunchTimeout, topoHeartbeatTimeoutSecs);
    }

    private void startTopology(String topoName, String topoId, TopologyStatus initStatus, String owner,
                               String principal, Map<String, Object> topoConf, StormTopology stormTopology)
        throws InvalidTopologyException {
        assert (TopologyStatus.ACTIVE == initStatus || TopologyStatus.INACTIVE == initStatus);
        Map<String, Integer> numExecutors = new HashMap<>();
        StormTopology topology = StormCommon.systemTopology(topoConf, stormTopology);
        for (Entry<String, Object> entry : StormCommon.allComponents(topology).entrySet()) {
            numExecutors.put(entry.getKey(), StormCommon.numStartExecutors(entry.getValue()));
        }
        LOG.info("Activating {}: {}", topoName, topoId);
        StormBase base = new StormBase();
        base.set_name(topoName);
        if (topoConf.containsKey(Config.TOPOLOGY_VERSION)) {
            base.set_topology_version(ObjectReader.getString(topoConf.get(Config.TOPOLOGY_VERSION)));
        }
        base.set_launch_time_secs(Time.currentTimeSecs());
        base.set_status(initStatus);
        base.set_num_workers(ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_WORKERS), 0));
        base.set_component_executors(numExecutors);
        base.set_owner(owner);
        base.set_principal(principal);
        base.set_component_debug(new HashMap<>());
        IStormClusterState state = stormClusterState;
        state.activateStorm(topoId, base, topoConf);
        idToExecutors.getAndUpdate(new Assoc<>(topoId,
            new HashSet<>(computeExecutors(base, topoConf, stormTopology))));
        notifyTopologyActionListener(topoName, "activate");
    }

    private void assertTopoActive(String topoName, boolean expectActive) throws NotAliveException, AlreadyAliveException {
        if (isTopologyActive(stormClusterState, topoName) != expectActive) {
            if (expectActive) {
                throw new WrappedNotAliveException(topoName + " is not alive");
            }
            throw new WrappedAlreadyAliveException(topoName + " is already alive");
        }
    }

    private Map<String, Object> tryReadTopoConfFromName(final String topoName) throws NotAliveException,
        AuthorizationException, IOException {
        return tryReadTopoConf(toTopoId(topoName), topoCache);
    }

    private StormTopology tryReadTopologyFromName(final String topoName) throws NotAliveException,
            AuthorizationException, IOException {
        return tryReadTopology(toTopoId(topoName), topoCache);
    }

    @VisibleForTesting
    public void checkAuthorization(String topoName, Map<String, Object> topoConf, String operation)
        throws AuthorizationException {
        checkAuthorization(topoName, topoConf, operation, null);
    }

    @VisibleForTesting
    public void checkAuthorization(String topoName, Map<String, Object> topoConf, String operation, ReqContext context)
            throws AuthorizationException {
        IAuthorizer impersonationAuthorizer = impersonationAuthorizationHandler;
        if (context == null) {
            context = ReqContext.context();
        }
        Map<String, Object> checkConf = new HashMap<>();
        if (topoConf != null) {
            checkConf.putAll(topoConf);
        } else if (topoName != null) {
            checkConf.put(Config.TOPOLOGY_NAME, topoName);
        }

        if (context.isImpersonating()) {
            LOG.info("principal: {} is trying to impersonate principal: {}", context.realPrincipal(), context.principal());
            if (impersonationAuthorizer == null) {
                LOG.warn("impersonation attempt but {} has no authorizer configured. potential security risk, "
                         + "please see SECURITY.MD to learn how to configure impersonation authorizer.",
                         DaemonConfig.NIMBUS_IMPERSONATION_AUTHORIZER);
            } else {
                if (!impersonationAuthorizer.permit(context, operation, checkConf)) {
                    ThriftAccessLogger.logAccess(context.requestID(), context.remoteAddress(),
                                                 context.principal(), operation, topoName, "access-denied");
                    throw new WrappedAuthorizationException("principal " + context.realPrincipal()
                                                     + " is not authorized to impersonate principal " + context.principal()
                                                     + " from host " + context.remoteAddress()
                                                     + " Please see SECURITY.MD to learn how to configure impersonation acls.");
                }
            }
        }

        IAuthorizer aclHandler = authorizationHandler;
        if (aclHandler != null) {
            if (!aclHandler.permit(context, operation, checkConf)) {
                ThriftAccessLogger.logAccess(context.requestID(), context.remoteAddress(), context.principal(), operation,
                                             topoName, "access-denied");
                throw new WrappedAuthorizationException(operation + (topoName != null ? " on topology " + topoName : "")
                                                 + " is not authorized");
            } else {
                ThriftAccessLogger.logAccess(context.requestID(), context.remoteAddress(), context.principal(),
                                             operation, topoName, "access-granted");
            }
        }
    }

    private boolean isAuthorized(String operation, String topoId) throws NotAliveException, AuthorizationException, IOException {
        Map<String, Object> topoConf = tryReadTopoConf(topoId, topoCache);
        topoConf = Utils.merge(conf, topoConf);
        String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);
        try {
            checkAuthorization(topoName, topoConf, operation);
            return true;
        } catch (AuthorizationException e) {
            return false;
        }
    }

    @VisibleForTesting
    public Set<String> filterAuthorized(String operation, Collection<String> topoIds) throws NotAliveException,
        AuthorizationException, IOException {
        Set<String> ret = new HashSet<>();
        for (String topoId : topoIds) {
            if (isAuthorized(operation, topoId)) {
                ret.add(topoId);
            }
        }
        return ret;
    }

    @VisibleForTesting
    public void rmDependencyJarsInTopology(String topoId) {
        try {
            BlobStore store = blobStore;
            IStormClusterState state = stormClusterState;
            StormTopology topo = readStormTopologyAsNimbus(topoId, topoCache);
            List<String> dependencyJars = topo.get_dependency_jars();
            LOG.info("Removing dependency jars from blobs - {}", dependencyJars);
            if (dependencyJars != null && !dependencyJars.isEmpty()) {
                for (String key : dependencyJars) {
                    rmBlobKey(store, key, state);
                }
            }
        } catch (Exception e) {
            //Yes eat the exception
            LOG.info("Exception {}", e);
        }
    }

    @VisibleForTesting
    public void rmTopologyKeys(String topoId) {
        BlobStore store = blobStore;
        IStormClusterState state = stormClusterState;
        try {
            topoCache.deleteTopoConf(topoId, NIMBUS_SUBJECT);
        } catch (Exception e) {
            //Just go on and try to delete the others
        }
        try {
            topoCache.deleteTopology(topoId, NIMBUS_SUBJECT);
        } catch (Exception e) {
            //Just go on and try to delte the others
        }
        rmBlobKey(store, ConfigUtils.masterStormJarKey(topoId), state);
    }

    @VisibleForTesting
    public void forceDeleteTopoDistDir(String topoId) throws IOException {
        Utils.forceDelete(ServerConfigUtils.masterStormDistRoot(conf, topoId));
    }

    @VisibleForTesting
    public void doCleanup() throws Exception {
        if (!isLeader()) {
            LOG.info("not a leader, skipping cleanup");
            return;
        }
        IStormClusterState state = stormClusterState;
        Set<String> toClean;
        synchronized (submitLock) {
            toClean = topoIdsToClean(state, blobStore, this.conf);
        }
        if (toClean != null) {
            for (String topoId : toClean) {
                LOG.info("Cleaning up {}", topoId);
                state.teardownHeartbeats(topoId);
                state.teardownTopologyErrors(topoId);
                state.removeAllPrivateWorkerKeys(topoId);
                state.removeBackpressure(topoId);
                rmDependencyJarsInTopology(topoId);
                forceDeleteTopoDistDir(topoId);
                rmTopologyKeys(topoId);
                heartbeatsCache.removeTopo(topoId);
                idToExecutors.getAndUpdate(new Dissoc<>(topoId));
            }
        }
    }

    /**
     * Deletes topologies from history older than mins minutes.
     *
     * @param mins the number of mins for old topologies
     */
    private void cleanTopologyHistory(int mins) {
        int cutoffAgeSecs = Time.currentTimeSecs() - (mins * 60);
        synchronized (topologyHistoryLock) {
            LocalState state = topologyHistoryState;
            state.filterOldTopologies(cutoffAgeSecs);
        }
    }

    private void addTopoToHistoryLog(String topoId, Map<String, Object> topoConf) {
        LOG.info("Adding topo to history log: {}", topoId);
        LocalState state = topologyHistoryState;
        List<String> users = ServerConfigUtils.getTopoLogsUsers(topoConf);
        List<String> groups = ServerConfigUtils.getTopoLogsGroups(topoConf);
        synchronized (topologyHistoryLock) {
            state.addTopologyHistory(new LSTopoHistory(topoId, Time.currentTimeSecs(), users, groups));
        }
    }

    private Set<String> userGroups(String user) throws IOException {
        if (user == null || user.isEmpty()) {
            return Collections.emptySet();
        }
        return groupMapper.getGroups(user);
    }

    /**
     * Check to see if any of the users groups intersect with the list of groups passed in.
     *
     * @param user          the user to check
     * @param groupsToCheck the groups to see if user is a part of
     * @return true if user is a part of groups, else false
     *
     * @throws IOException on any error
     */
    private boolean isUserPartOf(String user, Collection<String> groupsToCheck) throws IOException {
        Set<String> userGroups = new HashSet<>(userGroups(user));
        userGroups.retainAll(groupsToCheck);
        return !userGroups.isEmpty();
    }

    private List<String> readTopologyHistory(String user, Collection<String> adminUsers) throws IOException {
        LocalState state = topologyHistoryState;
        List<LSTopoHistory> topoHistoryList = state.getTopoHistoryList();
        if (topoHistoryList == null || topoHistoryList.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> ret = new ArrayList<>();
        for (LSTopoHistory history : topoHistoryList) {
            if (user == null || //Security off
                adminUsers.contains(user) || //is admin
                isUserPartOf(user, history.get_groups()) || //is in allowed group
                history.get_users().contains(user)) { //is an allowed user
                ret.add(history.get_topology_id());
            }
        }
        return ret;
    }

    private void renewCredentials() throws Exception {
        if (!isLeader()) {
            LOG.info("not a leader, skipping credential renewal.");
            return;
        }
        IStormClusterState state = stormClusterState;
        Collection<ICredentialsRenewer> renewers = credRenewers;
        Map<String, StormBase> assignedBases = state.topologyBases();
        if (assignedBases != null) {
            for (Entry<String, StormBase> entry : assignedBases.entrySet()) {
                String id = entry.getKey();
                String ownerPrincipal = entry.getValue().get_principal();
                Map<String, Object> topoConf = Collections.unmodifiableMap(Utils.merge(conf, tryReadTopoConf(id, topoCache)));
                synchronized (credUpdateLock) {
                    Credentials origCreds = state.credentials(id, null);
                    if (origCreds != null) {
                        Map<String, String> origCredsMap = origCreds.get_creds();
                        Map<String, String> newCredsMap = new HashMap<>(origCredsMap);
                        for (ICredentialsRenewer renewer : renewers) {
                            LOG.info("Renewing Creds For {} with {} owned by {}", id, renewer, ownerPrincipal);
                            renewer.renew(newCredsMap, topoConf, ownerPrincipal);
                        }
                        //Update worker tokens if needed
                        upsertWorkerTokensInCreds(newCredsMap, ownerPrincipal, id);
                        if (!newCredsMap.equals(origCredsMap)) {
                            state.setCredentials(id, new Credentials(newCredsMap), topoConf);
                        }
                    }
                }
            }
        }
    }

    private SupervisorSummary makeSupervisorSummary(String supervisorId, SupervisorInfo info) {
        Set<String> blacklistedSupervisorIds = Collections.emptySet();
        if (scheduler instanceof BlacklistScheduler) {
            BlacklistScheduler bs = (BlacklistScheduler) scheduler;
            blacklistedSupervisorIds = bs.getBlacklistSupervisorIds();
        }

        LOG.debug("INFO: {} ID: {}", info, supervisorId);
        int numPorts = 0;
        if (info.is_set_meta()) {
            numPorts = info.get_meta_size();
        }
        int numUsedPorts = 0;
        if (info.is_set_used_ports()) {
            numUsedPorts = info.get_used_ports_size();
        }
        LOG.debug("NUM PORTS: {}", numPorts);
        SupervisorSummary ret = new SupervisorSummary(info.get_hostname(),
                                                      (int) info.get_uptime_secs(), numPorts, numUsedPorts, supervisorId);
        ret.set_total_resources(info.get_resources_map());
        SupervisorResources resources = nodeIdToResources.get().get(supervisorId);
        if (resources != null && underlyingScheduler instanceof ResourceAwareScheduler) {
            ret.set_used_mem(resources.getUsedMem());
            ret.set_used_cpu(resources.getUsedCpu());
            ret.set_used_generic_resources(resources.getUsedGenericResources());
            if (isFragmented(resources)) {
                final double availableCpu = resources.getAvailableCpu();
                if (availableCpu < 0) {
                    LOG.warn("Negative fragmented CPU on {}", supervisorId);
                }
                ret.set_fragmented_cpu(availableCpu);
                final double availableMem = resources.getAvailableMem();
                if (availableMem < 0) {
                    LOG.warn("Negative fragmented Mem on {}", supervisorId);
                }
                ret.set_fragmented_mem(availableMem);
            }
        }
        if (info.is_set_version()) {
            ret.set_version(info.get_version());
        }

        if (blacklistedSupervisorIds.contains(supervisorId)) {
            ret.set_blacklisted(true);
        } else {
            ret.set_blacklisted(false);
        }

        return ret;
    }

    private ClusterSummary getClusterInfoImpl() throws Exception {
        IStormClusterState state = stormClusterState;
        Map<String, SupervisorInfo> infos = state.allSupervisorInfo();
        List<SupervisorSummary> summaries = new ArrayList<>(infos.size());
        for (Entry<String, SupervisorInfo> entry : infos.entrySet()) {
            summaries.add(makeSupervisorSummary(entry.getKey(), entry.getValue()));
        }
        int uptime = this.uptime.upTime();
        List<NimbusSummary> nimbuses = state.nimbuses();
        //update the isLeader field for each nimbus summary
        NimbusInfo leader = leaderElector.getLeader();
        for (NimbusSummary nimbusSummary : nimbuses) {
            nimbusSummary.set_uptime_secs(Time.deltaSecs(nimbusSummary.get_uptime_secs()));
            // sometimes Leader election indicates the current nimbus is leader, but the host was recently restarted,
            // and is currently not a leader.
            boolean isLeader = leader.getHost().equals(nimbusSummary.get_host()) && leader.getPort() == nimbusSummary.get_port();
            if (isLeader && this.nimbusHostPortInfo.getHost().equals(leader.getHost()) && !this.isLeader()) {
                isLeader = false;
            }
            nimbusSummary.set_isLeader(isLeader);
        }

        List<TopologySummary> topologySummaries = getTopologySummariesImpl();

        ClusterSummary ret = new ClusterSummary(summaries, topologySummaries, nimbuses);
        return ret;
    }

    private List<TopologySummary> getTopologySummariesImpl() throws IOException, TException {
        IStormClusterState state = stormClusterState;
        List<TopologySummary> topologySummaries = new ArrayList<>();
        Map<String, StormBase> bases = state.topologyBases();
        for (Entry<String, StormBase> entry : bases.entrySet()) {
            StormBase base = entry.getValue();
            if (base == null) {
                continue;
            }
            String topoId = entry.getKey();
            TopologySummary summary = getTopologySummaryImpl(topoId, base);
            topologySummaries.add(summary);
        }
        return topologySummaries;
    }

    private TopologySummary getTopologySummaryImpl(String topoId, StormBase base) throws IOException, TException {
        IStormClusterState state = stormClusterState;
        Assignment assignment = state.assignmentInfo(topoId, null);

        int numTasks = 0;
        int numExecutors = 0;
        int numWorkers = 0;
        if (assignment != null && assignment.is_set_executor_node_port()) {
            for (List<Long> ids : assignment.get_executor_node_port().keySet()) {
                numTasks += StormCommon.executorIdToTasks(ids).size();
            }

            numExecutors = assignment.get_executor_node_port_size();
            numWorkers = new HashSet<>(assignment.get_executor_node_port().values()).size();
        }

        TopologySummary summary = new TopologySummary(topoId, base.get_name(), numTasks, numExecutors, numWorkers,
                                                      Time.deltaSecs(base.get_launch_time_secs()), extractStatusStr(base));
        try {
            StormTopology topo = tryReadTopology(topoId, topoCache);
            if (topo != null && topo.is_set_storm_version()) {
                summary.set_storm_version(topo.get_storm_version());
            }
        } catch (NotAliveException e) {
            //Ignored it is not set
        }

        if (base.is_set_owner()) {
            summary.set_owner(base.get_owner());
        }

        if (base.is_set_topology_version()) {
            summary.set_topology_version(base.get_topology_version());
        }

        String status = idToSchedStatus.get().get(topoId);
        if (status != null) {
            summary.set_sched_status(status);
        }
        TopologyResources resources = getResourcesForTopology(topoId, base);
        if (resources != null) {
            summary.set_requested_memonheap(resources.getRequestedMemOnHeap());
            summary.set_requested_memoffheap(resources.getRequestedMemOffHeap());
            summary.set_requested_cpu(resources.getRequestedCpu());
            summary.set_requested_generic_resources(resources.getRequestedGenericResources());
            summary.set_assigned_memonheap(resources.getAssignedMemOnHeap());
            summary.set_assigned_memoffheap(resources.getAssignedMemOffHeap());
            summary.set_assigned_cpu(resources.getAssignedCpu());
            summary.set_assigned_generic_resources(resources.getAssignedGenericResources());
        }
        try {
            summary.set_replication_count(getBlobReplicationCount(ConfigUtils.masterStormCodeKey(topoId)));
        } catch (Exception e) {
            // This could fail if a blob gets deleted by mistake.  Don't crash nimbus.
            LOG.error("Unable to find blob entry", e);
        }
        return summary;
    }

    private void sendClusterMetricsToExecutors() throws Exception {
        ClusterInfo clusterInfo = mkClusterInfo();
        ClusterSummary clusterSummary = getClusterInfoImpl();
        List<DataPoint> clusterMetrics = extractClusterMetrics(clusterSummary);
        Map<IClusterMetricsConsumer.SupervisorInfo, List<DataPoint>> supervisorMetrics = extractSupervisorMetrics(clusterSummary);
        for (ClusterMetricsConsumerExecutor consumerExecutor : clusterConsumerExceutors) {
            consumerExecutor.handleDataPoints(clusterInfo, clusterMetrics);
            for (Entry<IClusterMetricsConsumer.SupervisorInfo, List<DataPoint>> entry : supervisorMetrics.entrySet()) {
                consumerExecutor.handleDataPoints(entry.getKey(), entry.getValue());
            }
        }
    }

    private CommonTopoInfo getCommonTopoInfo(String topoId, String operation) throws NotAliveException,
            AuthorizationException, IOException, InvalidTopologyException {
        CommonTopoInfo ret = new CommonTopoInfo();
        ret.topoConf = tryReadTopoConf(topoId, topoCache);
        ret.topoName = (String) ret.topoConf.get(Config.TOPOLOGY_NAME);
        checkAuthorization(ret.topoName, ret.topoConf, operation);
        StormTopology topology = tryReadTopology(topoId, topoCache);
        ret.topology = StormCommon.systemTopology(ret.topoConf, topology);
        ret.taskToComponent = StormCommon.stormTaskInfo(topology, ret.topoConf);
        IStormClusterState state = stormClusterState;
        ret.base = state.stormBase(topoId, null);
        if (ret.base != null && ret.base.is_set_launch_time_secs()) {
            ret.launchTimeSecs = ret.base.get_launch_time_secs();
        } else {
            ret.launchTimeSecs = 0;
        }
        ret.assignment = state.assignmentInfo(topoId, null);
        //get it from cluster state/zookeeper every time to collect the UI stats, may replace it with other StateStore later
        ret.beats = ret.assignment != null ? StatsUtil.convertExecutorBeats(state.executorBeats(topoId,
                                                                                                ret.assignment
                                                                                                    .get_executor_node_port())) :
            Collections
            .emptyMap();
        ret.allComponents = new HashSet<>(ret.taskToComponent.values());
        return ret;
    }
    
    @VisibleForTesting
    public boolean awaitLeadership(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return leaderElector.awaitLeadership(timeout, timeUnit);
    }

    @Override
    public void submitTopology(String name, String uploadedJarLocation, String jsonConf, StormTopology topology)
        throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, TException {
        submitTopologyCalls.mark();
        submitTopologyWithOpts(name, uploadedJarLocation, jsonConf, topology, new SubmitOptions(TopologyInitialStatus.ACTIVE));
    }

    private void upsertWorkerTokensInCreds(Map<String, String> creds, String user, String topologyId) {
        if (workerTokenManager != null) {
            workerTokenManager.upsertWorkerTokensInCredsForTopo(creds, user, topologyId);
        }
        //Remove any expired keys after possibly inserting new ones.
        stormClusterState.removeExpiredPrivateWorkerKeys(topologyId);
    }

    @Override
    public void submitTopologyWithOpts(String topoName, String uploadedJarLocation, String jsonConf,
                                       StormTopology topology, SubmitOptions options)
        throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, TException {
        try {
            submitTopologyWithOptsCalls.mark();
            assertIsLeader();
            assert (options != null);
            validateTopologyName(topoName);
            checkAuthorization(topoName, null, "submitTopology");
            assertTopoActive(topoName, false);
            @SuppressWarnings("unchecked")
            Map<String, Object> topoConf = (Map<String, Object>) JSONValue.parse(jsonConf);
            try {
                ConfigValidation.validateTopoConf(topoConf);
            } catch (IllegalArgumentException ex) {
                throw new WrappedInvalidTopologyException(ex.getMessage());
            }
            validator.validate(topoName, topoConf, topology);
            if ((boolean) conf.getOrDefault(Config.DISABLE_SYMLINKS, false)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> blobMap = (Map<String, Object>) topoConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
                if (blobMap != null && !blobMap.isEmpty()) {
                    throw new WrappedInvalidTopologyException("symlinks are disabled so blobs are not supported but "
                                                       + Config.TOPOLOGY_BLOBSTORE_MAP + " = " + blobMap);
                }
            }
            ServerUtils.validateTopologyWorkerMaxHeapSizeConfigs(topoConf, topology,
                                                     ObjectReader.getDouble(conf.get(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB)));
            Utils.validateTopologyBlobStoreMap(topoConf, blobStore);
            long uniqueNum = submittedCount.incrementAndGet();
            String topoId = topoName + "-" + uniqueNum + "-" + Time.currentTimeSecs();
            Map<String, String> creds = null;
            if (options.is_set_creds()) {
                creds = options.get_creds().get_creds();
            }
            topoConf.put(Config.STORM_ID, topoId);
            topoConf.put(Config.TOPOLOGY_NAME, topoName);
            topoConf = normalizeConf(conf, topoConf, topology);

            OciUtils.adjustImageConfigForTopo(conf, topoConf, topoId);

            ReqContext req = ReqContext.context();
            Principal principal = req.principal();
            String submitterPrincipal = principal == null ? null : principal.toString();
            Set<String> topoAcl = new HashSet<>(ObjectReader.getStrings(topoConf.get(Config.TOPOLOGY_USERS)));
            topoAcl.add(submitterPrincipal);
            String submitterUser = principalToLocal.toLocal(principal);
            topoAcl.add(submitterUser);

            String topologyPrincipal = Utils.OR(submitterPrincipal, "");
            topoConf.put(Config.TOPOLOGY_SUBMITTER_PRINCIPAL, topologyPrincipal);
            String systemUser = System.getProperty("user.name");
            String topologyOwner = Utils.OR(submitterUser, systemUser);
            topoConf.put(Config.TOPOLOGY_SUBMITTER_USER, topologyOwner); //Don't let the user set who we launch as
            topoConf.put(Config.TOPOLOGY_USERS, new ArrayList<>(topoAcl));
            topoConf.put(Config.STORM_ZOOKEEPER_SUPERACL, conf.get(Config.STORM_ZOOKEEPER_SUPERACL));
            if (!Utils.isZkAuthenticationConfiguredStormServer(conf)) {
                topoConf.remove(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME);
                topoConf.remove(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD);
            }
            if (!(Boolean) conf.getOrDefault(DaemonConfig.STORM_TOPOLOGY_CLASSPATH_BEGINNING_ENABLED, false)) {
                topoConf.remove(Config.TOPOLOGY_CLASSPATH_BEGINNING);
            }

            String topoVersionString = topology.get_storm_version();
            if (topoVersionString == null) {
                topoVersionString = (String) conf.getOrDefault(Config.SUPERVISOR_WORKER_DEFAULT_VERSION, VersionInfo.getVersion());
            }
            //Check if we can run a topology with that version of storm.
            SimpleVersion topoVersion = new SimpleVersion(topoVersionString);
            List<String> cp = Utils.getCompatibleVersion(supervisorClasspaths, topoVersion, "classpath", null);
            if (cp == null) {
                throw new WrappedInvalidTopologyException("Topology submitted with storm version " + topoVersionString
                                                   + " but could not find a configured compatible version to use "
                                                   + supervisorClasspaths.keySet());
            }
            Map<String, Object> otherConf = Utils.getConfigFromClasspath(cp, conf);
            Map<String, Object> totalConfToSave = Utils.merge(otherConf, topoConf);
            Map<String, Object> totalConf = Utils.merge(conf, totalConfToSave);


            //When reading the conf in nimbus we want to fall back to our own settings
            // if the other config does not have it set.
            topology = normalizeTopology(totalConf, topology);

            // if the Resource Aware Scheduler is used,
            // we might need to set the number of acker executors and eventlogger executors to be the estimated number of workers.
            if (ServerUtils.isRas(conf)) {
                int estimatedNumWorker = ServerUtils.getEstimatedWorkerCountForRasTopo(totalConf, topology);

                setUpAckerExecutorConfigs(topoName, totalConfToSave, totalConf, estimatedNumWorker);
                ServerUtils.validateTopologyAckerBundleResource(totalConfToSave, topology, topoName);

                int numEventLoggerExecs = ObjectReader.getInt(totalConf.get(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS), estimatedNumWorker);
                totalConfToSave.put(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, numEventLoggerExecs);
                LOG.debug("Config {} set to: {} for topology: {}",
                    Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, numEventLoggerExecs, topoName);

            }

            //Remove any configs that are specific to a host that might mess with the running topology.
            totalConfToSave.remove(Config.STORM_LOCAL_HOSTNAME); //Don't override the host name, or everything looks like it is on nimbus

            IStormClusterState state = stormClusterState;

            if (creds == null && workerTokenManager != null) {
                //Make sure we can store the worker tokens even if no creds are provided.
                creds = new HashMap<>();
            }
            if (creds != null) {
                Map<String, Object> finalConf = Collections.unmodifiableMap(topoConf);
                for (INimbusCredentialPlugin autocred : nimbusAutocredPlugins) {
                    autocred.populateCredentials(creds, finalConf);
                }
                upsertWorkerTokensInCreds(creds, topologyPrincipal, topoId);
            }

            if (ObjectReader.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false)
                && (submitterUser == null || submitterUser.isEmpty())) {
                throw new WrappedAuthorizationException("Could not determine the user to run this topology as.");
            }
            StormCommon.systemTopology(totalConf, topology); //this validates the structure of the topology
            validateTopologySize(topoConf, conf, topology);
            if (Utils.isZkAuthenticationConfiguredStormServer(conf)
                && !Utils.isZkAuthenticationConfiguredTopology(topoConf)) {
                throw new IllegalArgumentException("The cluster is configured for zookeeper authentication, but no payload was provided.");
            }
            LOG.info("Received topology submission for {} (storm-{} JDK-{}) with conf {}", topoName,
                     topoVersionString, topology.get_jdk_version(), ConfigUtils.maskPasswords(topoConf));

            // lock protects against multiple topologies being submitted at once and
            // cleanup thread killing topology in b/w assignment and starting the topology
            synchronized (submitLock) {
                assertTopoActive(topoName, false);
                //cred-update-lock is not needed here because creds are being added for the first time.
                if (creds != null) {
                    state.setCredentials(topoId, new Credentials(creds), topoConf);
                }
                LOG.info("uploadedJar {} for {}", uploadedJarLocation, topoName);
                setupStormCode(conf, topoId, uploadedJarLocation, totalConfToSave, topology);
                waitForDesiredCodeReplication(totalConf, topoId);
                state.setupHeatbeats(topoId, topoConf);
                state.setupErrors(topoId, topoConf);
                if (ObjectReader.getBoolean(totalConf.get(Config.TOPOLOGY_BACKPRESSURE_ENABLE), false)) {
                    state.setupBackpressure(topoId, topoConf);
                }
                notifyTopologyActionListener(topoName, "submitTopology");
                TopologyStatus status = null;
                switch (options.get_initial_status()) {
                    case INACTIVE:
                        status = TopologyStatus.INACTIVE;
                        break;
                    case ACTIVE:
                        status = TopologyStatus.ACTIVE;
                        break;
                    default:
                        throw new IllegalArgumentException("Inital Status of " + options.get_initial_status() + " is not allowed.");

                }
                startTopology(topoName, topoId, status, topologyOwner, topologyPrincipal, totalConfToSave, topology);
            }
        } catch (Exception e) {
            LOG.warn("Topology submission exception. (topology name='{}')", topoName, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    public static void setUpAckerExecutorConfigs(String topoName, Map<String, Object> totalConfToSave,
                                                 Map<String, Object> totalConf, int estimatedNumWorker) {

        int numAckerExecs;
        int numAckerExecsPerWorker;

        if (totalConf.get(Config.TOPOLOGY_ACKER_EXECUTORS) == null) {
            numAckerExecsPerWorker = ObjectReader.getInt(
                totalConf.get(Config.TOPOLOGY_RAS_ACKER_EXECUTORS_PER_WORKER));
            numAckerExecs = estimatedNumWorker * numAckerExecsPerWorker;
        } else {
            numAckerExecs = ObjectReader.getInt(totalConf.get(Config.TOPOLOGY_ACKER_EXECUTORS));
            if (estimatedNumWorker == 0) {
                numAckerExecsPerWorker = 0;
            } else {
                numAckerExecsPerWorker = (int) Math.ceil((double) numAckerExecs / (double) estimatedNumWorker);
            }
        }

        totalConfToSave.put(Config.TOPOLOGY_RAS_ACKER_EXECUTORS_PER_WORKER, numAckerExecsPerWorker);
        totalConfToSave.put(Config.TOPOLOGY_ACKER_EXECUTORS, numAckerExecs);

        LOG.info("Config {} set to: {} for topology: {}",
            Config.TOPOLOGY_RAS_ACKER_EXECUTORS_PER_WORKER, numAckerExecsPerWorker, topoName);
        LOG.info("Config {} set to: {} for topology: {}",
            Config.TOPOLOGY_ACKER_EXECUTORS, numAckerExecs, topoName);

    }

    @Override
    public void killTopology(String name) throws NotAliveException, AuthorizationException, TException {
        killTopologyCalls.mark();
        killTopologyWithOpts(name, new KillOptions());
    }

    @Override
    public void killTopologyWithOpts(final String topoName, final KillOptions options)
        throws NotAliveException, AuthorizationException, TException {
        killTopologyWithOptsCalls.mark();
        assertTopoActive(topoName, true);
        try {
            Map<String, Object> topoConf = tryReadTopoConfFromName(topoName);
            topoConf = Utils.merge(conf, topoConf);
            final String operation = "killTopology";
            checkAuthorization(topoName, topoConf, operation);
            Integer waitAmount = null;
            if (options.is_set_wait_secs()) {
                waitAmount = options.get_wait_secs();
            }
            transitionName(topoName, TopologyActions.KILL, waitAmount, true);
            notifyTopologyActionListener(topoName, operation);
            addTopoToHistoryLog((String) topoConf.get(Config.STORM_ID), topoConf);
        } catch (Exception e) {
            LOG.warn("Kill topology exception. (topology name='{}')", topoName, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void activate(String topoName) throws NotAliveException, AuthorizationException, TException {
        activateCalls.mark();
        try {
            Map<String, Object> topoConf = tryReadTopoConfFromName(topoName);
            topoConf = Utils.merge(conf, topoConf);
            final String operation = "activate";
            checkAuthorization(topoName, topoConf, operation);
            transitionName(topoName, TopologyActions.ACTIVATE, null, true);
            notifyTopologyActionListener(topoName, operation);
        } catch (Exception e) {
            LOG.warn("Activate topology exception. (topology name='{}')", topoName, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deactivate(String topoName) throws NotAliveException, AuthorizationException, TException {
        deactivateCalls.mark();
        try {
            Map<String, Object> topoConf = tryReadTopoConfFromName(topoName);
            topoConf = Utils.merge(conf, topoConf);
            final String operation = "deactivate";
            checkAuthorization(topoName, topoConf, operation);
            transitionName(topoName, TopologyActions.INACTIVATE, null, true);
            notifyTopologyActionListener(topoName, operation);
        } catch (Exception e) {
            LOG.warn("Deactivate topology exception. (topology name='{}')", topoName, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rebalance(String topoName, RebalanceOptions options)
        throws NotAliveException, InvalidTopologyException, AuthorizationException, TException {
        rebalanceCalls.mark();
        assertTopoActive(topoName, true);
        try {
            Map<String, Object> topoConf = tryReadTopoConfFromName(topoName);
            topoConf = Utils.merge(conf, topoConf);
            final String operation = "rebalance";
            checkAuthorization(topoName, topoConf, operation);
            // Set principal in RebalanceOptions to nil because users are not suppose to set this
            options.set_principal(null);
            // check if executor counts are correctly specified
            StormTopology stormTopology = tryReadTopologyFromName(topoName);
            Set<String> comps = new TreeSet<>();
            comps.addAll(stormTopology.get_spouts().keySet());
            comps.addAll(stormTopology.get_bolts().keySet());
            Map<String, Integer> execOverrides = options.is_set_num_executors() ? options.get_num_executors() : Collections.emptyMap();
            for (Map.Entry<String, Integer> e: execOverrides.entrySet()) {
                String comp = e.getKey();
                // validate non-system component ids
                if (!Utils.isSystemId(comp) && !comps.contains(comp)) {
                    throw new WrappedInvalidTopologyException(
                            String.format("Invalid component %s for topology %s, valid values are %s",
                                    comp, topoName, String.join(",", comps))
                    );
                }
                // validate executor count for component
                Integer value = e.getValue();
                if (value == null || value <= 0) {
                    throw new WrappedInvalidTopologyException("Number of executors must be greater than 0");
                }
            }
            if (options.is_set_topology_conf_overrides()) {
                Map<String, Object> topoConfigOverrides = Utils.parseJson(options.get_topology_conf_overrides());
                //Clean up some things the user should not set.  (Not a security issue, just might confuse the topology)
                topoConfigOverrides.remove(Config.TOPOLOGY_SUBMITTER_PRINCIPAL);
                topoConfigOverrides.remove(Config.TOPOLOGY_SUBMITTER_USER);
                topoConfigOverrides.remove(Config.STORM_ZOOKEEPER_SUPERACL);
                topoConfigOverrides.remove(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME);
                topoConfigOverrides.remove(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD);
                if ((boolean) conf.getOrDefault(DaemonConfig.STORM_TOPOLOGY_CLASSPATH_BEGINNING_ENABLED, false)) {
                    topoConfigOverrides.remove(Config.TOPOLOGY_CLASSPATH_BEGINNING);
                }
                topoConfigOverrides.remove(Config.STORM_LOCAL_HOSTNAME);
                options.set_topology_conf_overrides(JSONValue.toJSONString(topoConfigOverrides));
            }
            Subject subject = getSubject();
            if (subject != null) {
                options.set_principal(subject.getPrincipals().iterator().next().getName());
            }

            transitionName(topoName, TopologyActions.REBALANCE, options, true);
            notifyTopologyActionListener(topoName, operation);
        } catch (Exception e) {
            LOG.warn("rebalance topology exception. (topology name='{}')", topoName, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setLogConfig(String topoId, LogConfig config) throws TException {
        try {
            setLogConfigCalls.mark();
            Map<String, Object> topoConf = tryReadTopoConf(topoId, topoCache);
            topoConf = Utils.merge(conf, topoConf);
            String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);
            checkAuthorization(topoName, topoConf, "setLogConfig");
            IStormClusterState state = stormClusterState;
            LogConfig mergedLogConfig = state.topologyLogConfig(topoId, null);
            if (mergedLogConfig == null) {
                mergedLogConfig = new LogConfig();
            }

            if (mergedLogConfig.is_set_named_logger_level()) {
                Map<String, LogLevel> namedLoggers = mergedLogConfig.get_named_logger_level();
                for (LogLevel level : namedLoggers.values()) {
                    level.set_action(LogLevelAction.UNCHANGED);
                }
            }

            if (config.is_set_named_logger_level()) {
                for (Entry<String, LogLevel> entry : config.get_named_logger_level().entrySet()) {
                    LogLevel logConfig = entry.getValue();
                    String loggerName = entry.getKey();
                    LogLevelAction action = logConfig.get_action();
                    if (loggerName.isEmpty()) {
                        throw new RuntimeException("Named loggers need a valid name. Use ROOT for the root logger");
                    }
                    switch (action) {
                        case UPDATE:
                            setLoggerTimeouts(logConfig);
                            mergedLogConfig.put_to_named_logger_level(loggerName, logConfig);
                            break;
                        case REMOVE:
                            Map<String, LogLevel> nl = mergedLogConfig.get_named_logger_level();
                            if (nl != null) {
                                nl.remove(loggerName);
                            }
                            break;
                        default:
                            //NOOP
                            break;
                    }
                }
            }
            LOG.info("Setting log config for {}:{}", topoName, mergedLogConfig);
            state.setTopologyLogConfig(topoId, mergedLogConfig, topoConf);
        } catch (Exception e) {
            LOG.warn("set log config topology exception. (topology id='{}')", topoId, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public LogConfig getLogConfig(String topoId) throws TException {
        try {
            getLogConfigCalls.mark();
            Map<String, Object> topoConf = tryReadTopoConf(topoId, topoCache);
            topoConf = Utils.merge(conf, topoConf);
            String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);
            checkAuthorization(topoName, topoConf, "getLogConfig");
            IStormClusterState state = stormClusterState;
            LogConfig logConfig = state.topologyLogConfig(topoId, null);
            if (logConfig == null) {
                logConfig = new LogConfig();
            }
            return logConfig;
        } catch (Exception e) {
            LOG.warn("get log conf topology exception. (topology id='{}')", topoId, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void debug(String topoName, String componentId, boolean enable, double samplingPercentage)
        throws NotAliveException, AuthorizationException, TException {
        debugCalls.mark();
        try {
            IStormClusterState state = stormClusterState;
            String topoId = toTopoId(topoName);
            Map<String, Object> topoConf = tryReadTopoConf(topoId, topoCache);
            topoConf = Utils.merge(conf, topoConf);
            // make sure samplingPct is within bounds.
            double spct = Math.max(Math.min(samplingPercentage, 100.0), 0.0);
            // while disabling we retain the sampling pct.
            checkAuthorization(topoName, topoConf, "debug");
            if (topoId == null) {
                throw new WrappedNotAliveException(topoName);
            }

            DebugOptions options = new DebugOptions();
            options.set_enable(enable);
            if (enable) {
                options.set_samplingpct(spct);
            }
            StormBase updates = new StormBase();
            //For backwards compatability
            updates.set_component_executors(Collections.emptyMap());
            boolean hasCompId = componentId != null && !componentId.isEmpty();
            String key = hasCompId ? componentId : topoId;
            updates.put_to_component_debug(key, options);

            LOG.info("Nimbus setting debug to {} for storm-name '{}' storm-id '{}' sanpling pct '{}'"
                     + (hasCompId ? " component-id '" + componentId + "'" : ""),
                     enable, topoName, topoId, spct);
            synchronized (submitLock) {
                state.updateStorm(topoId, updates);
            }
        } catch (Exception e) {
            LOG.warn("debug topology exception. (topology name='{}')", topoName, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setWorkerProfiler(String topoId, ProfileRequest profileRequest) throws TException {
        try {
            setWorkerProfilerCalls.mark();
            Map<String, Object> topoConf = tryReadTopoConf(topoId, topoCache);
            topoConf = Utils.merge(conf, topoConf);
            String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);
            checkAuthorization(topoName, topoConf, "setWorkerProfiler");
            IStormClusterState state = stormClusterState;
            state.setWorkerProfileRequest(topoId, profileRequest);
        } catch (Exception e) {
            LOG.warn("set worker profiler topology exception. (topology id='{}')", topoId, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<ProfileRequest> getComponentPendingProfileActions(String id, String componentId, ProfileAction action)
        throws TException {
        try {
            getComponentPendingProfileActionsCalls.mark();
            CommonTopoInfo info = getCommonTopoInfo(id, "getComponentPendingProfileActions");
            Map<List<? extends Number>, List<Object>> exec2hostPort = new HashMap<>();
            if (info.assignment != null) {
                Map<String, String> nodeToHost = info.assignment.get_node_host();
                for (Entry<List<Long>, NodeInfo> entry : info.assignment.get_executor_node_port().entrySet()) {
                    NodeInfo ni = entry.getValue();
                    List<Object> hostPort = Arrays.asList(nodeToHost.get(ni.get_node()), ni.get_port_iterator().next().intValue());
                    exec2hostPort.put(entry.getKey(), hostPort);
                }
            }
            List<Map<String, Object>> nodeInfos =
                StatsUtil.extractNodeInfosFromHbForComp(exec2hostPort, info.taskToComponent, false, componentId);
            List<ProfileRequest> ret = new ArrayList<>();
            for (Map<String, Object> ni : nodeInfos) {
                String niHost = (String) ni.get("host");
                int niPort = ((Integer) ni.get("port")).intValue();
                ProfileRequest newestMatch = null;
                long reqTime = -1;
                for (ProfileRequest req : stormClusterState.getTopologyProfileRequests(id)) {
                    String expectedHost = req.get_nodeInfo().get_node();
                    int expectedPort = req.get_nodeInfo().get_port_iterator().next().intValue();
                    ProfileAction expectedAction = req.get_action();
                    if (niHost.equals(expectedHost) && niPort == expectedPort && action == expectedAction) {
                        long time = req.get_time_stamp();
                        if (time > reqTime) {
                            reqTime = time;
                            newestMatch = req;
                        }
                    }
                }
                if (newestMatch != null) {
                    ret.add(newestMatch);
                }
            }
            LOG.info("Latest profile actions for topology {} component {} {}", id, componentId, ret);
            return ret;
        } catch (Exception e) {
            LOG.warn("Get comp actions topology exception. (topology id='{}')", id, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void uploadNewCredentials(String topoName, Credentials credentials)
        throws NotAliveException, InvalidTopologyException, AuthorizationException, TException {
        try {
            uploadNewCredentialsCalls.mark();
            IStormClusterState state = stormClusterState;
            String topoId = toTopoId(topoName);
            if (topoId == null) {
                throw new WrappedNotAliveException(topoName + " is not alive");
            }
            Map<String, Object> topoConf = tryReadTopoConf(topoId, topoCache);
            topoConf = Utils.merge(conf, topoConf);
            if (credentials == null) {
                credentials = new Credentials(Collections.emptyMap());
            }
            checkAuthorization(topoName, topoConf, "uploadNewCredentials");
            String realPrincipal = (String) topoConf.get(Config.TOPOLOGY_SUBMITTER_PRINCIPAL);
            String realUser = (String) topoConf.get(Config.TOPOLOGY_SUBMITTER_USER);
            String expectedOwner = null;
            if (credentials.is_set_topoOwner()) {
                expectedOwner = credentials.get_topoOwner();
            } else {
                Principal p = ReqContext.context().principal();
                if (p != null) {
                    expectedOwner = p.getName();
                }
            }
            // expectedOwner being null means that security is disabled (which why are we uploading credentials with security disabled???
            if (expectedOwner == null) {
                LOG.warn("Please check you settings. Credentials are being uploaded to {} with security disabled.", topoId);
            } else if (!realPrincipal.equals(expectedOwner) && !realUser.equals(expectedOwner)) {
                throw new AuthorizationException(topoId + " is expected to be owned by " + expectedOwner
                    + " but is actually owned by " + realPrincipal);
            }

            synchronized (credUpdateLock) {
                //Merge the old credentials so creds nimbus created are not lost.
                // And in case the user forgot to upload something important this time.
                Credentials origCreds = state.credentials(topoId, null);
                if (origCreds != null) {
                    Map<String, String> mergedCreds = origCreds.get_creds();
                    mergedCreds.putAll(credentials.get_creds());
                    credentials.set_creds(mergedCreds);
                }
                state.setCredentials(topoId, credentials, topoConf);
            }
        } catch (Exception e) {
            LOG.warn("Upload Creds topology exception. (topology name='{}')", topoName, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public String beginCreateBlob(String key, SettableBlobMeta meta)
        throws AuthorizationException, KeyAlreadyExistsException, TException {
        try {
            String sessionId = Utils.uuid();
            blobUploaders.put(sessionId, blobStore.createBlob(key, meta, getSubject()));
            LOG.info("Created blob {} for session {}", key, sessionId);
            return sessionId;
        } catch (Exception e) {
            LOG.warn("begin create blob exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public String beginUpdateBlob(String key) throws AuthorizationException, KeyNotFoundException, TException {
        try {
            String sessionId = Utils.uuid();
            blobUploaders.put(sessionId, blobStore.updateBlob(key, getSubject()));
            LOG.info("Created upload session for {}", key);
            return sessionId;
        } catch (Exception e) {
            LOG.warn("begin update blob exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void uploadBlobChunk(String session, ByteBuffer chunk) throws AuthorizationException, TException {
        try {
            OutputStream os = blobUploaders.get(session);
            if (os == null) {
                throw new RuntimeException("Blob for session " + session + " does not exist (or timed out)");
            }
            byte[] array = chunk.array();
            int remaining = chunk.remaining();
            int offset = chunk.arrayOffset();
            int position = chunk.position();
            os.write(array, offset + position, remaining);
            blobUploaders.put(session, os);
        } catch (Exception e) {
            LOG.warn("upload blob chunk exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void finishBlobUpload(String session) throws AuthorizationException, TException {
        try {
            OutputStream os = blobUploaders.get(session);
            if (os == null) {
                throw new RuntimeException("Blob for session " + session + " does not exist (or timed out)");
            }
            os.close();
            LOG.info("Finished uploading blob for session {}. Closing session.", session);
            blobUploaders.remove(session);
            blobStore.updateLastBlobUpdateTime();
        } catch (Exception e) {
            LOG.warn("finish blob upload exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void cancelBlobUpload(String session) throws AuthorizationException, TException {
        try {
            AtomicOutputStream os = (AtomicOutputStream) blobUploaders.get(session);
            if (os == null) {
                throw new RuntimeException("Blob for session " + session + " does not exist (or timed out)");
            }
            os.cancel();
            LOG.info("Canceled uploading blob for session {}. Closing session.", session);
            blobUploaders.remove(session);
        } catch (Exception e) {
            LOG.warn("finish blob upload exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public ReadableBlobMeta getBlobMeta(String key) throws AuthorizationException, KeyNotFoundException, TException {
        try {
            return blobStore.getBlobMeta(key, getSubject());
        } catch (Exception e) {
            LOG.warn("get blob meta exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setBlobMeta(String key, SettableBlobMeta meta)
        throws AuthorizationException, KeyNotFoundException, TException {
        try {
            blobStore.setBlobMeta(key, meta, getSubject());
            blobStore.updateLastBlobUpdateTime();
        } catch (Exception e) {
            LOG.warn("set blob meta exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public BeginDownloadResult beginBlobDownload(String key)
        throws AuthorizationException, KeyNotFoundException, TException {
        try {
            InputStreamWithMeta is = blobStore.getBlob(key, getSubject());
            String sessionId = Utils.uuid();
            BeginDownloadResult ret = new BeginDownloadResult(is.getVersion(), sessionId);
            ret.set_data_size(is.getFileLength());
            blobDownloaders.put(sessionId, new BufferInputStream(is,
                                                                 (int) conf
                                                                     .getOrDefault(Config.STORM_BLOBSTORE_INPUTSTREAM_BUFFER_SIZE_BYTES,
                                                                                   65536)));
            LOG.info("Created download session {} for {}", sessionId, key);
            return ret;
        } catch (Exception e) {
            LOG.warn("begin blob download exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public ByteBuffer downloadBlobChunk(String session) throws AuthorizationException, TException {
        try {
            BufferInputStream is = blobDownloaders.get(session);
            if (is == null) {
                throw new RuntimeException("Blob for session " + session + " does not exist (or timed out)");
            }
            byte[] ret = is.read();
            if (ret.length == 0) {
                is.close();
                blobDownloaders.remove(session);
            } else {
                blobDownloaders.put(session, is);
            }
            LOG.debug("Sending {} bytes", ret.length);
            return ByteBuffer.wrap(ret);
        } catch (Exception e) {
            LOG.warn("download blob chunk exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteBlob(String key) throws AuthorizationException, KeyNotFoundException, IllegalStateException, TException {
        try {
            String topoName = ConfigUtils.getIdFromBlobKey(key);
            if (topoName != null) {
                if (isTopologyActiveOrActivating(stormClusterState, topoName)) {
                    String message = "Attempting to delete blob " + key + " from under active topology " + topoName;
                    LOG.warn(message);
                    throw new WrappedIllegalStateException(message);
                }
            }
            String topoId = topologyUsingThisBlob(stormClusterState, topoCache,  key);
            if (topoId != null) {
                String message = "Attempting to delete active blob " + key + " used by topology " + topoId;
                LOG.warn(message);
                throw new WrappedIllegalStateException(message);
            }
            blobStore.deleteBlob(key, getSubject());
            LOG.info("Deleted blob for key {} with {}", key, ReqContext.context());
        } catch (Exception e) {
            LOG.warn("delete blob exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public ListBlobsResult listBlobs(String session) throws TException {
        try {
            Iterator<String> keyIt;
            //Create a new session id if the user gave an empty session string.
            // This is the use case when the user wishes to list blobs
            // starting from the beginning.
            if (session == null || session.isEmpty()) {
                keyIt = blobStore.listKeys();
                session = Utils.uuid();
            } else {
                keyIt = blobListers.get(session);
            }

            if (keyIt == null) {
                throw new RuntimeException("Blob list for session " + session + " does not exist (or timed out)");
            }

            if (!keyIt.hasNext()) {
                blobListers.remove(session);
                LOG.info("No more blobs to list for session {}", session);
                // A blank result communicates that there are no more blobs.
                return new ListBlobsResult(Collections.emptyList(), session);
            }

            ArrayList<String> listChunk = new ArrayList<>();
            for (int i = 0; i < 100 && keyIt.hasNext(); i++) {
                listChunk.add(keyIt.next());
            }
            blobListers.put(session, keyIt);
            LOG.info("Downloading {} entries", listChunk.size());
            return new ListBlobsResult(listChunk, session);
        } catch (Exception e) {
            LOG.warn("list blobs exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getBlobReplication(String key) throws AuthorizationException, KeyNotFoundException, TException {
        try {
            return blobStore.getBlobReplication(key, getSubject());
        } catch (Exception e) {
            LOG.warn("get blob replication exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public int updateBlobReplication(String key, int replication)
        throws AuthorizationException, KeyNotFoundException, TException {
        try {
            int result = blobStore.updateBlobReplication(key, replication, getSubject());
            blobStore.updateLastBlobUpdateTime();
            return result;
        } catch (Exception e) {
            LOG.warn("update blob replication exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createStateInZookeeper(String key) throws TException {
        try {
            IStormClusterState state = stormClusterState;
            BlobStore store = blobStore;
            NimbusInfo ni = nimbusHostPortInfo;
            if (store instanceof LocalFsBlobStore) {
                state.setupBlob(key, ni, getVersionForKey(key, ni, zkClient));
            }
            LOG.debug("Created state in zookeeper {} {} {}", state, store, ni);
        } catch (Exception e) {
            LOG.warn("Exception while creating state in zookeeper - key: " + key, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public String beginFileUpload() throws AuthorizationException, TException {
        try {
            beginFileUploadCalls.mark();
            assertIsLeader();
            checkAuthorization(null, null, "fileUpload");
            String fileloc = getInbox() + "/stormjar-" + Utils.uuid() + ".jar";
            uploaders.put(fileloc, new TimedWritableByteChannel(Channels.newChannel(new FileOutputStream(fileloc)), fileUploadDuration));
            LOG.info("Uploading file from client to {}", fileloc);
            return fileloc;
        } catch (Exception e) {
            LOG.warn("Begin file upload exception", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void uploadChunk(String location, ByteBuffer chunk) throws AuthorizationException, TException {
        try {
            uploadChunkCalls.mark();
            checkAuthorization(null, null, "fileUpload");
            WritableByteChannel channel = uploaders.get(location);
            if (channel == null) {
                throw new RuntimeException("File for that location does not exist (or timed out)");
            }
            channel.write(chunk);
            uploaders.put(location, channel);
        } catch (Exception e) {
            LOG.warn("uploadChunk exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void finishFileUpload(String location) throws AuthorizationException, TException {
        try {
            finishFileUploadCalls.mark();
            checkAuthorization(null, null, "fileUpload");
            WritableByteChannel channel = uploaders.get(location);
            if (channel == null) {
                throw new RuntimeException("File for that location does not exist (or timed out)");
            }
            channel.close();
            LOG.info("Finished uploading file from client: {}", location);
            uploaders.remove(location);
            blobStore.updateLastBlobUpdateTime();
        } catch (Exception e) {
            LOG.warn("finish file upload exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public ByteBuffer downloadChunk(String id) throws AuthorizationException, TException {
        try {
            downloadChunkCalls.mark();
            checkAuthorization(null, null, "fileDownload");
            BufferInputStream is = downloaders.get(id);
            if (is == null) {
                throw new RuntimeException("Could not find input stream for id " + id);
            }
            byte[] ret = is.read();
            if (ret.length == 0) {
                is.close();
                downloaders.remove(id);
            }
            return ByteBuffer.wrap(ret);
        } catch (Exception e) {
            LOG.warn("download chunk exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getNimbusConf() throws AuthorizationException, TException {
        try {
            getNimbusConfCalls.mark();
            checkAuthorization(null, null, "getNimbusConf");
            return JSONValue.toJSONString(conf);
        } catch (Exception e) {
            LOG.warn("get nimbus conf exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public TopologyInfo getTopologyInfo(String id) throws NotAliveException, AuthorizationException, TException {
        try {
            getTopologyInfoCalls.mark();
            GetInfoOptions options = new GetInfoOptions();
            options.set_num_err_choice(NumErrorsChoice.ALL);
            return getTopologyInfoWithOptsImpl(id, options);
        } catch (Exception e) {
            LOG.warn("get topology ino exception. (topology id={})", id, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public TopologyInfo getTopologyInfoByName(String name) throws TException {
        try {
            getTopologyInfoByNameCalls.mark();
            GetInfoOptions options = new GetInfoOptions();
            options.set_num_err_choice(NumErrorsChoice.ALL);
            return getTopologyInfoByNameImpl(name, options);
        } catch (Exception e) {
            LOG.warn("get topology info exception. (topology name={})", name, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    private TopologyInfo getTopologyInfoByNameImpl(String name, GetInfoOptions options)
        throws NotAliveException, AuthorizationException, Exception {
        IStormClusterState state = stormClusterState;
        String id = state.getTopoId(name).orElseThrow(() -> new WrappedNotAliveException(name + " is not alive"));
        return getTopologyInfoWithOptsImpl(id, options);
    }

    @Override
    public TopologyInfo getTopologyInfoByNameWithOpts(String name, GetInfoOptions options)
        throws NotAliveException, AuthorizationException, TException {
        try {
            getTopologyInfoByNameWithOptsCalls.mark();
            return getTopologyInfoByNameImpl(name, options);
        } catch (Exception e) {
            LOG.warn("get topology info withOpts by name exception. (topology name={})", name, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public TopologyInfo getTopologyInfoWithOpts(String topoId, GetInfoOptions options)
        throws NotAliveException, AuthorizationException, TException {
        getTopologyInfoWithOptsCalls.mark();

        try {
            return getTopologyInfoWithOptsImpl(topoId, options);
        } catch (Exception e) {
            LOG.warn("Get topo info exception. (topology id='{}')", topoId, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    private TopologyInfo getTopologyInfoWithOptsImpl(String topoId, GetInfoOptions options) throws NotAliveException,
        AuthorizationException, InvalidTopologyException, Exception {
        CommonTopoInfo common = getCommonTopoInfo(topoId, "getTopologyInfo");
        if (common.base == null) {
            throw new WrappedNotAliveException(topoId);
        }
        IStormClusterState state = stormClusterState;
        NumErrorsChoice numErrChoice = Utils.OR(options.get_num_err_choice(), NumErrorsChoice.ALL);
        Map<String, List<ErrorInfo>> errors = new HashMap<>();
        for (String component : common.allComponents) {
            switch (numErrChoice) {
                case NONE:
                    errors.put(component, Collections.emptyList());
                    break;
                case ONE:
                    List<ErrorInfo> errList = new ArrayList<>();
                    ErrorInfo info = state.lastError(topoId, component);
                    if (info != null) {
                        errList.add(info);
                    }
                    errors.put(component, errList);
                    break;
                case ALL:
                    errors.put(component, state.errors(topoId, component));
                    break;
                default:
                    LOG.warn("Got invalid NumErrorsChoice '{}'", numErrChoice);
                    errors.put(component, state.errors(topoId, component));
                    break;
            }
        }

        List<ExecutorSummary> summaries = new ArrayList<>();
        if (common.assignment != null) {
            for (Entry<List<Long>, NodeInfo> entry : common.assignment.get_executor_node_port().entrySet()) {
                NodeInfo ni = entry.getValue();
                ExecutorInfo execInfo = toExecInfo(entry.getKey());
                Map<String, String> nodeToHost = common.assignment.get_node_host();
                Map<String, Object> heartbeat = common.beats.get(ClientStatsUtil.convertExecutor(entry.getKey()));
                if (heartbeat == null) {
                    heartbeat = Collections.emptyMap();
                }
                ExecutorSummary summ = new ExecutorSummary(execInfo,
                                                           common.taskToComponent.get(execInfo.get_task_start()),
                                                           nodeToHost.get(ni.get_node()), ni.get_port_iterator().next().intValue(),
                                                           (Integer) heartbeat.getOrDefault("uptime", 0));

                //heartbeats "stats"
                Map ex = (Map) heartbeat.get("stats");
                if (ex != null) {
                    ExecutorStats stats = StatsUtil.thriftifyExecutorStats(ex);
                    summ.set_stats(stats);
                }
                summaries.add(summ);
            }
        }
        TopologyInfo topoInfo = new TopologyInfo(topoId, common.topoName, Time.deltaSecs(common.launchTimeSecs),
                                                 summaries, extractStatusStr(common.base), errors);
        if (common.topology.is_set_storm_version()) {
            topoInfo.set_storm_version(common.topology.get_storm_version());
        }
        if (common.base.is_set_owner()) {
            topoInfo.set_owner(common.base.get_owner());
        }
        String schedStatus = idToSchedStatus.get().get(topoId);
        if (schedStatus != null) {
            topoInfo.set_sched_status(schedStatus);
        }
        TopologyResources resources = getResourcesForTopology(topoId, common.base);
        if (resources != null && underlyingScheduler instanceof ResourceAwareScheduler) {
            topoInfo.set_requested_memonheap(resources.getRequestedMemOnHeap());
            topoInfo.set_requested_memoffheap(resources.getRequestedMemOffHeap());
            topoInfo.set_requested_cpu(resources.getRequestedCpu());
            topoInfo.set_assigned_memonheap(resources.getAssignedMemOnHeap());
            topoInfo.set_assigned_memoffheap(resources.getAssignedMemOffHeap());
            topoInfo.set_assigned_cpu(resources.getAssignedCpu());

        }
        if (common.base.is_set_component_debug()) {
            topoInfo.set_component_debug(common.base.get_component_debug());
        }
        topoInfo.set_replication_count(getBlobReplicationCount(ConfigUtils.masterStormCodeKey(topoId)));
        return topoInfo;
    }

    @Override
    public TopologyPageInfo getTopologyPageInfo(String topoId, String window, boolean includeSys)
        throws NotAliveException, AuthorizationException, TException {
        try {
            getTopologyPageInfoCalls.mark();
            CommonTopoInfo common = getCommonTopoInfo(topoId, "getTopologyPageInfo");
            String topoName = common.topoName;
            IStormClusterState state = stormClusterState;
            Assignment assignment = common.assignment;
            Map<List<Integer>, Map<String, Object>> beats = common.beats;
            Map<Integer, String> taskToComp = common.taskToComponent;
            StormTopology topology = common.topology;
            StormBase base = common.base;
            if (base == null) {
                throw new WrappedNotAliveException(topoId);
            }
            String owner = base.get_owner();
            Map<WorkerSlot, WorkerResources> workerToResources = getWorkerResourcesForTopology(topoId);
            List<WorkerSummary> workerSummaries = null;
            Map<List<Long>, List<Object>> exec2NodePort = new HashMap<>();
            if (assignment != null) {
                Map<List<Long>, NodeInfo> execToNodeInfo = assignment.get_executor_node_port();
                Map<String, String> nodeToHost = assignment.get_node_host();
                for (Entry<List<Long>, NodeInfo> entry : execToNodeInfo.entrySet()) {
                    NodeInfo ni = entry.getValue();
                    List<Object> nodePort = Arrays.asList(ni.get_node(), ni.get_port_iterator().next());
                    exec2NodePort.put(entry.getKey(), nodePort);
                }

                workerSummaries = StatsUtil.aggWorkerStats(topoId,
                                                           topoName,
                                                           taskToComp,
                                                           beats,
                                                           exec2NodePort,
                                                           nodeToHost,
                                                           workerToResources,
                                                           includeSys,
                                                           true, //this is the topology page, so we know the user is authorized
                                                           null,
                                                           owner);
            }

            TopologyPageInfo topoPageInfo = StatsUtil.aggTopoExecsStats(topoId,
                                                                        exec2NodePort,
                                                                        taskToComp,
                                                                        beats,
                                                                        topology,
                                                                        window,
                                                                        includeSys,
                                                                        state);

            if (topology.is_set_storm_version()) {
                topoPageInfo.set_storm_version(topology.get_storm_version());
            }

            Map<String, Object> topoConf = Utils.merge(conf, common.topoConf);


            addSpoutAggStats(topoPageInfo, topology, topoConf);
            addBoltAggStats(topoPageInfo, topology, topoConf, includeSys);

            if (workerSummaries != null) {
                topoPageInfo.set_workers(workerSummaries);
            }
            if (base.is_set_owner()) {
                topoPageInfo.set_owner(base.get_owner());
            }

            if (base.is_set_topology_version()) {
                topoPageInfo.set_topology_version(base.get_topology_version());
            }

            String schedStatus = idToSchedStatus.get().get(topoId);
            if (schedStatus != null) {
                topoPageInfo.set_sched_status(schedStatus);
            }
            TopologyResources resources = getResourcesForTopology(topoId, base);
            if (resources != null && underlyingScheduler instanceof ResourceAwareScheduler) {
                topoPageInfo.set_requested_memonheap(resources.getRequestedMemOnHeap());
                topoPageInfo.set_requested_memoffheap(resources.getRequestedMemOffHeap());
                topoPageInfo.set_requested_cpu(resources.getRequestedCpu());
                topoPageInfo.set_assigned_memonheap(resources.getAssignedMemOnHeap());
                topoPageInfo.set_assigned_memoffheap(resources.getAssignedMemOffHeap());
                topoPageInfo.set_assigned_cpu(resources.getAssignedCpu());
                topoPageInfo.set_requested_shared_off_heap_memory(resources.getRequestedSharedMemOffHeap());
                topoPageInfo.set_requested_regular_off_heap_memory(resources.getRequestedNonSharedMemOffHeap());
                topoPageInfo.set_requested_shared_on_heap_memory(resources.getRequestedSharedMemOnHeap());
                topoPageInfo.set_requested_regular_on_heap_memory(resources.getRequestedNonSharedMemOnHeap());
                topoPageInfo.set_assigned_shared_off_heap_memory(resources.getAssignedSharedMemOffHeap());
                topoPageInfo.set_assigned_regular_off_heap_memory(resources.getAssignedNonSharedMemOffHeap());
                topoPageInfo.set_assigned_shared_on_heap_memory(resources.getAssignedSharedMemOnHeap());
                topoPageInfo.set_assigned_regular_on_heap_memory(resources.getAssignedNonSharedMemOnHeap());
                topoPageInfo.set_assigned_generic_resources(resources.getAssignedGenericResources());
                topoPageInfo.set_requested_generic_resources(resources.getRequestedGenericResources());
            }
            int launchTimeSecs = common.launchTimeSecs;
            topoPageInfo.set_name(topoName);
            topoPageInfo.set_status(extractStatusStr(base));
            topoPageInfo.set_uptime_secs(Time.deltaSecs(launchTimeSecs));
            topoPageInfo.set_topology_conf(JSONValue.toJSONString(topoConf));
            topoPageInfo.set_replication_count(getBlobReplicationCount(ConfigUtils.masterStormCodeKey(topoId)));
            if (base.is_set_component_debug()) {
                DebugOptions debug = base.get_component_debug().get(topoId);
                if (debug != null) {
                    topoPageInfo.set_debug_options(debug);
                }
            }
            return topoPageInfo;
        } catch (Exception e) {
            LOG.warn("Get topo page info exception. (topology id='{}')", topoId, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * If aggStats are not populated, compute common and component(spout) agg and create placeholder stat.
     * This allow the topology page to show component spec even the topo is not scheduled.
     * Otherwise, just fetch data from current topoPageInfo.
     *
     * @param topoPageInfo topology page info holding spout AggStats
     * @param topology     storm topology used to get spout names
     * @param topoConf     storm topology config
     */
    private void addSpoutAggStats(TopologyPageInfo topoPageInfo, StormTopology topology, Map<String, Object> topoConf) {
        Map<String, NormalizedResourceRequest> spoutResources = ResourceUtils.getSpoutsResources(topology, topoConf);

        // if agg stats were not populated yet, create placeholder
        if (topoPageInfo.get_id_to_spout_agg_stats().isEmpty()) {
            for (Entry<String, SpoutSpec> entry : topology.get_spouts().entrySet()) {
                String spoutName = entry.getKey();
                SpoutSpec spoutSpec = entry.getValue();

                // component
                ComponentAggregateStats placeholderComponentStats = new ComponentAggregateStats();
                placeholderComponentStats.set_type(ComponentType.SPOUT);

                // common aggregate
                CommonAggregateStats commonStats = getPlaceholderCommonAggregateStats(spoutSpec);
                commonStats.set_resources_map(spoutResources.getOrDefault(spoutName, new NormalizedResourceRequest())
                            .toNormalizedMap());
                placeholderComponentStats.set_common_stats(commonStats);

                // spout aggregate
                SpoutAggregateStats spoutAggStats = new SpoutAggregateStats();
                spoutAggStats.set_complete_latency_ms(0);
                SpecificAggregateStats specificStats = new SpecificAggregateStats();
                specificStats.set_spout(spoutAggStats);
                placeholderComponentStats.set_specific_stats(specificStats);
                topoPageInfo.get_id_to_spout_agg_stats().put(spoutName, placeholderComponentStats);
            }
        } else {
            for (Entry<String, ComponentAggregateStats> entry : topoPageInfo.get_id_to_spout_agg_stats().entrySet()) {
                CommonAggregateStats commonStats = entry.getValue().get_common_stats();
                setResourcesDefaultIfNotSet(spoutResources, entry.getKey(), topoConf);
                commonStats.set_resources_map(spoutResources.get(entry.getKey()).toNormalizedMap());
            }
        }
    }

    /**
     * If aggStats are not populated, compute common and component(bolt) agg and create placeholder stat.
     * This allow the topology page to show component spec even the topo is not scheduled.
     * Otherwise, just fetch data from current topoPageInfo.
     *
     * @param topoPageInfo  topology page info holding bolt AggStats
     * @param topology      storm topology used to get bolt names
     * @param topoConf      storm topology config
     * @param includeSys    whether to show system bolts
     */
    private void addBoltAggStats(TopologyPageInfo topoPageInfo, StormTopology topology,
                                                 Map<String, Object> topoConf, boolean includeSys) {
        Map<String, NormalizedResourceRequest> boltResources = ResourceUtils.getBoltsResources(topology, topoConf);

        // if agg stats were not populated yet, create placeholder
        if (topoPageInfo.get_id_to_bolt_agg_stats().isEmpty()) {
            for (Entry<String, Bolt> entry : topology.get_bolts().entrySet()) {
                String boltName = entry.getKey();
                Bolt bolt = entry.getValue();
                if ((!includeSys && Utils.isSystemId(boltName)) || boltName.equals(Constants.SYSTEM_COMPONENT_ID)) {
                    continue;
                }

                // component
                ComponentAggregateStats placeholderComponentStats = new ComponentAggregateStats();
                placeholderComponentStats.set_type(ComponentType.BOLT);

                // common aggregate
                CommonAggregateStats commonStats = getPlaceholderCommonAggregateStats(bolt);
                commonStats.set_resources_map(boltResources.getOrDefault(boltName, new NormalizedResourceRequest())
                            .toNormalizedMap());
                placeholderComponentStats.set_common_stats(commonStats);

                // bolt aggregate
                BoltAggregateStats boltAggStats = new BoltAggregateStats();
                boltAggStats.set_execute_latency_ms(0);
                boltAggStats.set_process_latency_ms(0);
                boltAggStats.set_executed(0);
                boltAggStats.set_capacity(0);
                SpecificAggregateStats specificStats = new SpecificAggregateStats();
                specificStats.set_bolt(boltAggStats);
                placeholderComponentStats.set_specific_stats(specificStats);

                topoPageInfo.get_id_to_bolt_agg_stats().put(boltName, placeholderComponentStats);
            }
        } else {
            for (Entry<String, ComponentAggregateStats> entry : topoPageInfo.get_id_to_bolt_agg_stats().entrySet()) {
                CommonAggregateStats commonStats = entry.getValue().get_common_stats();
                setResourcesDefaultIfNotSet(boltResources, entry.getKey(), topoConf);
                commonStats.set_resources_map(boltResources.get(entry.getKey()).toNormalizedMap());
            }
        }
    }

    private CommonAggregateStats getPlaceholderCommonAggregateStats(Object component) {
        // common aggregate
        CommonAggregateStats commonStats = new CommonAggregateStats();

        // get num_executors
        int numExecutors = 0;
        try {
            numExecutors = StormCommon.numStartExecutors(component);
        } catch (InvalidTopologyException e) {
            // ignore
        }

        // get num_tasks
        Map<String, Object> jsonMap = StormCommon.componentConf(component);
        int numTasks = ObjectReader.getInt(jsonMap.getOrDefault(Config.TOPOLOGY_TASKS, numExecutors));

        commonStats.set_num_executors(numExecutors);
        commonStats.set_num_tasks(numTasks);
        commonStats.set_emitted(0);
        commonStats.set_transferred(0);
        commonStats.set_acked(0);

        return commonStats;
    }

    @Override
    public SupervisorPageInfo getSupervisorPageInfo(String superId, String host, boolean includeSys)
        throws NotAliveException, AuthorizationException, TException {
        try {
            getSupervisorPageInfoCalls.mark();
            IStormClusterState state = stormClusterState;
            Map<String, SupervisorInfo> superInfos = state.allSupervisorInfo();
            Map<String, List<String>> hostToSuperId = new HashMap<>();
            for (Entry<String, SupervisorInfo> entry : superInfos.entrySet()) {
                String h = entry.getValue().get_hostname();
                List<String> superIds = hostToSuperId.get(h);
                if (superIds == null) {
                    superIds = new ArrayList<>();
                    hostToSuperId.put(h, superIds);
                }
                superIds.add(entry.getKey());
            }
            List<String> supervisorIds = null;
            if (superId == null) {
                supervisorIds = hostToSuperId.get(host);
            } else {
                supervisorIds = Arrays.asList(superId);
            }
            SupervisorPageInfo pageInfo = new SupervisorPageInfo();
            Map<String, Assignment> topoToAssignment = state.assignmentsInfo();
            for (String sid : supervisorIds) {
                SupervisorInfo info = superInfos.get(sid);
                LOG.info("SIDL {} SI: {} ALL: {}", sid, info, superInfos);
                SupervisorSummary supSum = makeSupervisorSummary(sid, info);
                pageInfo.add_to_supervisor_summaries(supSum);
                List<String> superTopologies = topologiesOnSupervisor(topoToAssignment, sid);
                Set<String> userTopologies = filterAuthorized("getTopology", superTopologies);
                for (String topoId : superTopologies) {
                    CommonTopoInfo common = getCommonTopoInfo(topoId, "getSupervisorPageInfo");
                    String topoName = common.topoName;
                    Assignment assignment = common.assignment;
                    Map<List<Integer>, Map<String, Object>> beats = common.beats;
                    Map<Integer, String> taskToComp = common.taskToComponent;
                    Map<List<Long>, List<Object>> exec2NodePort = new HashMap<>();
                    Map<String, String> nodeToHost;
                    if (assignment != null) {
                        Map<List<Long>, NodeInfo> execToNodeInfo = assignment.get_executor_node_port();
                        for (Entry<List<Long>, NodeInfo> entry : execToNodeInfo.entrySet()) {
                            NodeInfo ni = entry.getValue();
                            List<Object> nodePort = Arrays.asList(ni.get_node(), ni.get_port_iterator().next());
                            exec2NodePort.put(entry.getKey(), nodePort);
                        }
                        nodeToHost = assignment.get_node_host();
                    } else {
                        nodeToHost = Collections.emptyMap();
                    }
                    Map<WorkerSlot, WorkerResources> workerResources = getWorkerResourcesForTopology(topoId);
                    boolean isAllowed = userTopologies.contains(topoId);
                    String owner = (common.base == null) ? null : common.base.get_owner();
                    for (WorkerSummary workerSummary : StatsUtil.aggWorkerStats(topoId, topoName, taskToComp, beats,
                                                                                exec2NodePort, nodeToHost, workerResources, includeSys,
                                                                                isAllowed, sid, owner)) {
                        pageInfo.add_to_worker_summaries(workerSummary);
                    }
                }
            }
            return pageInfo;
        } catch (Exception e) {
            LOG.warn("Get super page info exception. (super id='{}')", superId, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public ComponentPageInfo getComponentPageInfo(String topoId, String componentId, String window, boolean includeSys)
        throws NotAliveException, AuthorizationException, TException {
        try {
            getComponentPageInfoCalls.mark();
            CommonTopoInfo info = getCommonTopoInfo(topoId, "getComponentPageInfo");
            if (info.base == null) {
                throw new WrappedNotAliveException(topoId);
            }
            StormTopology topology = info.topology;
            Map<String, Object> topoConf = info.topoConf;
            topoConf = Utils.merge(conf, topoConf);
            Assignment assignment = info.assignment;
            Map<List<Long>, List<Object>> exec2NodePort = new HashMap<>();
            Map<String, String> nodeToHost;
            Map<List<Long>, List<Object>> exec2HostPort = new HashMap<>();
            if (assignment != null) {
                Map<List<Long>, NodeInfo> execToNodeInfo = assignment.get_executor_node_port();
                nodeToHost = assignment.get_node_host();
                for (Entry<List<Long>, NodeInfo> entry : execToNodeInfo.entrySet()) {
                    NodeInfo ni = entry.getValue();
                    List<Object> nodePort = Arrays.asList(ni.get_node(), ni.get_port_iterator().next());
                    List<Object> hostPort = Arrays.asList(nodeToHost.get(ni.get_node()), ni.get_port_iterator().next());
                    exec2NodePort.put(entry.getKey(), nodePort);
                    exec2HostPort.put(entry.getKey(), hostPort);
                }
            } else {
                nodeToHost = Collections.emptyMap();
            }

            ComponentPageInfo compPageInfo = StatsUtil.aggCompExecsStats(exec2HostPort, info.taskToComponent, info.beats, window,
                                                                         includeSys, topoId, topology, componentId);
            if (compPageInfo.get_component_type() == ComponentType.SPOUT) {
                NormalizedResourceRequest spoutResources = ResourceUtils.getSpoutResources(topology, topoConf, componentId);
                if (spoutResources == null) {
                    spoutResources = new NormalizedResourceRequest(topoConf, componentId);
                }
                compPageInfo.set_resources_map(spoutResources.toNormalizedMap());
            } else { //bolt
                NormalizedResourceRequest boltResources = ResourceUtils.getBoltResources(topology, topoConf, componentId);
                if (boltResources == null) {
                    boltResources = new NormalizedResourceRequest(topoConf, componentId);
                }
                compPageInfo.set_resources_map(boltResources.toNormalizedMap());
            }
            compPageInfo.set_topology_name(info.topoName);
            compPageInfo.set_errors(stormClusterState.errors(topoId, componentId));
            compPageInfo.set_topology_status(extractStatusStr(info.base));
            if (info.base.is_set_component_debug()) {
                DebugOptions debug = info.base.get_component_debug().get(componentId);
                if (debug != null) {
                    compPageInfo.set_debug_options(debug);
                }
            }
            // Add the event logger details.
            Map<String, List<Integer>> compToTasks = Utils.reverseMap(info.taskToComponent);
            if (compToTasks.containsKey(StormCommon.EVENTLOGGER_COMPONENT_ID)) {
                List<Integer> tasks = compToTasks.get(StormCommon.EVENTLOGGER_COMPONENT_ID);
                tasks.sort(null);
                // Find the task the events from this component route to.
                int taskIndex = TupleUtils.chooseTaskIndex(Collections.singletonList(componentId), tasks.size());
                int taskId = tasks.get(taskIndex);
                String host = null;
                Integer port = null;
                for (Entry<List<Long>, List<Object>> entry : exec2HostPort.entrySet()) {
                    int start = entry.getKey().get(0).intValue();
                    int end = entry.getKey().get(1).intValue();
                    if (taskId >= start && taskId <= end) {
                        host = (String) entry.getValue().get(0);
                        port = ((Number) entry.getValue().get(1)).intValue();
                        break;
                    }
                }

                if (host != null && port != null) {
                    compPageInfo.set_eventlog_host(host);
                    compPageInfo.set_eventlog_port(port);
                }
            }
            return compPageInfo;
        } catch (Exception e) {
            LOG.warn("getComponentPageInfo exception. (topo id='{}')", topoId, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getTopologyConf(String id) throws NotAliveException, AuthorizationException, TException {
        try {
            getTopologyConfCalls.mark();
            Map<String, Object> topoConf = tryReadTopoConf(id, topoCache);
            Map<String, Object> checkConf = Utils.merge(conf, topoConf);
            String topoName = (String) checkConf.get(Config.TOPOLOGY_NAME);
            checkAuthorization(topoName, checkConf, "getTopologyConf");
            return JSONValue.toJSONString(topoConf);
        } catch (Exception e) {
            LOG.warn("Get topo conf exception. (topology id='{}')", id, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public StormTopology getTopology(String id) throws NotAliveException, AuthorizationException, TException {
        try {
            getTopologyCalls.mark();
            Map<String, Object> topoConf = tryReadTopoConf(id, topoCache);
            topoConf = Utils.merge(conf, topoConf);
            String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);
            checkAuthorization(topoName, topoConf, "getTopology");
            return StormCommon.systemTopology(topoConf, tryReadTopology(id, topoCache));
        } catch (Exception e) {
            LOG.warn("Get topology exception. (topology id='{}')", id, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public StormTopology getUserTopology(String id) throws NotAliveException, AuthorizationException, TException {
        try {
            getUserTopologyCalls.mark();
            Map<String, Object> topoConf = tryReadTopoConf(id, topoCache);
            topoConf = Utils.merge(conf, topoConf);
            String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);
            checkAuthorization(topoName, topoConf, "getUserTopology");
            return tryReadTopology(id, topoCache);
        } catch (Exception e) {
            LOG.warn("Get user topology exception. (topology id='{}')", id, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public TopologyHistoryInfo getTopologyHistory(String user) throws AuthorizationException, TException {
        try {
            List<String> adminUsers = (List<String>) conf.getOrDefault(Config.NIMBUS_ADMINS, Collections.emptyList());
            List<String> adminGroups = (List<String>) conf.getOrDefault(Config.NIMBUS_ADMINS_GROUPS, Collections.emptyList());
            IStormClusterState state = stormClusterState;
            List<String> assignedIds = state.assignments(null);
            Set<String> ret = new HashSet<>();
            boolean isAdmin = adminUsers.contains(user);
            for (String topoId : assignedIds) {
                Map<String, Object> topoConf = tryReadTopoConf(topoId, topoCache);
                topoConf = Utils.merge(conf, topoConf);
                List<String> groups = ServerConfigUtils.getTopoLogsGroups(topoConf);
                List<String> topoLogUsers = ServerConfigUtils.getTopoLogsUsers(topoConf);
                if (user == null || isAdmin
                    || isUserPartOf(user, groups)
                    || isUserPartOf(user, adminGroups)
                    || topoLogUsers.contains(user)) {
                    ret.add(topoId);
                }
            }
            ret.addAll(readTopologyHistory(user, adminUsers));
            return new TopologyHistoryInfo(new ArrayList<>(ret));
        } catch (Exception e) {
            LOG.warn("Get topology history. (user='{}')", user, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public ClusterSummary getClusterInfo() throws AuthorizationException, TException {
        try {
            getClusterInfoCalls.mark();
            checkAuthorization(null, null, "getClusterInfo");
            return getClusterInfoImpl();
        } catch (Exception e) {
            LOG.warn("Get cluster info exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<TopologySummary> getTopologySummaries() throws AuthorizationException, TException {
        try {
            getTopologySummariesCalls.mark();
            checkAuthorization(null, null, "getTopologySummaries");
            return getTopologySummariesImpl();
        } catch (Exception e) {
            LOG.warn("Get TopologySummary info exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public TopologySummary getTopologySummaryByName(String name)
        throws NotAliveException, AuthorizationException, TException {
        try {
            getTopologySummaryByNameCalls.mark();
            checkAuthorization(name, null, "getTopologySummaryByName");
            IStormClusterState state = stormClusterState;
            String topoId = state.getTopoId(name).orElseThrow(() -> new WrappedNotAliveException(name + " is not alive"));
            return getTopologySummaryImpl(topoId, state.topologyBases().get(topoId));
        } catch (Exception e) {
            LOG.warn("Get TopologySummaryByName info exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public TopologySummary getTopologySummary(String id)
        throws NotAliveException, AuthorizationException, TException {
        try {
            getTopologySummaryCalls.mark();
            IStormClusterState state = stormClusterState;
            StormBase base = state.topologyBases().get(id);
            if (base == null) {
                throw new WrappedNotAliveException(id + " is not alive");
            }
            checkAuthorization(base.get_name(), null, "getTopologySummary");
            return getTopologySummaryImpl(id, base);
        } catch (Exception e) {
            LOG.warn("Get TopologySummaryById info exception.", e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public NimbusSummary getLeader() throws AuthorizationException, TException {
        getLeaderCalls.mark();
        checkAuthorization(null, null, "getLeader");
        List<NimbusSummary> nimbuses = stormClusterState.nimbuses();
        NimbusInfo leader = leaderElector.getLeader();
        for (NimbusSummary nimbusSummary : nimbuses) {
            if (leader.getHost().equals(nimbusSummary.get_host())
                && leader.getPort() == nimbusSummary.get_port()) {
                nimbusSummary.set_uptime_secs(Time.deltaSecs(nimbusSummary.get_uptime_secs()));
                nimbusSummary.set_isLeader(true);
                return nimbusSummary;
            }
        }
        return null;
    }

    @Override
    public boolean isTopologyNameAllowed(String name) throws AuthorizationException, TException {
        isTopologyNameAllowedCalls.mark();
        try {
            checkAuthorization(name, null, "isTopologyNameAllowed");
            validateTopologyName(name);
            assertTopoActive(name, false);
            return true;
        } catch (InvalidTopologyException | AlreadyAliveException e) {
            return false;
        }
    }

    @Override
    public List<OwnerResourceSummary> getOwnerResourceSummaries(String owner) throws AuthorizationException, TException {
        try {
            getOwnerResourceSummariesCalls.mark();
            checkAuthorization(null, null, "getOwnerResourceSummaries");
            IStormClusterState state = stormClusterState;
            Map<String, Assignment> topoIdToAssignments = state.assignmentsInfo();
            Map<String, StormBase> topoIdToBases = state.topologyBases();
            Map<String, Number> clusterSchedulerConfig = scheduler.config();

            //put [owner-> StormBase-list] mapping to ownerToBasesMap
            //if this owner (the input parameter) is null, add all the owners with stormbase and guarantees
            //else, add only this owner (the input paramter) to the map
            Map<String, List<StormBase>> ownerToBasesMap = new HashMap<>();

            if (owner == null) {
                // add all the owners to the map
                for (StormBase base : topoIdToBases.values()) {
                    String baseOwner = base.get_owner();
                    if (!ownerToBasesMap.containsKey(baseOwner)) {
                        List<StormBase> stormbases = new ArrayList<>();
                        stormbases.add(base);
                        ownerToBasesMap.put(baseOwner, stormbases);
                    } else {
                        ownerToBasesMap.get(baseOwner).add(base);
                    }
                }
                //in addition, add all the owners with guarantees
                List<String> ownersWithGuarantees = new ArrayList<>(clusterSchedulerConfig.keySet());
                for (String ownerWithGuarantees : ownersWithGuarantees) {
                    if (!ownerToBasesMap.containsKey(ownerWithGuarantees)) {
                        ownerToBasesMap.put(ownerWithGuarantees, new ArrayList<>());
                    }
                }
            } else {
                //only put this owner to the map
                List<StormBase> stormbases = new ArrayList<>();
                for (StormBase base : topoIdToBases.values()) {
                    if (owner.equals(base.get_owner())) {
                        stormbases.add(base);
                    }
                }
                ownerToBasesMap.put(owner, stormbases);
            }

            List<OwnerResourceSummary> ret = new ArrayList<>();

            //for each owner, get resources, configs, and aggregate
            for (Entry<String, List<StormBase>> ownerToBasesEntry : ownerToBasesMap.entrySet()) {
                String theOwner = ownerToBasesEntry.getKey();
                TopologyResources totalResourcesAggregate = new TopologyResources();

                int totalExecutors = 0;
                int totalWorkers = 0;
                int totalTasks = 0;

                for (StormBase base : ownerToBasesEntry.getValue()) {
                    try {
                        String topoId = toTopoId(base.get_name());
                        TopologyResources resources = getResourcesForTopology(topoId, base);
                        totalResourcesAggregate = totalResourcesAggregate.add(resources);
                        Assignment ownerAssignment = topoIdToAssignments.get(topoId);
                        if (ownerAssignment != null && ownerAssignment.get_executor_node_port() != null) {
                            totalExecutors += ownerAssignment.get_executor_node_port().keySet().size();
                            totalWorkers += new HashSet(ownerAssignment.get_executor_node_port().values()).size();
                            for (List<Long> executorId : ownerAssignment.get_executor_node_port().keySet()) {
                                totalTasks += StormCommon.executorIdToTasks(executorId).size();
                            }
                        }
                    } catch (NotAliveException e) {
                        LOG.warn("{} is not alive.", base.get_name());
                    }
                }

                double requestedTotalMemory = totalResourcesAggregate.getRequestedMemOnHeap()
                                              + totalResourcesAggregate.getRequestedMemOffHeap();
                double assignedTotalMemory = totalResourcesAggregate.getAssignedMemOnHeap()
                                             + totalResourcesAggregate.getAssignedMemOffHeap();

                OwnerResourceSummary ownerResourceSummary = new OwnerResourceSummary(theOwner);
                ownerResourceSummary.set_total_topologies(ownerToBasesEntry.getValue().size());
                ownerResourceSummary.set_total_executors(totalExecutors);
                ownerResourceSummary.set_total_workers(totalWorkers);
                ownerResourceSummary.set_total_tasks(totalTasks);
                ownerResourceSummary.set_memory_usage(assignedTotalMemory);
                ownerResourceSummary.set_cpu_usage(totalResourcesAggregate.getAssignedCpu());
                ownerResourceSummary.set_requested_on_heap_memory(totalResourcesAggregate.getRequestedMemOnHeap());
                ownerResourceSummary.set_requested_off_heap_memory(totalResourcesAggregate.getRequestedMemOffHeap());
                ownerResourceSummary.set_requested_total_memory(requestedTotalMemory);
                ownerResourceSummary.set_requested_cpu(totalResourcesAggregate.getRequestedCpu());
                ownerResourceSummary.set_assigned_on_heap_memory(totalResourcesAggregate.getAssignedMemOnHeap());
                ownerResourceSummary.set_assigned_off_heap_memory(totalResourcesAggregate.getAssignedMemOffHeap());

                if (clusterSchedulerConfig.containsKey(theOwner)) {
                    if (underlyingScheduler instanceof ResourceAwareScheduler) {
                        Map<String, Object> schedulerConfig = (Map) clusterSchedulerConfig.get(theOwner);
                        if (schedulerConfig != null) {
                            ownerResourceSummary.set_memory_guarantee((double) schedulerConfig.getOrDefault("memory", 0));
                            ownerResourceSummary.set_cpu_guarantee((double) schedulerConfig.getOrDefault("cpu", 0));
                            ownerResourceSummary.set_memory_guarantee_remaining(ownerResourceSummary.get_memory_guarantee()
                                                                                - ownerResourceSummary.get_memory_usage());
                            ownerResourceSummary.set_cpu_guarantee_remaining(ownerResourceSummary.get_cpu_guarantee()
                                                                             - ownerResourceSummary.get_cpu_usage());
                        }
                    } else if (underlyingScheduler instanceof MultitenantScheduler) {
                        ownerResourceSummary.set_isolated_node_guarantee((int) clusterSchedulerConfig.getOrDefault(theOwner, 0));
                    }
                }

                LOG.debug("{}", ownerResourceSummary.toString());
                ret.add(ownerResourceSummary);
            }

            return ret;
        } catch (Exception e) {
            LOG.warn("Get owner resource summaries exception. (owner = '{}')", owner);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public SupervisorAssignments getSupervisorAssignments(String nodeId) throws AuthorizationException, TException {
        checkAuthorization(null, null, "getSupervisorAssignments");
        try {
            if (isLeader() && isAssignmentsRecovered()) {
                SupervisorAssignments supervisorAssignments = new SupervisorAssignments();
                supervisorAssignments.set_storm_assignment(assignmentsForNodeId(stormClusterState.assignmentsInfo(), nodeId));
                return supervisorAssignments;
            }
        } catch (Exception e) {
            LOG.debug("Exception when node {} fetching assignments", nodeId);
            if (e instanceof TException) {
                throw (TException) e;
            }
            // When this master is not leader and get a sync request from node,
            // just return nil which will cause client/node to get an unknown error,
            // the node/supervisor will sync it as a timer task.
            LOG.debug("Exception when node {} fetching assignments", nodeId);
        }
        return null;
    }

    @Override
    public void sendSupervisorWorkerHeartbeats(SupervisorWorkerHeartbeats heartbeats)
        throws AuthorizationException, TException {
        checkAuthorization(null, null, "sendSupervisorWorkerHeartbeats");
        try {
            if (isLeader()) {
                updateCachedHeartbeatsFromSupervisor(heartbeats);
            }
        } catch (Exception e) {
            LOG.debug("Exception when update heartbeats for node {} heartbeats report.",
                      heartbeats.get_supervisor_id());
            if (e instanceof TException) {
                throw (TException) e;
            }
            // When this master is not leader and get heartbeats report from supervisor/node, just ignore it.
        }
    }

    @Override
    public void sendSupervisorWorkerHeartbeat(SupervisorWorkerHeartbeat hb) throws AuthorizationException, TException {
        String id = hb.get_storm_id();
        try {
            Map<String, Object> topoConf = tryReadTopoConf(id, topoCache);
            String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);
            checkAuthorization(topoName, topoConf, "sendSupervisorWorkerHeartbeat");
            if (isLeader()) {
                int heartbeatTimeoutSecs = getTopologyHeartbeatTimeoutSecs(topoConf);
                updateCachedHeartbeatsFromWorker(hb, heartbeatTimeoutSecs);
            }
        } catch (Exception e) {
            LOG.warn("Send HB exception. (topology id='{}')", id, e);
            if (e instanceof TException) {
                throw (TException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void shutdown() {
        shutdownCalls.mark();
        try {
            LOG.info("Shutting down master");
            timer.close();
            stormClusterState.disconnect();
            downloaders.cleanup();
            uploaders.cleanup();
            blobDownloaders.cleanup();
            blobUploaders.cleanup();
            blobListers.cleanup();
            scheduler.cleanup();
            blobStore.shutdown();
            leaderElector.close();
            assignmentsDistributer.close();
            ITopologyActionNotifierPlugin actionNotifier = nimbusTopologyActionNotifier;
            if (actionNotifier != null) {
                actionNotifier.cleanup();
            }
            zkClient.close();
            if (metricsStore != null) {
                metricsStore.close();
            }
            clusterMetricSet.setActive(false);
            LOG.info("Shut down master");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isWaiting() {
        return timer.isTimerWaiting();
    }

    @Override
    public void processWorkerMetrics(WorkerMetrics metrics) throws TException {
        processWorkerMetricsCalls.mark();

        checkAuthorization(null, null, "processWorkerMetrics");

        if (this.metricsStore == null) {
            return;
        }

        for (WorkerMetricPoint m : metrics.get_metricList().get_metrics()) {
            try {
                Metric metric = new Metric(m.get_metricName(), m.get_timestamp(), metrics.get_topologyId(),
                                           m.get_metricValue(), m.get_componentId(), m.get_executorId(), metrics.get_hostname(),
                                           m.get_streamId(), metrics.get_port(), AggLevel.AGG_LEVEL_NONE);
                this.metricsStore.insert(metric);
            } catch (Exception e) {
                LOG.error("Failed to save metric", e);
            }
        }
    }

    @Override
    public boolean isRemoteBlobExists(String blobKey) throws AuthorizationException, TException {
        try {
            blobStore.getBlobMeta(blobKey, getSubject());
        } catch (KeyNotFoundException e) {
            return false;
        }
        return true;
    }

    private static final class Assoc<K, V> implements UnaryOperator<Map<K, V>> {
        private final K key;
        private final V value;

        Assoc(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public Map<K, V> apply(Map<K, V> t) {
            Map<K, V> ret = new HashMap<>(t);
            ret.put(key, value);
            return ret;
        }
    }

    // Shutdownable methods

    private static final class Dissoc<K, V> implements UnaryOperator<Map<K, V>> {
        private final K key;

        Dissoc(K key) {
            this.key = key;
        }

        @Override
        public Map<K, V> apply(Map<K, V> t) {
            Map<K, V> ret = new HashMap<>(t);
            ret.remove(key);
            return ret;
        }
    }

    //Daemon common methods

    @VisibleForTesting
    public static class StandaloneINimbus implements INimbus {

        @Override
        public void prepare(Map<String, Object> topoConf, String schedulerLocalDir) {
            //NOOP
        }

        @SuppressWarnings("unchecked")
        @Override
        public Collection<WorkerSlot> allSlotsAvailableForScheduling(Collection<SupervisorDetails> supervisors,
                                                                     Topologies topologies, Set<String> topologiesMissingAssignments) {
            Set<WorkerSlot> ret = new HashSet<>();
            for (SupervisorDetails sd : supervisors) {
                String id = sd.getId();
                for (Number port : (Collection<Number>) sd.getMeta()) {
                    ret.add(new WorkerSlot(id, port));
                }
            }
            return ret;
        }

        @Override
        public void assignSlots(Topologies topologies, Map<String, Collection<WorkerSlot>> newSlotsByTopologyId) {
            //NOOP
        }

        @Override
        public String getHostName(Map<String, SupervisorDetails> supervisors, String nodeId) {
            SupervisorDetails sd = supervisors.get(nodeId);
            if (sd != null) {
                return sd.getHost();
            }
            return null;
        }

        @Override
        public IScheduler getForcedScheduler() {
            return null;
        }

    }

    private static class CommonTopoInfo {
        public Map<String, Object> topoConf;
        public String topoName;
        public StormTopology topology;
        public Map<Integer, String> taskToComponent;
        public StormBase base;
        public int launchTimeSecs;
        public Assignment assignment;
        public Map<List<Integer>, Map<String, Object>> beats;
        public HashSet<String> allComponents;

    }

    private static class ClusterSummaryMetrics implements MetricSet {
        private static final String SUMMARY = "summary";
        private final Map<String, com.codahale.metrics.Metric> metrics = new HashMap<>();
        
        public com.codahale.metrics.Metric put(String key, com.codahale.metrics.Metric value) {
            return metrics.put(MetricRegistry.name(SUMMARY, key), value);
        }

        @Override
        public Map<String, com.codahale.metrics.Metric> getMetrics() {
            return metrics;
        }
    }
    
    private class ClusterSummaryMetricSet implements Runnable {
        private static final int CACHING_WINDOW = 5;
        
        private final ClusterSummaryMetrics clusterSummaryMetrics = new ClusterSummaryMetrics();
        
        private final Function<String, Histogram> registerHistogram = (name) -> {
            //This histogram reflects the data distribution across only one ClusterSummary, i.e.,
            // data distribution across all entities of a type (e.g., data from all nimbus/topologies) at one moment.
            // Hence we use half of the CACHING_WINDOW time to ensure it retains only data from the most recent update
            final Histogram histogram = new Histogram(new SlidingTimeWindowReservoir(CACHING_WINDOW / 2, TimeUnit.SECONDS));
            clusterSummaryMetrics.put(name, histogram);
            return histogram;
        };
        private volatile boolean active = false;

        //NImbus metrics distribution
        private final Histogram nimbusUptime = registerHistogram.apply("nimbuses:uptime-secs");

        //Supervisor metrics distribution
        private final Histogram supervisorsUptime = registerHistogram.apply("supervisors:uptime-secs");
        private final Histogram supervisorsNumWorkers = registerHistogram.apply("supervisors:num-workers");
        private final Histogram supervisorsNumUsedWorkers = registerHistogram.apply("supervisors:num-used-workers");
        private final Histogram supervisorsUsedMem = registerHistogram.apply("supervisors:used-mem");
        private final Histogram supervisorsUsedCpu = registerHistogram.apply("supervisors:used-cpu");
        private final Histogram supervisorsFragmentedMem = registerHistogram.apply("supervisors:fragmented-mem");
        private final Histogram supervisorsFragmentedCpu = registerHistogram.apply("supervisors:fragmented-cpu");

        //Topology metrics distribution
        private final Histogram topologiesNumTasks = registerHistogram.apply("topologies:num-tasks");
        private final Histogram topologiesNumExecutors = registerHistogram.apply("topologies:num-executors");
        private final Histogram topologiesNumWorker = registerHistogram.apply("topologies:num-workers");
        private final Histogram topologiesUptime = registerHistogram.apply("topologies:uptime-secs");
        private final Histogram topologiesReplicationCount = registerHistogram.apply("topologies:replication-count");
        private final Histogram topologiesRequestedMemOnHeap = registerHistogram.apply("topologies:requested-mem-on-heap");
        private final Histogram topologiesRequestedMemOffHeap = registerHistogram.apply("topologies:requested-mem-off-heap");
        private final Histogram topologiesRequestedCpu = registerHistogram.apply("topologies:requested-cpu");
        private final Histogram topologiesAssignedMemOnHeap = registerHistogram.apply("topologies:assigned-mem-on-heap");
        private final Histogram topologiesAssignedMemOffHeap = registerHistogram.apply("topologies:assigned-mem-off-heap");
        private final Histogram topologiesAssignedCpu = registerHistogram.apply("topologies:assigned-cpu");
        private final StormMetricsRegistry metricsRegistry;

        /**
         * Constructor to put all items in ClusterSummary in MetricSet as a metric.
         * All metrics are derived from a cached ClusterSummary object,
         * expired {@link ClusterSummaryMetricSet#CACHING_WINDOW} seconds after first query in a while from reporters.
         * In case of {@link com.codahale.metrics.ScheduledReporter}, CACHING_WINDOW should be set shorter than
         * reporting interval to avoid outdated reporting.
         */
        ClusterSummaryMetricSet(StormMetricsRegistry metricsRegistry) {
            this.metricsRegistry = metricsRegistry;
            //Break the code if out of sync to thrift protocol
            assert ClusterSummary._Fields.values().length == 3
                && ClusterSummary._Fields.findByName("supervisors") == ClusterSummary._Fields.SUPERVISORS
                && ClusterSummary._Fields.findByName("topologies") == ClusterSummary._Fields.TOPOLOGIES
                && ClusterSummary._Fields.findByName("nimbuses") == ClusterSummary._Fields.NIMBUSES;

            final CachedGauge<ClusterSummary> cachedSummary = new CachedGauge<ClusterSummary>(CACHING_WINDOW, TimeUnit.SECONDS) {
                @Override
                protected ClusterSummary loadValue() {
                    try {
                        ClusterSummary newSummary = getClusterInfoImpl();
                        LOG.debug("The new summary is {}", newSummary);
                        /*
                         * Update histograms based on the new summary. Most common implementation of Reporter reports Gauges before
                         * Histograms. Because DerivativeGauge will trigger cache refresh upon reporter's query, histogram will also be
                         * updated before query
                         */
                        updateHistogram(newSummary);
                        return newSummary;
                    } catch (Exception e) {
                        LOG.warn("Get cluster info exception.", e);
                        throw new RuntimeException(e);
                    }
                }
            };

            clusterSummaryMetrics.put("cluster:num-nimbus-leaders",
                    new DerivativeGauge<ClusterSummary, Long>(cachedSummary) {
                        @Override
                        protected Long transform(ClusterSummary clusterSummary) {
                            return clusterSummary.get_nimbuses().stream()
                                    .filter(NimbusSummary::is_isLeader)
                                    .count();
                        }
                    });
            clusterSummaryMetrics.put("cluster:num-nimbuses",
                    new DerivativeGauge<ClusterSummary, Integer>(cachedSummary) {
                        @Override
                        protected Integer transform(ClusterSummary clusterSummary) {
                            return clusterSummary.get_nimbuses_size();
                        }
                    });
            clusterSummaryMetrics.put("cluster:num-supervisors",
                    new DerivativeGauge<ClusterSummary, Integer>(cachedSummary) {
                        @Override
                        protected Integer transform(ClusterSummary clusterSummary) {
                            return clusterSummary.get_supervisors_size();
                        }
                    });
            clusterSummaryMetrics.put("cluster:num-topologies",
                    new DerivativeGauge<ClusterSummary, Integer>(cachedSummary) {
                        @Override
                        protected Integer transform(ClusterSummary clusterSummary) {
                            return clusterSummary.get_topologies_size();
                        }
                    });
            clusterSummaryMetrics.put("cluster:num-total-workers",
                    new DerivativeGauge<ClusterSummary, Integer>(cachedSummary) {
                        @Override
                        protected Integer transform(ClusterSummary clusterSummary) {
                            return clusterSummary.get_supervisors().stream()
                                    .mapToInt(SupervisorSummary::get_num_workers)
                                    .sum();
                        }
                    });
            clusterSummaryMetrics.put("cluster:num-total-used-workers",
                    new DerivativeGauge<ClusterSummary, Integer>(cachedSummary) {
                        @Override
                        protected Integer transform(ClusterSummary clusterSummary) {
                            return clusterSummary.get_supervisors().stream()
                                    .mapToInt(SupervisorSummary::get_num_used_workers)
                                    .sum();
                        }
                    });
            clusterSummaryMetrics.put("cluster:total-fragmented-memory-non-negative",
                    new DerivativeGauge<ClusterSummary, Double>(cachedSummary) {
                        @Override
                        protected Double transform(ClusterSummary clusterSummary) {
                            return clusterSummary.get_supervisors().stream()
                                    //Filtered negative value
                                    .mapToDouble(supervisorSummary -> Math.max(supervisorSummary.get_fragmented_mem(), 0))
                                    .sum();
                        }
                    });
            clusterSummaryMetrics.put("cluster:total-fragmented-cpu-non-negative",
                    new DerivativeGauge<ClusterSummary, Double>(cachedSummary) {
                        @Override
                        protected Double transform(ClusterSummary clusterSummary) {
                            return clusterSummary.get_supervisors().stream()
                                    //Filtered negative value
                                    .mapToDouble(supervisorSummary -> Math.max(supervisorSummary.get_fragmented_cpu(), 0))
                                    .sum();
                        }
                    });
        }

        private void updateHistogram(ClusterSummary newSummary) {
            for (NimbusSummary nimbusSummary : newSummary.get_nimbuses()) {
                nimbusUptime.update(nimbusSummary.get_uptime_secs());
            }
            for (SupervisorSummary summary : newSummary.get_supervisors()) {
                supervisorsUptime.update(summary.get_uptime_secs());
                supervisorsNumWorkers.update(summary.get_num_workers());
                supervisorsNumUsedWorkers.update(summary.get_num_used_workers());
                supervisorsUsedMem.update(Math.round(summary.get_used_mem()));
                supervisorsUsedCpu.update(Math.round(summary.get_used_cpu()));
                supervisorsFragmentedMem.update(Math.round(summary.get_fragmented_mem()));
                supervisorsFragmentedCpu.update(Math.round(summary.get_fragmented_cpu()));
            }
            for (TopologySummary summary : newSummary.get_topologies()) {
                topologiesNumTasks.update(summary.get_num_tasks());
                topologiesNumExecutors.update(summary.get_num_executors());
                topologiesNumWorker.update(summary.get_num_workers());
                topologiesUptime.update(summary.get_uptime_secs());
                topologiesReplicationCount.update(summary.get_replication_count());
                topologiesRequestedMemOnHeap.update(Math.round(summary.get_requested_memonheap()));
                topologiesRequestedMemOffHeap.update(Math.round(summary.get_requested_memoffheap()));
                topologiesRequestedCpu.update(Math.round(summary.get_requested_cpu()));
                topologiesAssignedMemOnHeap.update(Math.round(summary.get_assigned_memonheap()));
                topologiesAssignedMemOffHeap.update(Math.round(summary.get_assigned_memoffheap()));
                topologiesAssignedCpu.update(Math.round(summary.get_assigned_cpu()));
            }
        }

        void setActive(final boolean active) {
            if (this.active != active) {
                this.active = active;
                if (active) {
                    metricsRegistry.registerAll(clusterSummaryMetrics);
                } else {
                    metricsRegistry.removeAll(clusterSummaryMetrics);
                }
            }
        }

        @Override
        public void run() {
            try {
                setActive(isLeader());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}

