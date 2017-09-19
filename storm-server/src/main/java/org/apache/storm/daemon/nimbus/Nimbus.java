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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.auth.Subject;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.StormTimer;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.BlobSynchronizer;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.blobstore.KeySequenceNumber;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.DaemonCommon;
import org.apache.storm.daemon.Shutdownable;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.BeginDownloadResult;
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
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.generated.SupervisorPageInfo;
import org.apache.storm.generated.SupervisorSummary;
import org.apache.storm.generated.TopologyActionOptions;
import org.apache.storm.generated.TopologyHistoryInfo;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologyInitialStatus;
import org.apache.storm.generated.TopologyPageInfo;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.generated.WorkerSummary;
import org.apache.storm.logging.ThriftAccessLogger;
import org.apache.storm.metric.ClusterMetricsConsumerExecutor;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.metric.api.DataPoint;
import org.apache.storm.metric.api.IClusterMetricsConsumer;
import org.apache.storm.metric.api.IClusterMetricsConsumer.ClusterInfo;
import org.apache.storm.nimbus.DefaultTopologyValidator;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.ITopologyActionNotifierPlugin;
import org.apache.storm.nimbus.ITopologyValidator;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.DefaultScheduler;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.multitenant.MultitenantScheduler;
import org.apache.storm.scheduler.resource.ResourceAwareScheduler;
import org.apache.storm.scheduler.Cluster.SupervisorResources;
import org.apache.storm.scheduler.resource.ResourceUtils;
import org.apache.storm.security.INimbusCredentialPlugin;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ICredentialsRenewer;
import org.apache.storm.security.auth.IGroupMappingServiceProvider;
import org.apache.storm.security.auth.IPrincipalToLocal;
import org.apache.storm.security.auth.NimbusPrincipal;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.storm.security.auth.ThriftServer;
import org.apache.storm.stats.StatsUtil;
import org.apache.storm.utils.BufferInputStream;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ReflectionUtils;
import org.apache.storm.utils.ServerConfigUtils;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.SimpleVersion;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.TimeCacheMap;
import org.apache.storm.utils.TupleUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.Utils.UptimeComputer;
import org.apache.storm.utils.VersionInfo;
import org.apache.storm.validation.ConfigValidation;
import org.apache.storm.zookeeper.Zookeeper;
import org.apache.thrift.TException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class Nimbus implements Iface, Shutdownable, DaemonCommon {
    private final static Logger LOG = LoggerFactory.getLogger(Nimbus.class);
    
    //    Metrics
    private static final Meter submitTopologyWithOptsCalls = StormMetricsRegistry.registerMeter("nimbus:num-submitTopologyWithOpts-calls");
    private static final Meter submitTopologyCalls = StormMetricsRegistry.registerMeter("nimbus:num-submitTopology-calls");
    private static final Meter killTopologyWithOptsCalls = StormMetricsRegistry.registerMeter("nimbus:num-killTopologyWithOpts-calls");
    private static final Meter killTopologyCalls = StormMetricsRegistry.registerMeter("nimbus:num-killTopology-calls");
    private static final Meter rebalanceCalls = StormMetricsRegistry.registerMeter("nimbus:num-rebalance-calls");
    private static final Meter activateCalls = StormMetricsRegistry.registerMeter("nimbus:num-activate-calls");
    private static final Meter deactivateCalls = StormMetricsRegistry.registerMeter("nimbus:num-deactivate-calls");
    private static final Meter debugCalls = StormMetricsRegistry.registerMeter("nimbus:num-debug-calls");
    private static final Meter setWorkerProfilerCalls = StormMetricsRegistry.registerMeter("nimbus:num-setWorkerProfiler-calls");
    private static final Meter getComponentPendingProfileActionsCalls = StormMetricsRegistry.registerMeter("nimbus:num-getComponentPendingProfileActions-calls");
    private static final Meter setLogConfigCalls = StormMetricsRegistry.registerMeter("nimbus:num-setLogConfig-calls");
    private static final Meter uploadNewCredentialsCalls = StormMetricsRegistry.registerMeter("nimbus:num-uploadNewCredentials-calls");
    private static final Meter beginFileUploadCalls = StormMetricsRegistry.registerMeter("nimbus:num-beginFileUpload-calls");
    private static final Meter uploadChunkCalls = StormMetricsRegistry.registerMeter("nimbus:num-uploadChunk-calls");
    private static final Meter finishFileUploadCalls = StormMetricsRegistry.registerMeter("nimbus:num-finishFileUpload-calls");
    private static final Meter beginFileDownloadCalls = StormMetricsRegistry.registerMeter("nimbus:num-beginFileDownload-calls");
    private static final Meter downloadChunkCalls = StormMetricsRegistry.registerMeter("nimbus:num-downloadChunk-calls");
    private static final Meter getNimbusConfCalls = StormMetricsRegistry.registerMeter("nimbus:num-getNimbusConf-calls");
    private static final Meter getLogConfigCalls = StormMetricsRegistry.registerMeter("nimbus:num-getLogConfig-calls");
    private static final Meter getTopologyConfCalls = StormMetricsRegistry.registerMeter("nimbus:num-getTopologyConf-calls");
    private static final Meter getTopologyCalls = StormMetricsRegistry.registerMeter("nimbus:num-getTopology-calls");
    private static final Meter getUserTopologyCalls = StormMetricsRegistry.registerMeter("nimbus:num-getUserTopology-calls");
    private static final Meter getClusterInfoCalls = StormMetricsRegistry.registerMeter("nimbus:num-getClusterInfo-calls");
    private static final Meter getLeaderCalls = StormMetricsRegistry.registerMeter("nimbus:num-getLeader-calls");
    private static final Meter isTopologyNameAllowedCalls = StormMetricsRegistry.registerMeter("nimbus:num-isTopologyNameAllowed-calls");
    private static final Meter getTopologyInfoWithOptsCalls = StormMetricsRegistry.registerMeter("nimbus:num-getTopologyInfoWithOpts-calls");
    private static final Meter getTopologyInfoCalls = StormMetricsRegistry.registerMeter("nimbus:num-getTopologyInfo-calls");
    private static final Meter getTopologyPageInfoCalls = StormMetricsRegistry.registerMeter("nimbus:num-getTopologyPageInfo-calls");
    private static final Meter getSupervisorPageInfoCalls = StormMetricsRegistry.registerMeter("nimbus:num-getSupervisorPageInfo-calls");
    private static final Meter getComponentPageInfoCalls = StormMetricsRegistry.registerMeter("nimbus:num-getComponentPageInfo-calls");
    private static final Meter getOwnerResourceSummariesCalls = StormMetricsRegistry.registerMeter("nimbus:num-getOwnerResourceSummaries-calls");
    private static final Meter shutdownCalls = StormMetricsRegistry.registerMeter("nimbus:num-shutdown-calls");
    // END Metrics
    
    private static final String STORM_VERSION = VersionInfo.getVersion();
    @VisibleForTesting
    public static final List<ACL> ZK_ACLS = Arrays.asList(ZooDefs.Ids.CREATOR_ALL_ACL.get(0),
            new ACL(ZooDefs.Perms.READ | ZooDefs.Perms.CREATE, ZooDefs.Ids.ANYONE_ID_UNSAFE));
    private static final Subject NIMBUS_SUBJECT = new Subject();
    static {
        NIMBUS_SUBJECT.getPrincipals().add(new NimbusPrincipal());
        NIMBUS_SUBJECT.setReadOnly();
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
    
    private static final TopologyStateTransition NOOP_TRANSITION = (arg, nimbus, topoId, base) -> null;
    private static final TopologyStateTransition INACTIVE_TRANSITION = (arg, nimbus, topoId, base) -> Nimbus.make(TopologyStatus.INACTIVE);
    private static final TopologyStateTransition ACTIVE_TRANSITION = (arg, nimbus, topoId, base) -> Nimbus.make(TopologyStatus.ACTIVE);
    private static final TopologyStateTransition KILL_TRANSITION = (killTime, nimbus, topoId, base) -> {
        int delay = 0;
        if (killTime != null) {
            delay = ((Number)killTime).intValue();
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
    
    private static final TopologyStateTransition STARTUP_WHEN_KILLED_TRANSITION = (args, nimbus, topoId, base) -> {
        int delay = base.get_topology_action_options().get_kill_options().get_wait_secs();
        nimbus.delayEvent(topoId, delay, TopologyActions.REMOVE, null);
        return null;
    };
    
    private static final TopologyStateTransition REMOVE_TRANSITION = (args, nimbus, topoId, base) -> {
        LOG.info("Killing topology: {}", topoId);
        IStormClusterState state = nimbus.getStormClusterState();
        state.removeStorm(topoId);
        BlobStore store = nimbus.getBlobStore();
        if (store instanceof LocalFsBlobStore) {
            for (String key: Nimbus.getKeyListFromId(nimbus.getConf(), topoId)) {
                state.removeBlobstoreKey(key);
                state.removeKeyVersion(key);
            }
        }
        return null;
    };
    
    private static final TopologyStateTransition STARTUP_WHEN_REBALANCING_TRANSITION = (args, nimbus, topoId, base) -> {
        int delay = base.get_topology_action_options().get_rebalance_options().get_wait_secs();
        nimbus.delayEvent(topoId, delay, TopologyActions.DO_REBALANCE, null);
        return null;
    };
    
    private static final TopologyStateTransition DO_REBALANCE_TRANSITION = (args, nimbus, topoId, base) -> {
        nimbus.doRebalance(topoId, base);
        return Nimbus.make(base.get_prev_status());
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
                    .put(TopologyActions.STARTUP, STARTUP_WHEN_KILLED_TRANSITION)
                    .put(TopologyActions.KILL, KILL_TRANSITION)
                    .put(TopologyActions.REMOVE, REMOVE_TRANSITION)
                    .build())
            .put(TopologyStatus.REBALANCING, new ImmutableMap.Builder<TopologyActions, TopologyStateTransition>()
                    .put(TopologyActions.STARTUP, STARTUP_WHEN_REBALANCING_TRANSITION)
                    .put(TopologyActions.KILL, KILL_TRANSITION)
                    .put(TopologyActions.DO_REBALANCE, DO_REBALANCE_TRANSITION)
                    .build())
            .build();
    
    // END TOPOLOGY STATE TRANSITIONS
    
    private static final class Assoc<K,V> implements UnaryOperator<Map<K, V>> {
        private final K key;
        private final V value;
        
        public Assoc(K key, V value) {
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
    
    private static final class Dissoc<K,V> implements UnaryOperator<Map<K, V>> {
        private final K key;
        
        public Dissoc(K key) {
            this.key = key;
        }
        
        @Override
        public Map<K, V> apply(Map<K, V> t) {
            Map<K, V> ret = new HashMap<>(t);
            ret.remove(key);
            return ret;
        }
    }
    
    @VisibleForTesting
    public static class StandaloneINimbus implements INimbus {

        @Override
        public void prepare(@SuppressWarnings("rawtypes") Map<String, Object> topoConf, String schedulerLocalDir) {
            //NOOP
        }

        @SuppressWarnings("unchecked")
        @Override
        public Collection<WorkerSlot> allSlotsAvailableForScheduling(Collection<SupervisorDetails> supervisors,
                Topologies topologies, Set<String> topologiesMissingAssignments) {
            Set<WorkerSlot> ret = new HashSet<>();
            for (SupervisorDetails sd: supervisors) {
                String id = sd.getId();
                for (Number port: (Collection<Number>)sd.getMeta()) {
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
        
    };
    
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

    private static <K, V> Map<K, V> merge(Map<? extends K, ? extends V> first, Map<? extends K, ? extends V> other) {
        Map<K, V> ret = new HashMap<>(first);
        if (other != null) {
            ret.putAll(other);
        }
        return ret;
    }
    
    private static <K, V> Map<K, V> mapDiff(Map<? extends K, ? extends V> first, Map<? extends K, ? extends V> second) {
        Map<K, V> ret = new HashMap<>();
        for (Entry<? extends K, ? extends V> entry: second.entrySet()) {
            if (!entry.getValue().equals(first.get(entry.getKey()))) {
                ret.put(entry.getKey(), entry.getValue());
            }
        }
        return ret;
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
        scheduler.prepare(conf);
        return scheduler;
    }

    /**
     * Constructs a TimeCacheMap instance with a blob store timeout whose
     * expiration callback invokes cancel on the value held by an expired entry when
     * that value is an AtomicOutputStream and calls close otherwise.
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
     * @param conf
     * @return
     */
    @SuppressWarnings("deprecation")
    private static TimeCacheMap<String, Iterator<String>> makeBlobListCachMap(Map<String, Object> conf) {
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
        Collection<Map<String, Object>> consumers = (Collection<Map<String, Object>>) conf.get(DaemonConfig.STORM_CLUSTER_METRICS_CONSUMER_REGISTER);
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
    
    static Map<String, Object> readTopoConf(String topoId, TopoCache tc) throws KeyNotFoundException, AuthorizationException, IOException {
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
    
    private static int getVersionForKey(String key, NimbusInfo nimbusInfo, Map<String, Object> conf) throws KeyNotFoundException {
        KeySequenceNumber kseq = new KeySequenceNumber(key, nimbusInfo);
        return kseq.getKeySequenceNumber(conf);
    }
    
    private static StormTopology readStormTopology(String topoId, TopoCache tc) throws KeyNotFoundException, AuthorizationException, IOException {
        return tc.readTopology(topoId, getSubject());
    }
    
    private static Map<String, Object> readTopoConfAsNimbus(String topoId, TopoCache tc) throws KeyNotFoundException, AuthorizationException, IOException {
        return tc.readTopoConf(topoId, NIMBUS_SUBJECT);
    }
    
    private static StormTopology readStormTopologyAsNimbus(String topoId, TopoCache tc) throws KeyNotFoundException, AuthorizationException, IOException {
        return tc.readTopology(topoId, NIMBUS_SUBJECT);
    }
    
    /**
     * convert {topology-id -> SchedulerAssignment} to
     *         {topology-id -> {executor [node port]}}
     * @return
     */
    private static Map<String, Map<List<Long>, List<Object>>> computeTopoToExecToNodePort(Map<String, SchedulerAssignment> schedAssignments) {
        Map<String, Map<List<Long>, List<Object>>> ret = new HashMap<>();
        for (Entry<String, SchedulerAssignment> schedEntry: schedAssignments.entrySet()) {
            Map<List<Long>, List<Object>> execToNodePort = new HashMap<>();
            for (Entry<ExecutorDetails, WorkerSlot> execAndNodePort: schedEntry.getValue().getExecutorToSlot().entrySet()) {
                ExecutorDetails exec = execAndNodePort.getKey();
                WorkerSlot slot = execAndNodePort.getValue();
                
                List<Long> listExec = new ArrayList<>(2);
                listExec.add((long) exec.getStartTask());
                listExec.add((long) exec.getEndTask());
                
                List<Object> nodePort = new ArrayList<>(2);
                nodePort.add(slot.getNodeId());
                nodePort.add((long)slot.getPort());
                
                execToNodePort.put(listExec, nodePort);
            }
            ret.put(schedEntry.getKey(), execToNodePort);
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
     * Convert {topology-id -> SchedulerAssignment} to {topology-id -> {WorkerSlot WorkerResources}}. Make sure this
     * can deal with other non-RAS schedulers later we may further support map-for-any-resources.
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

    private static Map<String, Map<List<Long>, List<Object>>> computeNewTopoToExecToNodePort(Map<String, SchedulerAssignment> schedAssignments,
            Map<String, Assignment> existingAssignments) {
        Map<String, Map<List<Long>, List<Object>>> ret = computeTopoToExecToNodePort(schedAssignments);
        // Print some useful information
        if (existingAssignments != null && !existingAssignments.isEmpty()) {
            for (Entry<String, Map<List<Long>, List<Object>>> entry: ret.entrySet()) {
                String topoId = entry.getKey();
                Map<List<Long>, List<Object>> execToNodePort = entry.getValue();
                Assignment assignment = existingAssignments.get(topoId);
                if (assignment == null) {
                    continue;
                }
                Map<List<Long>, NodeInfo> old = assignment.get_executor_node_port();
                Map<List<Long>, List<Object>> reassigned = new HashMap<>();
                for (Entry<List<Long>, List<Object>> execAndNodePort: execToNodePort.entrySet()) {
                    NodeInfo oldAssigned = old.get(execAndNodePort.getKey());
                    String node = (String) execAndNodePort.getValue().get(0);
                    Long port = (Long) execAndNodePort.getValue().get(1);
                    if (oldAssigned == null || !oldAssigned.get_node().equals(node) 
                            || !port.equals(oldAssigned.get_port_iterator().next())) {
                        reassigned.put(execAndNodePort.getKey(), execAndNodePort.getValue());
                    }
                }

                if (!reassigned.isEmpty()) {
                    int count = (new HashSet<>(execToNodePort.values())).size();
                    Set<List<Long>> reExecs = reassigned.keySet();
                    LOG.info("Reassigning {} to {} slots", topoId, count);
                    LOG.info("Reassign executors: {}", reExecs);
                }
            }
        }
        return ret;
    }
    
    private static List<List<Long>> changedExecutors(Map<List<Long>, NodeInfo> map,
            Map<List<Long>, List<Object>> newExecToNodePort) {
        HashMap<NodeInfo, List<List<Long>>> tmpSlotAssigned = map == null ? new HashMap<>() : Utils.reverseMap(map);
        HashMap<List<Object>, List<List<Long>>> slotAssigned = new HashMap<>();
        for (Entry<NodeInfo, List<List<Long>>> entry: tmpSlotAssigned.entrySet()) {
            NodeInfo ni = entry.getKey();
            List<Object> key = new ArrayList<>(2);
            key.add(ni.get_node());
            key.add(ni.get_port_iterator().next());
            List<List<Long>> value = new ArrayList<>(entry.getValue());
            value.sort((a, b) -> a.get(0).compareTo(b.get(0)));
            slotAssigned.put(key, value);
        }
        HashMap<List<Object>, List<List<Long>>> tmpNewSlotAssigned = newExecToNodePort == null ? new HashMap<>() : Utils.reverseMap(newExecToNodePort);
        HashMap<List<Object>, List<List<Long>>> newSlotAssigned = new HashMap<>();
        for (Entry<List<Object>, List<List<Long>>> entry: tmpNewSlotAssigned.entrySet()) {
            List<List<Long>> value = new ArrayList<>(entry.getValue());
            value.sort((a, b) -> a.get(0).compareTo(b.get(0)));
            newSlotAssigned.put(entry.getKey(), value);
        }
        Map<List<Object>, List<List<Long>>> diff = mapDiff(slotAssigned, newSlotAssigned);
        List<List<Long>> ret = new ArrayList<>();
        for (List<List<Long>> val: diff.values()) {
            ret.addAll(val);
        }
        return ret;
    }

    private static Set<WorkerSlot> newlyAddedSlots(Assignment old, Assignment current) {
        Set<NodeInfo> oldSlots = new HashSet<>(old.get_executor_node_port().values());
        Set<NodeInfo> niRet = new HashSet<>(current.get_executor_node_port().values());
        niRet.removeAll(oldSlots);
        Set<WorkerSlot> ret = new HashSet<>();
        for (NodeInfo ni: niRet) {
            ret.add(new WorkerSlot(ni.get_node(), ni.get_port_iterator().next()));
        }
        return ret;
    }
    
    private static Map<String, SupervisorDetails> basicSupervisorDetailsMap(IStormClusterState state) {
        Map<String, SupervisorDetails> ret = new HashMap<>();
        for (Entry<String, SupervisorInfo> entry: state.allSupervisorInfo().entrySet()) {
            String id = entry.getKey();
            SupervisorInfo info = entry.getValue();
            ret.put(id, new SupervisorDetails(id, info.get_hostname(), info.get_scheduler_meta(), null,
                    info.get_resources_map()));
        }
        return ret;
    }
    
    private static boolean isTopologyActive(IStormClusterState state, String topoName) {
        return state.getTopoId(topoName).isPresent();
    }
    
    private static Map<String, Object> tryReadTopoConf(String topoId, TopoCache tc) throws NotAliveException, AuthorizationException, IOException {
        try {
            return readTopoConfAsNimbus(topoId, tc);
            //Was a try-cause but I looked at the code around this and key not found is not wrapped in runtime,
            // so it is not needed
        } catch (KeyNotFoundException e) {
            if (topoId == null) {
                throw new NullPointerException();
            }
            throw new NotAliveException(topoId);
        }
    }
    
    private static final List<String> EMPTY_STRING_LIST = Collections.unmodifiableList(Collections.emptyList());
    private static final Set<String> EMPTY_STRING_SET = Collections.unmodifiableSet(Collections.emptySet());
    
    @VisibleForTesting
    public static Set<String> topoIdsToClean(IStormClusterState state, BlobStore store) {
        Set<String> ret = new HashSet<>();
        ret.addAll(Utils.OR(state.heartbeatStorms(), EMPTY_STRING_LIST));
        ret.addAll(Utils.OR(state.errorTopologies(), EMPTY_STRING_LIST));
        ret.addAll(Utils.OR(store.storedTopoIds(), EMPTY_STRING_SET));
        ret.addAll(Utils.OR(state.backpressureTopologies(), EMPTY_STRING_LIST));
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
    
    private static int componentParallelism(Map<String, Object> topoConf, Object component) throws InvalidTopologyException {
        Map<String, Object> combinedConf = merge(topoConf, StormCommon.componentConf(component));
        int numTasks = ObjectReader.getInt(combinedConf.get(Config.TOPOLOGY_TASKS), StormCommon.numStartExecutors(component));
        Integer maxParallel = ObjectReader.getInt(combinedConf.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM), null);
        int ret = numTasks;
        if (maxParallel != null) {
            ret = Math.min(maxParallel, numTasks);
        }
        return ret;
    }
    
    private static StormTopology normalizeTopology(Map<String, Object> topoConf, StormTopology topology) throws InvalidTopologyException {
        StormTopology ret = topology.deepCopy();
        for (Object comp: StormCommon.allComponents(ret).values()) {
            Map<String, Object> mergedConf = StormCommon.componentConf(comp);
            mergedConf.put(Config.TOPOLOGY_TASKS, componentParallelism(topoConf, comp));
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
            for (Object o: conf) {
                if (o instanceof Map) {
                    ser.putAll((Map<String,String>)o);
                } else {
                    ser.put((String)o, null);
                }
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    private static Map<String, Object> normalizeConf(Map<String,Object> conf, Map<String, Object> topoConf, StormTopology topology) {
        //ensure that serializations are same for all tasks no matter what's on
        // the supervisors. this also allows you to declare the serializations as a sequence
        List<Map<String, Object>> allConfs = new ArrayList<>();
        for (Object comp: StormCommon.allComponents(topology).values()) {
            allConfs.add(StormCommon.componentConf(comp));
        }

        Set<String> decorators = new HashSet<>();
        //Yes we are putting in a config that is not the same type we pulled out.
        Map<String, String> serializers = new HashMap<>();
        for (Map<String, Object> c: allConfs) {
            addToDecorators(decorators, (List<String>) c.get(Config.TOPOLOGY_KRYO_DECORATORS));
            addToSerializers(serializers, (List<Object>) c.get(Config.TOPOLOGY_KRYO_REGISTER));
        }
        addToDecorators(decorators, (List<String>)topoConf.getOrDefault(Config.TOPOLOGY_KRYO_DECORATORS, 
                conf.get(Config.TOPOLOGY_KRYO_DECORATORS)));
        addToSerializers(serializers, (List<Object>)topoConf.getOrDefault(Config.TOPOLOGY_KRYO_REGISTER, 
                conf.get(Config.TOPOLOGY_KRYO_REGISTER)));
        
        Map<String, Object> mergedConf = merge(conf, topoConf);
        Map<String, Object> ret = new HashMap<>(topoConf);
        ret.put(Config.TOPOLOGY_KRYO_REGISTER, serializers);
        ret.put(Config.TOPOLOGY_KRYO_DECORATORS, new ArrayList<>(decorators));
        ret.put(Config.TOPOLOGY_ACKER_EXECUTORS, mergedConf.get(Config.TOPOLOGY_ACKER_EXECUTORS));
        ret.put(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, mergedConf.get(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS));
        ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, mergedConf.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM));
        return ret;
    }
    
    private static void rmBlobKey(BlobStore store, String key, IStormClusterState state) {
        try {
            store.deleteBlob(key, NIMBUS_SUBJECT);
            if (store instanceof LocalFsBlobStore) {
                state.removeBlobstoreKey(key);
            }
        } catch (Exception e) {
            //Yes eat the exception
            LOG.info("Exception {}", e);
        }
    }
    
    /**
     * Deletes jar files in dirLoc older than seconds.
     * @param dirLoc the location to look in for file
     * @param seconds how old is too old and should be deleted
     */
    @VisibleForTesting
    public static void cleanInbox(String dirLoc, int seconds) {
        final long now = Time.currentTimeMillis();
        final long ms = Time.secsToMillis(seconds);
        File dir = new File(dirLoc);
        for (File f : dir.listFiles((f) -> f.isFile() && ((f.lastModified() + ms) <= now))) {
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
    
    private static final Pattern TOPOLOGY_NAME_REGEX = Pattern.compile("^[^/.:\\\\]+$");
    private static void validateTopologyName(String name) throws InvalidTopologyException {
        Matcher m = TOPOLOGY_NAME_REGEX.matcher(name);
        if (!m.matches()) {
            throw new InvalidTopologyException("Topology name must match " + TOPOLOGY_NAME_REGEX);
        }
    }
    
    private static StormTopology tryReadTopology(String topoId, TopoCache tc) throws NotAliveException, AuthorizationException, IOException {
        try {
            return readStormTopologyAsNimbus(topoId, tc);
        } catch (KeyNotFoundException e) {
            throw new NotAliveException(topoId);
        }
    }
    
    private static void validateTopologySize(Map<String, Object> topoConf, Map<String, Object> nimbusConf, StormTopology topology) throws InvalidTopologyException {
        int workerCount = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_WORKERS), 1);
        Integer allowedWorkers = ObjectReader.getInt(nimbusConf.get(DaemonConfig.NIMBUS_SLOTS_PER_TOPOLOGY), null);
        int executorsCount = 0;
        for (Object comp : StormCommon.allComponents(topology).values()) {
            executorsCount += StormCommon.numStartExecutors(comp);
        }
        Integer allowedExecutors = ObjectReader.getInt(nimbusConf.get(DaemonConfig.NIMBUS_EXECUTORS_PER_TOPOLOGY), null);
        if (allowedExecutors != null && executorsCount > allowedExecutors) {
            throw new InvalidTopologyException("Failed to submit topology. Topology requests more than " +
                    allowedExecutors + " executors.");
        }
        
        if (allowedWorkers != null && workerCount > allowedWorkers) {
            throw new InvalidTopologyException("Failed to submit topology. Topology requests more than " +
                    allowedWorkers + " workers.");
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
        for (Entry<String, Assignment> entry: assignments.entrySet()) {
            Assignment assignment = entry.getValue();
            for (NodeInfo nodeInfo: assignment.get_executor_node_port().values()) {
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
        for (SupervisorSummary sup: summ.get_supervisors()) {
            usedSlots += sup.get_num_used_workers();
            totalSlots += sup.get_num_workers();
        }
        ret.add(new DataPoint("slotsTotal", totalSlots));
        ret.add(new DataPoint("slotsUsed", usedSlots));
        ret.add(new DataPoint("slotsFree", totalSlots - usedSlots));
        
        int totalExecutors = 0;
        int totalTasks = 0;
        for (TopologySummary topo: summ.get_topologies()) {
            totalExecutors += topo.get_num_executors();
            totalTasks += topo.get_num_tasks();
        }
        ret.add(new DataPoint("executorsTotal", totalExecutors));
        ret.add(new DataPoint("tasksTotal", totalTasks));
        return ret;
    }

    private static Map<IClusterMetricsConsumer.SupervisorInfo, List<DataPoint>> extractSupervisorMetrics(ClusterSummary summ) {
        Map<IClusterMetricsConsumer.SupervisorInfo, List<DataPoint>> ret = new HashMap<>();
        for (SupervisorSummary sup: summ.get_supervisors()) {
            IClusterMetricsConsumer.SupervisorInfo info = new IClusterMetricsConsumer.SupervisorInfo(sup.get_host(), sup.get_supervisor_id(), Time.currentTimeSecs());
            List<DataPoint> metrics = new ArrayList<>();
            metrics.add(new DataPoint("slotsTotal", sup.get_num_workers()));
            metrics.add(new DataPoint("slotsUsed", sup.get_num_used_workers()));
            metrics.add(new DataPoint("totalMem", sup.get_total_resources().get(Config.SUPERVISOR_MEMORY_CAPACITY_MB)));
            metrics.add(new DataPoint("totalCpu", sup.get_total_resources().get(Config.SUPERVISOR_CPU_CAPACITY)));
            metrics.add(new DataPoint("usedMem", sup.get_used_mem()));
            metrics.add(new DataPoint("usedCpu", sup.get_used_cpu()));
            ret.put(info, metrics);
        }
        return ret;
    }
    
    private static Map<String, Double> setResourcesDefaultIfNotSet(Map<String, Map<String, Double>> compResourcesMap, String compId, Map<String, Object> topoConf) {
        Map<String, Double> resourcesMap = compResourcesMap.get(compId);
        if (resourcesMap == null) {
            resourcesMap = new HashMap<>();
        }
        ResourceUtils.checkIntialization(resourcesMap, compId, topoConf);
        return resourcesMap;
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
    
    private static Nimbus launchServer(Map<String, Object> conf, INimbus inimbus) throws Exception {
        StormCommon.validateDistributedMode(conf);
        validatePortAvailable(conf);
        final Nimbus nimbus = new Nimbus(conf, inimbus);
        nimbus.launchServer();
        final ThriftServer server = new ThriftServer(conf, new Processor<>(nimbus), ThriftConnectionType.NIMBUS);
        Utils.addShutdownHookWithForceKillIn1Sec(() -> {
            nimbus.shutdown();
            server.stop();
        });
        LOG.info("Starting nimbus server for storm version '{}'", STORM_VERSION);
        server.serve();
        return nimbus;
    }
    
    public static Nimbus launch(INimbus inimbus) throws Exception {
        Map<String, Object> conf = merge(Utils.readStormConfig(),
                ConfigUtils.readYamlConfig("storm-cluster-auth.yaml", false));
        return launchServer(conf, inimbus);
    }
    
    public static void main(String[] args) throws Exception {
        Utils.setupDefaultUncaughtExceptionHandler();
        launch(new StandaloneINimbus());
    }
    
    private final Map<String, Object> conf;
    private final NavigableMap<SimpleVersion, List<String>> supervisorClasspaths;
    private final NimbusInfo nimbusHostPortInfo;
    private final INimbus inimbus;
    private IAuthorizer authorizationHandler;
    private final IAuthorizer impersonationAuthorizationHandler;
    private final AtomicLong submittedCount;
    private final IStormClusterState stormClusterState;
    private final Object submitLock = new Object();
    private final Object schedLock = new Object();
    private final Object credUpdateLock = new Object();
    private final AtomicReference<Map<String, Map<List<Integer>, Map<String, Object>>>> heartbeatsCache;
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
    private final ILeaderElector leaderElector;
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
    
    private static IStormClusterState makeStormClusterState(Map<String, Object> conf) throws Exception {
        List<ACL> acls = null;
        if (Utils.isZkAuthenticationConfiguredStormServer(conf)) {
            acls = ZK_ACLS;
        }
        return ClusterUtils.mkStormClusterState(conf, acls, new ClusterStateContext(DaemonType.NIMBUS));
    }
    
    public Nimbus(Map<String, Object> conf, INimbus inimbus) throws Exception {
        this(conf, inimbus, null, null, null, null, null);
    }
    
    public Nimbus(Map<String, Object> conf, INimbus inimbus, IStormClusterState stormClusterState, NimbusInfo hostPortInfo,
                  BlobStore blobStore, ILeaderElector leaderElector, IGroupMappingServiceProvider groupMapper) throws Exception {
        this(conf, inimbus, stormClusterState, hostPortInfo, blobStore, null, leaderElector, groupMapper);
    }

    public Nimbus(Map<String, Object> conf, INimbus inimbus, IStormClusterState stormClusterState, NimbusInfo hostPortInfo,
            BlobStore blobStore, TopoCache topoCache, ILeaderElector leaderElector, IGroupMappingServiceProvider groupMapper)
        throws Exception {
        this.conf = conf;
        if (hostPortInfo == null) {
            hostPortInfo = NimbusInfo.fromConf(conf);
        }
        this.nimbusHostPortInfo = hostPortInfo;
        if (inimbus != null) {
            inimbus.prepare(conf, ServerConfigUtils.masterInimbusDir(conf));
        }
        
        this.inimbus = inimbus;
        this.authorizationHandler = StormCommon.mkAuthorizationHandler((String) conf.get(DaemonConfig.NIMBUS_AUTHORIZER), conf);
        this.impersonationAuthorizationHandler = StormCommon.mkAuthorizationHandler((String) conf.get(DaemonConfig.NIMBUS_IMPERSONATION_AUTHORIZER), conf);
        this.submittedCount = new AtomicLong(0);
        if (stormClusterState == null) {
            stormClusterState =  makeStormClusterState(conf);
        }
        this.stormClusterState = stormClusterState;
        this.heartbeatsCache = new AtomicReference<>(new HashMap<>());
        this.downloaders = fileCacheMap(conf);
        this.uploaders = fileCacheMap(conf);
        if (blobStore == null) {
            blobStore = ServerUtils.getNimbusBlobStore(conf, this.nimbusHostPortInfo);
        }
        this.blobStore = blobStore;
        if (topoCache == null) {
            topoCache = new TopoCache(blobStore, conf);
        }
        this.topoCache = topoCache;
        this.blobDownloaders = makeBlobCacheMap(conf);
        this.blobUploaders = makeBlobCacheMap(conf);
        this.blobListers = makeBlobListCachMap(conf);
        this.uptime = Utils.makeUptimeComputer();
        this.validator = ReflectionUtils.newInstance((String) conf.getOrDefault(DaemonConfig.NIMBUS_TOPOLOGY_VALIDATOR, DefaultTopologyValidator.class.getName()));
        this.timer = new StormTimer(null, (t, e) -> {
            LOG.error("Error while processing event", e);
            Utils.exitProcess(20, "Error while processing event");
        });
        this.scheduler = makeScheduler(conf, inimbus);
        if (leaderElector == null) {
            leaderElector = Zookeeper.zkLeaderElector(conf, blobStore, topoCache);
        }
        this.leaderElector = leaderElector;
        this.idToSchedStatus = new AtomicReference<>(new HashMap<>());
        this.nodeIdToResources = new AtomicReference<>(new HashMap<>());
        this.idToResources = new AtomicReference<>(new HashMap<>());
        this.idToWorkerResources = new AtomicReference<>(new HashMap<>());
        this.credRenewers = AuthUtils.GetCredentialRenewers(conf);
        this.topologyHistoryLock = new Object();
        this.topologyHistoryState = ServerConfigUtils.nimbusTopoHistoryState(conf);
        this.nimbusAutocredPlugins = AuthUtils.getNimbusAutoCredPlugins(conf);
        this.nimbusTopologyActionNotifier = createTopologyActionNotifier(conf);
        this.clusterConsumerExceutors = makeClusterMetricsConsumerExecutors(conf);
        if (groupMapper == null) {
            groupMapper = AuthUtils.GetGroupMappingServiceProviderPlugin(conf);
        }
        this.groupMapper = groupMapper;
        this.principalToLocal = AuthUtils.GetPrincipalToLocalPlugin(conf);
        this.supervisorClasspaths = Collections.unmodifiableNavigableMap(Utils.getConfiguredClasspathVersions(conf, EMPTY_STRING_LIST));// We don't use the classpath part of this, so just an empty list
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
    
    @VisibleForTesting
    public AtomicReference<Map<String,Map<List<Integer>,Map<String,Object>>>> getHeartbeatsCache() {
        return heartbeatsCache;
    }

    private BlobStore getBlobStore() {
        return blobStore;
    }

    private TopoCache getTopoCache() {
        return topoCache;
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
        mkAssignments(topoId);
    }
    
    private String toTopoId(String topoName) throws NotAliveException {
        return stormClusterState.getTopoId(topoName)
                .orElseThrow(() -> new NotAliveException(topoName + " is not alive"));
    }
    
    private void transitionName(String topoName, TopologyActions event, Object eventArg, boolean errorOnNoTransition) throws Exception {
        transition(toTopoId(topoName), event, eventArg, errorOnNoTransition);
    }

    private void transition(String topoId, TopologyActions event, Object eventArg) throws Exception {
        transition(topoId, event, eventArg, false);
    }
    
    private void transition(String topoId, TopologyActions event, Object eventArg, boolean errorOnNoTransition) throws Exception {
        LOG.info("TRANSITION: {} {} {} {}", topoId, event, eventArg, errorOnNoTransition);
        assertIsLeader();
        synchronized(submitLock) {
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
                    
                    if (TopologyActions.STARTUP != event) {
                        //STARTUP is a system event so don't log an issue
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
        String codeKey = ConfigUtils.masterStormCodeKey(topoId);
        String confKey = ConfigUtils.masterStormConfKey(topoId);
        NimbusInfo hostPortInfo = nimbusHostPortInfo;
        if (tmpJarLocation != null) {
            //in local mode there is no jar
            try (FileInputStream fin = new FileInputStream(tmpJarLocation)) {
                store.createBlob(jarKey, fin, new SettableBlobMeta(BlobStoreAclHandler.DEFAULT), subject);
            }
            if (store instanceof LocalFsBlobStore) {
                clusterState.setupBlobstore(jarKey, hostPortInfo, getVersionForKey(jarKey, hostPortInfo, conf));
            }
        }

        topoCache.addTopoConf(topoId, subject, topoConf);
        if (store instanceof LocalFsBlobStore) {
            clusterState.setupBlobstore(confKey, hostPortInfo, getVersionForKey(confKey, hostPortInfo, conf));
        }

        topoCache.addTopology(topoId, subject, topology);
        if (store instanceof LocalFsBlobStore) {
            clusterState.setupBlobstore(codeKey, hostPortInfo, getVersionForKey(codeKey, hostPortInfo, conf));
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
            while (jarCount < minReplicationCount &&
                    codeCount < minReplicationCount &&
                    confCount < minReplicationCount) {
                if (maxWaitTime > 0 && totalWaitTime > maxWaitTime) {
                    LOG.info("desired replication count of {} not achieved but we have hit the max wait time {}"
                            + " so moving on with replication count for conf key = {} for code key = {} for jar key = ",
                            minReplicationCount, maxWaitTime, confCount, codeCount, jarCount);
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
        LOG.info("desired replication count {} achieved, current-replication-count for conf key = {},"
                + " current-replication-count for code key = {}, current-replication-count for jar key = {}", 
                minReplicationCount, confCount, codeCount, jarCount);
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
        Map<List<Integer>, String> rawExecToComponent = computeExecutorToComponent(topoId, base);
        Map<ExecutorDetails, String> executorsToComponent = new HashMap<>();
        for (Entry<List<Integer>, String> entry: rawExecToComponent.entrySet()) {
            List<Integer> execs = entry.getKey();
            ExecutorDetails execDetails = new ExecutorDetails(execs.get(0), execs.get(1));
            executorsToComponent.put(execDetails, entry.getValue());
        }
        
        return new TopologyDetails(topoId, topoConf, topo, base.get_num_workers(), executorsToComponent, base.get_launch_time_secs(),
            base.get_owner());
    }
    
    private void updateHeartbeats(String topoId, Set<List<Integer>> allExecutors, Assignment existingAssignment) {
        LOG.debug("Updating heartbeats for {} {}", topoId, allExecutors);
        IStormClusterState state = stormClusterState;
        Map<List<Integer>, Map<String, Object>> executorBeats = StatsUtil.convertExecutorBeats(state.executorBeats(topoId, existingAssignment.get_executor_node_port()));
        Map<List<Integer>, Map<String, Object>> cache = StatsUtil.updateHeartbeatCache(heartbeatsCache.get().get(topoId),
            executorBeats, allExecutors, ObjectReader.getInt(conf.get(DaemonConfig.NIMBUS_TASK_TIMEOUT_SECS)));
        heartbeatsCache.getAndUpdate(new Assoc<>(topoId, cache));
    }
    
    /**
     * Update all the heartbeats for all the topologies' executors.
     * @param existingAssignments current assignments (thrift)
     * @param topologyToExecutors topology ID to executors.
     */
    private void updateAllHeartbeats(Map<String, Assignment> existingAssignments, Map<String, Set<List<Integer>>> topologyToExecutors) {
        for (Entry<String, Assignment> entry: existingAssignments.entrySet()) {
            String topoId = entry.getKey();
            updateHeartbeats(topoId, topologyToExecutors.get(topoId), entry.getValue());
        }
    }
    
    private Set<List<Integer>> aliveExecutors(TopologyDetails td, Set<List<Integer>> allExecutors, Assignment assignment) {
        String topoId = td.getId();
        Map<List<Integer>, Map<String, Object>> hbCache = heartbeatsCache.get().get(topoId);
        LOG.debug("NEW  Computing alive executors for {}\nExecutors: {}\nAssignment: {}\nHeartbeat cache: {}",
                topoId, allExecutors, assignment, hbCache);
        
        int taskLaunchSecs = ObjectReader.getInt(conf.get(DaemonConfig.NIMBUS_TASK_LAUNCH_SECS));
        Set<List<Integer>> ret = new HashSet<>();
        Map<List<Long>, Long> execToStartTimes = assignment.get_executor_start_time_secs();

        for (List<Integer> exec: allExecutors) {
            List<Long> longExec = new ArrayList<Long>(exec.size());
            for (Integer num : exec) {
                longExec.add(num.longValue());
            }

            Long startTime = execToStartTimes.get(longExec);
            Boolean isTimedOut = (Boolean)hbCache.get(StatsUtil.convertExecutor(longExec)).get("is-timed-out");
            Integer delta = startTime == null ? null : Time.deltaSecs(startTime.intValue());
            if (startTime != null && ((delta < taskLaunchSecs) || !isTimedOut)) {
                ret.add(exec);
            } else {
                LOG.info("Executor {}:{} not alive", topoId, exec);
            }
        }
        return ret;
    }
    
    private List<List<Integer>> computeExecutors(String topoId, StormBase base) throws KeyNotFoundException, AuthorizationException, IOException, InvalidTopologyException {
        assert (base != null);

        Map<String, Integer> compToExecutors = base.get_component_executors();
        Map<String, Object> topoConf = readTopoConfAsNimbus(topoId, topoCache);
        StormTopology topology = readStormTopologyAsNimbus(topoId, topoCache);
        List<List<Integer>> ret = new ArrayList<>();
        if (compToExecutors != null) {
            Map<Integer, String> taskInfo = StormCommon.stormTaskInfo(topology, topoConf);
            Map<String, List<Integer>> compToTaskList = Utils.reverseMap(taskInfo);
            for (Entry<String, List<Integer>> entry: compToTaskList.entrySet()) {
                List<Integer> comps = entry.getValue();
                comps.sort(null);
                Integer numExecutors = compToExecutors.get(entry.getKey());
                if (numExecutors != null) {
                    List<List<Integer>> partitioned = Utils.partitionFixed(numExecutors, comps);
                    for (List<Integer> partition: partitioned) {
                        ret.add(Arrays.asList(partition.get(0), partition.get(partition.size() - 1)));
                    }
                }
            }
        }
        return ret;
    }
    
    private Map<List<Integer>, String> computeExecutorToComponent(String topoId, StormBase base) throws KeyNotFoundException, AuthorizationException, InvalidTopologyException, IOException {
        List<List<Integer>> executors = computeExecutors(topoId, base);
        StormTopology topology = readStormTopologyAsNimbus(topoId, topoCache);
        Map<String, Object> topoConf = readTopoConfAsNimbus(topoId, topoCache);
        Map<Integer, String> taskToComponent = StormCommon.stormTaskInfo(topology, topoConf);
        Map<List<Integer>, String> ret = new HashMap<>();
        for (List<Integer> executor: executors) {
            ret.put(executor, taskToComponent.get(executor.get(0)));
        }
        return ret;
    }
    
    private Map<String, Set<List<Integer>>> computeTopologyToExecutors(Map<String, StormBase> bases) throws KeyNotFoundException, AuthorizationException, InvalidTopologyException, IOException {
        Map<String, Set<List<Integer>>> ret = new HashMap<>();
        if (bases != null) {
            for (Entry<String, StormBase> entry: bases.entrySet()) {
                String topoId = entry.getKey();
                ret.put(topoId, new HashSet<>(computeExecutors(topoId, entry.getValue())));
            }
        }
        return ret;
    }
    
    /**
     * compute a topology-id -> alive executors map
     * @param existingAssignment the current assignments
     * @param topologies the current topologies
     * @param topologyToExecutors the executors for the current topologies
     * @param scratchTopologyId the topology being rebalanced and should be excluded
     * @return the map of topology id to alive executors
     */
    private Map<String, Set<List<Integer>>> computeTopologyToAliveExecutors(Map<String, Assignment> existingAssignment, Topologies topologies, 
            Map<String, Set<List<Integer>>> topologyToExecutors, String scratchTopologyId) {
        Map<String, Set<List<Integer>>> ret = new HashMap<>();
        for (Entry<String, Assignment> entry: existingAssignment.entrySet()) {
            String topoId = entry.getKey();
            Assignment assignment = entry.getValue();
            TopologyDetails td = topologies.getById(topoId);
            Set<List<Integer>> allExecutors = topologyToExecutors.get(topoId);
            Set<List<Integer>> aliveExecutors;
            if (topoId.equals(scratchTopologyId)) {
                aliveExecutors = allExecutors;
            } else {
                aliveExecutors = new HashSet<>(aliveExecutors(td, allExecutors, assignment));
            }
            ret.put(topoId, aliveExecutors);
        }
        return ret;
    }
    
    private static List<Integer> asIntExec(List<Long> exec) {
        List<Integer> ret = new ArrayList<>(2);
        ret.add(exec.get(0).intValue());
        ret.add(exec.get(1).intValue());
        return ret;
    }
    
    private Map<String, Set<Long>> computeSupervisorToDeadPorts(Map<String, Assignment> existingAssignments, Map<String, Set<List<Integer>>> topologyToExecutors,
            Map<String, Set<List<Integer>>> topologyToAliveExecutors) {
        Map<String, Set<Long>> ret = new HashMap<>();
        for (Entry<String, Assignment> entry: existingAssignments.entrySet()) {
            String topoId = entry.getKey();
            Assignment assignment = entry.getValue();
            Set<List<Integer>> allExecutors = topologyToExecutors.get(topoId);
            Set<List<Integer>> aliveExecutors = topologyToAliveExecutors.get(topoId);
            Set<List<Integer>> deadExecutors = new HashSet<>(allExecutors);
            deadExecutors.removeAll(aliveExecutors);
            Map<List<Long>, NodeInfo> execToNodePort = assignment.get_executor_node_port();
            for (Entry<List<Long>, NodeInfo> assigned: execToNodePort.entrySet()) {
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
     * @param existingAssignments current assignments
     * @param topologyToAliveExecutors executors that are alive
     * @return topo ID to schedulerAssignment
     */
    private Map<String, SchedulerAssignmentImpl> computeTopologyToSchedulerAssignment(Map<String, Assignment> existingAssignments,
            Map<String, Set<List<Integer>>> topologyToAliveExecutors) {
        Map<String, SchedulerAssignmentImpl> ret = new HashMap<>();
        for (Entry<String, Assignment> entry: existingAssignments.entrySet()) {
            String topoId = entry.getKey();
            Assignment assignment = entry.getValue();
            Set<List<Integer>> aliveExecutors = topologyToAliveExecutors.get(topoId);
            Map<List<Long>, NodeInfo> execToNodePort = assignment.get_executor_node_port();
            Map<NodeInfo, WorkerResources> workerToResources = assignment.get_worker_resources();
            Map<NodeInfo, WorkerSlot> nodePortToSlot = new HashMap<>();
            Map<WorkerSlot, WorkerResources> slotToResources = new HashMap<>();
            for (Entry<NodeInfo, WorkerResources> nodeAndResources: workerToResources.entrySet()) {
                NodeInfo info = nodeAndResources.getKey();
                WorkerResources resources = nodeAndResources.getValue();
                WorkerSlot slot = new WorkerSlot(info.get_node(), info.get_port_iterator().next());
                nodePortToSlot.put(info, slot);
                slotToResources.put(slot, resources);
            }
            Map<ExecutorDetails, WorkerSlot> execToSlot = new HashMap<>();
            for (Entry<List<Long>, NodeInfo> execAndNodePort: execToNodePort.entrySet()) {
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
     * @param superToDeadPorts dead ports on the supervisor
     * @param topologies all of the topologies
     * @param missingAssignmentTopologies topologies that need assignments
     * @return a map: {supervisor-id SupervisorDetails}
     */
    private Map<String, SupervisorDetails> readAllSupervisorDetails(Map<String, Set<Long>> superToDeadPorts,
            Topologies topologies, Collection<String> missingAssignmentTopologies) {
        Map<String, SupervisorDetails> ret = new HashMap<>();
        IStormClusterState state = stormClusterState;
        Map<String, SupervisorInfo> superInfos = state.allSupervisorInfo();
        List<SupervisorDetails> superDetails = new ArrayList<>();
        for (Entry<String, SupervisorInfo> entry: superInfos.entrySet()) {
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
        for (Entry<String, SupervisorInfo> entry: superInfos.entrySet()) {
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
    
    private Map<String, SchedulerAssignment> computeNewSchedulerAssignments(Map<String, Assignment> existingAssignments,
            Topologies topologies, Map<String, StormBase> bases, String scratchTopologyId) throws KeyNotFoundException, AuthorizationException, InvalidTopologyException, IOException {
        Map<String, Set<List<Integer>>> topoToExec = computeTopologyToExecutors(bases);
        
        updateAllHeartbeats(existingAssignments, topoToExec);

        Map<String, Set<List<Integer>>> topoToAliveExecutors = computeTopologyToAliveExecutors(existingAssignments, topologies,
                topoToExec, scratchTopologyId);
        Map<String, Set<Long>> supervisorToDeadPorts = computeSupervisorToDeadPorts(existingAssignments, topoToExec,
                topoToAliveExecutors);
        Map<String, SchedulerAssignmentImpl> topoToSchedAssignment = computeTopologyToSchedulerAssignment(existingAssignments,
                topoToAliveExecutors);
        Set<String> missingAssignmentTopologies = new HashSet<>();
        for (TopologyDetails topo: topologies.getTopologies()) {
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
        Map<String, SupervisorDetails> supervisors = readAllSupervisorDetails(supervisorToDeadPorts, topologies, missingAssignmentTopologies);
        Cluster cluster = new Cluster(inimbus, supervisors, topoToSchedAssignment, topologies, conf);
        cluster.setStatusMap(idToSchedStatus.get());
        scheduler.schedule(topologies, cluster);

        //merge with existing statuses
        idToSchedStatus.set(merge(idToSchedStatus.get(), cluster.getStatusMap()));
        nodeIdToResources.set(cluster.getSupervisorsResourcesMap());
        
        // This is a hack for non-ras scheduler topology and worker resources
        Map<String, TopologyResources> resources = cluster.getTopologyResourcesMap();
        idToResources.getAndAccumulate(resources, (orig, update) -> merge(orig, update));
        
        Map<String, Map<WorkerSlot, WorkerResources>> workerResources = new HashMap<>();
        for (Entry<String, Map<WorkerSlot, WorkerResources>> uglyWorkerResources: cluster.getWorkerResourcesMap().entrySet()) {
            Map<WorkerSlot, WorkerResources> slotToResources = new HashMap<>();
            for (Entry<WorkerSlot, WorkerResources> uglySlotToResources : uglyWorkerResources.getValue().entrySet()) {
                WorkerResources wr = uglySlotToResources.getValue();
                slotToResources.put(uglySlotToResources.getKey(), wr);
            }
            workerResources.put(uglyWorkerResources.getKey(), slotToResources);
        }
        idToWorkerResources.getAndAccumulate(workerResources, (orig, update) -> merge(orig, update));
        
        return cluster.getAssignments();
    }
    
    private TopologyResources getResourcesForTopology(String topoId, StormBase base) throws NotAliveException, AuthorizationException, InvalidTopologyException, IOException {
        TopologyResources ret = idToResources.get().get(topoId);
        if (ret == null) {
            try {
                IStormClusterState state = stormClusterState;
                TopologyDetails details = readTopologyDetails(topoId, base);
                Assignment assignment = state.assignmentInfo(topoId, null);
                ret = new TopologyResources(details, assignment);
            } catch(KeyNotFoundException e) {
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
                for (Entry<NodeInfo, WorkerResources> entry: assignment.get_worker_resources().entrySet()) {
                    NodeInfo ni = entry.getKey();
                    WorkerSlot slot = new WorkerSlot(ni.get_node(), ni.get_port_iterator().next());
                    ret.put(slot, entry.getValue());
                }
                idToWorkerResources.getAndUpdate(new Assoc<>(topoId, ret));
            }
        }
        return ret;
    }

    private void mkAssignments() throws Exception {
        mkAssignments(null);
    }
    
    private void mkAssignments(String scratchTopoId) throws Exception {
        if (!isLeader()) {
            LOG.info("not a leader, skipping assignments");
            return;
        }
        // get existing assignment (just the topologyToExecutorToNodePort map) -> default to {}
        // filter out ones which have a executor timeout
        // figure out available slots on cluster. add to that the used valid slots to get total slots. figure out how many executors should be in each slot (e.g., 4, 4, 4, 5)
        // only keep existing slots that satisfy one of those slots. for rest, reassign them across remaining slots
        // edge case for slots with no executor timeout but with supervisor timeout... just treat these as valid slots that can be reassigned to. worst comes to worse the executor will timeout and won't assign here next time around

        IStormClusterState state = stormClusterState;
        //read all the topologies
        Map<String, StormBase> bases;
        Map<String, TopologyDetails> tds = new HashMap<>();
        synchronized (submitLock) {
            bases = state.topologyBases();
            for (Iterator<Entry<String, StormBase>> it = bases.entrySet().iterator(); it.hasNext(); ) {
                Entry<String, StormBase> entry = it.next();
                String id = entry.getKey();
                try {
                    tds.put(id, readTopologyDetails(id, entry.getValue()));
                } catch (KeyNotFoundException e) {
                    //A race happened and it is probably not running
                    it.remove();
                }
            }
        }
        Topologies topologies = new Topologies(tds);
        List<String> assignedTopologyIds = state.assignments(null);
        Map<String, Assignment> existingAssignments = new HashMap<>();
        for (String id: assignedTopologyIds) {
            //for the topology which wants rebalance (specified by the scratchTopoId)
            // we exclude its assignment, meaning that all the slots occupied by its assignment
            // will be treated as free slot in the scheduler code.
            if (!id.equals(scratchTopoId)) {
                Assignment currentAssignment = state.assignmentInfo(id, null);
                if (!currentAssignment.is_set_owner()) {
                    TopologyDetails td = tds.get(id);
                    if (td != null) {
                        currentAssignment.set_owner(td.getTopologySubmitter());
                        state.setAssignment(id, currentAssignment);
                    }
                }
                existingAssignments.put(id, currentAssignment);
            }
        }
        // make the new assignments for topologies
        Map<String, SchedulerAssignment> newSchedulerAssignments = null;
        synchronized (schedLock) {
            newSchedulerAssignments = computeNewSchedulerAssignments(existingAssignments, topologies, bases, scratchTopoId);

            Map<String, Map<List<Long>, List<Object>>> topologyToExecutorToNodePort = computeNewTopoToExecToNodePort(newSchedulerAssignments, existingAssignments);
            for (String id: assignedTopologyIds) {
                if (!topologyToExecutorToNodePort.containsKey(id)) {
                    topologyToExecutorToNodePort.put(id, null);
                }
            }
            Map<String, Map<WorkerSlot, WorkerResources>> newAssignedWorkerToResources = computeTopoToNodePortToResources(newSchedulerAssignments);
            int nowSecs = Time.currentTimeSecs();
            Map<String, SupervisorDetails> basicSupervisorDetailsMap = basicSupervisorDetailsMap(state);
            //construct the final Assignments by adding start-times etc into it
            Map<String, Assignment> newAssignments  = new HashMap<>();
            for (Entry<String, Map<List<Long>, List<Object>>> entry: topologyToExecutorToNodePort.entrySet()) {
                String topoId = entry.getKey();
                Map<List<Long>, List<Object>> execToNodePort = entry.getValue();
                if (execToNodePort == null) {
                    execToNodePort = new HashMap<>();
                }
                Assignment existingAssignment = existingAssignments.get(topoId);
                Set<String> allNodes = new HashSet<>();
                if (execToNodePort != null) {
                    for (List<Object> nodePort: execToNodePort.values()) {
                        allNodes.add((String) nodePort.get(0));
                    }
                }
                Map<String, String> allNodeHost = new HashMap<>();
                if (existingAssignment != null) {
                    allNodeHost.putAll(existingAssignment.get_node_host());
                }
                for (String node: allNodes) {
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
                for (List<Long> id: reassignExecutors) {
                    startTimes.put(id, (long)nowSecs);
                }
                Map<WorkerSlot, WorkerResources> workerToResources = newAssignedWorkerToResources.get(topoId);
                if (workerToResources == null) {
                    workerToResources = new HashMap<>();
                }
                Assignment newAssignment = new Assignment((String)conf.get(Config.STORM_LOCAL_DIR));
                Map<String, String> justAssignedKeys = new HashMap<>(allNodeHost);
                //Modifies justAssignedKeys
                justAssignedKeys.keySet().retainAll(allNodes);
                newAssignment.set_node_host(justAssignedKeys);
                //convert NodePort to NodeInfo (again!!!).
                Map<List<Long>, NodeInfo> execToNodeInfo = new HashMap<>();
                for (Entry<List<Long>, List<Object>> execAndNodePort: execToNodePort.entrySet()) {
                    List<Object> nodePort = execAndNodePort.getValue();
                    NodeInfo ni = new NodeInfo();
                    ni.set_node((String) nodePort.get(0));
                    ni.add_to_port((Long)nodePort.get(1));
                    execToNodeInfo.put(execAndNodePort.getKey(), ni);
                }
                newAssignment.set_executor_node_port(execToNodeInfo);
                newAssignment.set_executor_start_time_secs(startTimes);
                //do another conversion (lets just make this all common)
                Map<NodeInfo, WorkerResources> workerResources = new HashMap<>();
                for (Entry<WorkerSlot, WorkerResources> wr: workerToResources.entrySet()) {
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

            if (!newAssignments.equals(existingAssignments)) {
                LOG.debug("RESETTING id->resources and id->worker-resources cache!");
                idToResources.set(new HashMap<>());
                idToWorkerResources.set(new HashMap<>());
            }

            //tasks figure out what tasks to talk to by looking at topology at runtime
            // only log/set when there's been a change to the assignment
            for (Entry<String, Assignment> entry: newAssignments.entrySet()) {
                String topoId = entry.getKey();
                Assignment assignment = entry.getValue();
                Assignment existingAssignment = existingAssignments.get(topoId);
                //NOT Used TopologyDetails topologyDetails = topologies.getById(topoId);
                if (assignment.equals(existingAssignment)) {
                    LOG.debug("Assignment for {} hasn't changed", topoId);
                } else {
                    LOG.info("Setting new assignment for topology id {}: {}", topoId, assignment);
                    state.setAssignment(topoId, assignment);
                }
            }

            Map<String, Collection<WorkerSlot>> addedSlots = new HashMap<>();
            for (Entry<String, Assignment> entry: newAssignments.entrySet()) {
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

    private void startTopology(String topoName, String topoId, TopologyStatus initStatus, String owner, String principal)
        throws KeyNotFoundException, AuthorizationException, IOException, InvalidTopologyException {
        assert(TopologyStatus.ACTIVE == initStatus || TopologyStatus.INACTIVE == initStatus);
        IStormClusterState state = stormClusterState;
        Map<String, Object> topoConf = readTopoConf(topoId, topoCache);
        StormTopology topology = StormCommon.systemTopology(topoConf, readStormTopology(topoId, topoCache));
        Map<String, Integer> numExecutors = new HashMap<>();
        for (Entry<String, Object> entry: StormCommon.allComponents(topology).entrySet()) {
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
        state.activateStorm(topoId, base);
        notifyTopologyActionListener(topoName, "activate");
    }
    
    private void assertTopoActive(String topoName, boolean expectActive) throws NotAliveException, AlreadyAliveException {
        if (isTopologyActive(stormClusterState, topoName) != expectActive) {
            if (expectActive) {
                throw new NotAliveException(topoName + " is not alive");
            }
            throw new AlreadyAliveException(topoName + " is already alive");
        }
    }
    
    private Map<String, Object> tryReadTopoConfFromName(final String topoName) throws NotAliveException, AuthorizationException, IOException {
        IStormClusterState state = stormClusterState;
        String topoId = state.getTopoId(topoName)
                .orElseThrow(() -> new NotAliveException(topoName + " is not alive"));
        return tryReadTopoConf(topoId, topoCache);
    }

    @VisibleForTesting
    public void checkAuthorization(String topoName, Map<String, Object> topoConf, String operation) throws AuthorizationException {
        checkAuthorization(topoName, topoConf, operation, null);
    }
    
    @VisibleForTesting
    public void checkAuthorization(String topoName, Map<String, Object> topoConf, String operation, ReqContext context) throws AuthorizationException {
        IAuthorizer aclHandler = authorizationHandler;
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
            LOG.warn("principal: {} is trying to impersonate principal: {}", context.realPrincipal(), context.principal());
            if (impersonationAuthorizer == null) {
                LOG.warn("impersonation attempt but {} has no authorizer configured. potential security risk, "
                        + "please see SECURITY.MD to learn how to configure impersonation authorizer.", DaemonConfig.NIMBUS_IMPERSONATION_AUTHORIZER);
            } else {
                if (!impersonationAuthorizer.permit(context, operation, checkConf)) {
                    ThriftAccessLogger.logAccess(context.requestID(), context.remoteAddress(),
                            context.principal(), operation, topoName, "access-denied");
                    throw new AuthorizationException("principal " + context.realPrincipal() + 
                            " is not authorized to impersonate principal " + context.principal() +
                            " from host " + context.remoteAddress() +
                            " Please see SECURITY.MD to learn how to configure impersonation acls.");
                }
            }
        }
        
        if (aclHandler != null) {
            if (!aclHandler.permit(context, operation, checkConf)) {
              ThriftAccessLogger.logAccess(context.requestID(), context.remoteAddress(), context.principal(), operation,
                      topoName, "access-denied");
              throw new AuthorizationException( operation + (topoName != null ? " on topology " + topoName : "") + 
                      " is not authorized");
            } else {
              ThriftAccessLogger.logAccess(context.requestID(), context.remoteAddress(), context.principal(),
                      operation, topoName, "access-granted");
            }
        }
    }
    
    private boolean isAuthorized(String operation, String topoId) throws NotAliveException, AuthorizationException, IOException {
        Map<String, Object> topoConf = tryReadTopoConf(topoId, topoCache);
        topoConf = merge(conf, topoConf);
        String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);
        try {
            checkAuthorization(topoName, topoConf, operation);
            return true;
        } catch (AuthorizationException e) {
            return false;
        }
    }
    
    @VisibleForTesting
    public Set<String> filterAuthorized(String operation, Collection<String> topoIds) throws NotAliveException, AuthorizationException, IOException {
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
                for (String key: dependencyJars) {
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
        synchronized(submitLock) {
            toClean = topoIdsToClean(state, blobStore);
        }
        if (toClean != null) {
            for (String topoId: toClean) {
                LOG.info("Cleaning up {}", topoId);
                state.teardownHeartbeats(topoId);
                state.teardownTopologyErrors(topoId);
                state.removeBackpressure(topoId);
                rmDependencyJarsInTopology(topoId);
                forceDeleteTopoDistDir(topoId);
                rmTopologyKeys(topoId);
                heartbeatsCache.getAndUpdate(new Dissoc<>(topoId));
            }
        }
    }
    
    /**
     * Deletes topologies from history older than mins minutes.
     * @param mins the number of mins for old topologies
     */
    private void cleanTopologyHistory(int mins) {
        int cutoffAgeSecs = Time.currentTimeSecs() - (mins * 60);
        synchronized(topologyHistoryLock) {
            LocalState state = topologyHistoryState;
            state.filterOldTopologies(cutoffAgeSecs);
        }
    }

    /**
     * Sets up blobstore state for all current keys.
     * @throws KeyNotFoundException 
     * @throws AuthorizationException 
     */
    private void setupBlobstore() throws AuthorizationException, KeyNotFoundException {
        IStormClusterState state = stormClusterState;
        BlobStore store = blobStore;
        Set<String> localKeys = new HashSet<>();
        for (Iterator<String> it = store.listKeys(); it.hasNext();) {
            localKeys.add(it.next());
        }
        Set<String> activeKeys = new HashSet<>(state.activeKeys());
        Set<String> activeLocalKeys = new HashSet<>(localKeys);
        activeLocalKeys.retainAll(activeKeys);
        Set<String> keysToDelete = new HashSet<>(localKeys);
        keysToDelete.removeAll(activeKeys);
        NimbusInfo nimbusInfo = nimbusHostPortInfo;
        LOG.debug("Deleting keys not on the zookeeper {}", keysToDelete);
        for (String toDelete: keysToDelete) {
            store.deleteBlob(toDelete, NIMBUS_SUBJECT);
        }
        LOG.debug("Creating list of key entries for blobstore inside zookeeper {} local {}", activeKeys, activeLocalKeys);
        for (String key: activeLocalKeys) {
            try {
                state.setupBlobstore(key, nimbusInfo, getVersionForKey(key, nimbusInfo, conf));
            } catch (KeyNotFoundException e) {
                // invalid key, remove it from blobstore
                store.deleteBlob(key, NIMBUS_SUBJECT);
            }
        }
    }

    private void addTopoToHistoryLog(String topoId, Map<String, Object> topoConf) {
        LOG.info("Adding topo to history log: {}", topoId);
        LocalState state = topologyHistoryState;
        List<String> users = ServerConfigUtils.getTopoLogsUsers(topoConf);
        List<String> groups = ServerConfigUtils.getTopoLogsGroups(topoConf);
        synchronized(topologyHistoryLock) {
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
     * Check to see if any of the users groups intersect with the list of groups passed in
     * @param user the user to check
     * @param groupsToCheck the groups to see if user is a part of
     * @return true if user is a part of groups, else false
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
        for (LSTopoHistory history: topoHistoryList) {
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
        Object lock = credUpdateLock;
        Map<String, StormBase> assignedBases = state.topologyBases();
        if (assignedBases != null) {
            for (Entry<String, StormBase> entry: assignedBases.entrySet()) {
                String id = entry.getKey();
                String ownerPrincipal = entry.getValue().get_principal();
                Map<String, Object> topoConf = Collections.unmodifiableMap(merge(conf, tryReadTopoConf(id, topoCache)));
                synchronized(lock) {
                    Credentials origCreds = state.credentials(id, null);
                    if (origCreds != null) {
                        Map<String, String> origCredsMap = origCreds.get_creds();
                        Map<String, String> newCredsMap = new HashMap<>(origCredsMap);
                        for (ICredentialsRenewer renewer: renewers) {
                            LOG.info("Renewing Creds For {} with {} owned by {}", id, renewer, ownerPrincipal);
                            renewer.renew(newCredsMap, topoConf, ownerPrincipal);
                        }
                        if (!newCredsMap.equals(origCredsMap)) {
                            state.setCredentials(id, new Credentials(newCredsMap), topoConf);
                        }
                    }
                }
            }
        }
    }

    private void blobSync() throws Exception {
        if ("distributed".equals(conf.get(Config.STORM_CLUSTER_MODE))) {
            if (!isLeader()) {
                IStormClusterState state = stormClusterState;
                NimbusInfo nimbusInfo = nimbusHostPortInfo;
                BlobStore store = blobStore;
                Set<String> allKeys = new HashSet<>();
                for (Iterator<String> it = store.listKeys(); it.hasNext();) {
                    allKeys.add(it.next());
                }
                Set<String> zkKeys = new HashSet<>(state.blobstore(() -> {
                    try {
                        this.blobSync();
                    } catch(Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
                LOG.debug("blob-sync blob-store-keys {} zookeeper-keys {}", allKeys, zkKeys);
                BlobSynchronizer sync = new BlobSynchronizer(store, conf);
                sync.setNimbusInfo(nimbusInfo);
                sync.setBlobStoreKeySet(allKeys);
                sync.setZookeeperKeySet(zkKeys);
                sync.syncBlobs();
            } //else not leader (NOOP)
        } //else local (NOOP)
    }

    private SupervisorSummary makeSupervisorSummary(String supervisorId, SupervisorInfo info) {
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
        if (resources != null) {
            ret.set_used_mem(resources.getUsedMem());
            ret.set_used_cpu(resources.getUsedCpu());
        }
        if (info.is_set_version()) {
            ret.set_version(info.get_version());
        }
        return ret;
    }

    private ClusterSummary getClusterInfoImpl() throws Exception {
        IStormClusterState state = stormClusterState;
        Map<String, SupervisorInfo> infos = state.allSupervisorInfo();
        List<SupervisorSummary> summaries = new ArrayList<>(infos.size());
        for (Entry<String, SupervisorInfo> entry: infos.entrySet()) {
            summaries.add(makeSupervisorSummary(entry.getKey(), entry.getValue()));
        }
        int uptime = this.uptime.upTime();
        Map<String, StormBase> bases = state.topologyBases();

        List<NimbusSummary> nimbuses = state.nimbuses();
        //update the isLeader field for each nimbus summary
        NimbusInfo leader = leaderElector.getLeader();
        for (NimbusSummary nimbusSummary: nimbuses) {
            nimbusSummary.set_uptime_secs(Time.deltaSecs(nimbusSummary.get_uptime_secs()));
            nimbusSummary.set_isLeader(leader.getHost().equals(nimbusSummary.get_host()) &&
                    leader.getPort() == nimbusSummary.get_port());
        }
        
        List<TopologySummary> topologySummaries = new ArrayList<>();
        for (Entry<String, StormBase> entry: bases.entrySet()) {
            StormBase base = entry.getValue();
            if (base == null) {
                continue;
            }
            String topoId = entry.getKey();
            Assignment assignment = state.assignmentInfo(topoId, null);
            
            int numTasks = 0;
            int numExecutors = 0;
            int numWorkers = 0;
            if (assignment != null && assignment.is_set_executor_node_port()) {
                for (List<Long> ids: assignment.get_executor_node_port().keySet()) {
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
                summary.set_assigned_memonheap(resources.getAssignedMemOnHeap());
                summary.set_assigned_memoffheap(resources.getAssignedMemOffHeap());
                summary.set_assigned_cpu(resources.getAssignedCpu());
            }
            summary.set_replication_count(getBlobReplicationCount(ConfigUtils.masterStormCodeKey(topoId)));
            topologySummaries.add(summary);
        }
        
        ClusterSummary ret = new ClusterSummary(summaries, topologySummaries, nimbuses);
        ret.set_nimbus_uptime_secs(uptime);
        return ret;
    }
    
    private void sendClusterMetricsToExecutors() throws Exception {
        ClusterInfo clusterInfo = mkClusterInfo();
        ClusterSummary clusterSummary = getClusterInfoImpl();
        List<DataPoint> clusterMetrics = extractClusterMetrics(clusterSummary);
        Map<IClusterMetricsConsumer.SupervisorInfo, List<DataPoint>> supervisorMetrics = extractSupervisorMetrics(clusterSummary);
        for (ClusterMetricsConsumerExecutor consumerExecutor: clusterConsumerExceutors) {
            consumerExecutor.handleDataPoints(clusterInfo, clusterMetrics);
            for (Entry<IClusterMetricsConsumer.SupervisorInfo, List<DataPoint>> entry: supervisorMetrics.entrySet()) {
                consumerExecutor.handleDataPoints(entry.getKey(), entry.getValue());
            }
        }
    }

    private CommonTopoInfo getCommonTopoInfo(String topoId, String operation) throws NotAliveException, AuthorizationException, IOException, InvalidTopologyException {
        IStormClusterState state = stormClusterState;
        CommonTopoInfo ret = new CommonTopoInfo();
        ret.topoConf = tryReadTopoConf(topoId, topoCache);
        ret.topoName = (String)ret.topoConf.get(Config.TOPOLOGY_NAME);
        checkAuthorization(ret.topoName, ret.topoConf, operation);
        ret.topology = tryReadTopology(topoId, topoCache);
        ret.taskToComponent = StormCommon.stormTaskInfo(ret.topology, ret.topoConf);
        ret.base = state.stormBase(topoId, null);
        if (ret.base != null && ret.base.is_set_launch_time_secs()) {
            ret.launchTimeSecs = ret.base.get_launch_time_secs();
        } else {
            ret.launchTimeSecs = 0;
        }
        ret.assignment = state.assignmentInfo(topoId, null);
        ret.beats = Utils.OR(heartbeatsCache.get().get(topoId), Collections.emptyMap());
        ret.allComponents = new HashSet<>(ret.taskToComponent.values());
        return ret;
    }
    
    @VisibleForTesting
    public void launchServer() throws Exception {
        try {
            BlobStore store = blobStore;
            IStormClusterState state = stormClusterState;
            NimbusInfo hpi = nimbusHostPortInfo;
            
            LOG.info("Starting Nimbus with conf {}", conf);
            validator.prepare(conf);
            
            //add to nimbuses
            state.addNimbusHost(hpi.getHost(), new NimbusSummary(hpi.getHost(), hpi.getPort(), Time.currentTimeSecs(), false, STORM_VERSION));
            leaderElector.addToLeaderLockQueue();
            
            if (store instanceof LocalFsBlobStore) {
                //register call back for blob-store
                state.blobstore(() -> {
                    try {
                        blobSync();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                setupBlobstore();
            }
            
            for (ClusterMetricsConsumerExecutor exec: clusterConsumerExceutors) {
                exec.prepare();
            }

            if (isLeader()) {
                for (String topoId: state.activeStorms()) {
                    transition(topoId, TopologyActions.STARTUP, null);
                }
            }
            
            final boolean doNotReassign = (Boolean)conf.getOrDefault(ServerConfigUtils.NIMBUS_DO_NOT_REASSIGN, false);
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
            
            //Schedule nimbus code sync thread to sync code from other nimbuses.
            if (store instanceof LocalFsBlobStore) {
                timer.scheduleRecurring(0, ObjectReader.getInt(conf.get(DaemonConfig.NIMBUS_CODE_SYNC_FREQ_SECS)),
                        () -> {
                            try {
                                blobSync();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
            
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
            
            StormMetricsRegistry.registerGauge("nimbus:num-supervisors", () -> state.supervisors(null));
            StormMetricsRegistry.startMetricsReporters(conf);
            
            if (clusterConsumerExceutors != null) {
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
            }
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
    
    //THRIFT SERVER METHODS...
    
    @Override
    public void submitTopology(String name, String uploadedJarLocation, String jsonConf, StormTopology topology)
            throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, TException {
        submitTopologyCalls.mark();
        submitTopologyWithOpts(name, uploadedJarLocation, jsonConf, topology, new SubmitOptions(TopologyInitialStatus.ACTIVE));
    }

    @VisibleForTesting
    static void validateTopologyWorkerMaxHeapSizeConfigs(
        Map<String, Object> stormConf, StormTopology topology) {
        double largestMemReq = getMaxExecutorMemoryUsageForTopo(topology, stormConf);
        double topologyWorkerMaxHeapSize =
            ObjectReader.getDouble(stormConf.get(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB), 768.0);
        if (topologyWorkerMaxHeapSize < largestMemReq) {
            throw new IllegalArgumentException(
                "Topology will not be able to be successfully scheduled: Config "
                    + "TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB="
                    + topologyWorkerMaxHeapSize
                    + " < " + largestMemReq + " (Largest memory requirement of a component in the topology)."
                    + " Perhaps set TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB to a larger amount");
        }
    }

    private static double getMaxExecutorMemoryUsageForTopo(
        StormTopology topology, Map<String, Object> topologyConf) {
        double largestMemoryOperator = 0.0;
        for (Map<String, Double> entry :
            ResourceUtils.getBoltsResources(topology, topologyConf).values()) {
            double memoryRequirement =
                entry.getOrDefault(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 0.0)
                    + entry.getOrDefault(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 0.0);
            if (memoryRequirement > largestMemoryOperator) {
                largestMemoryOperator = memoryRequirement;
            }
        }
        for (Map<String, Double> entry :
            ResourceUtils.getSpoutsResources(topology, topologyConf).values()) {
            double memoryRequirement =
                entry.getOrDefault(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 0.0)
                    + entry.getOrDefault(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 0.0);
            if (memoryRequirement > largestMemoryOperator) {
                largestMemoryOperator = memoryRequirement;
            }
        }
        return largestMemoryOperator;
    }
    
    @Override
    public void submitTopologyWithOpts(String topoName, String uploadedJarLocation, String jsonConf, StormTopology topology,
            SubmitOptions options)
            throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, TException {
        try {
            submitTopologyWithOptsCalls.mark();
            assertIsLeader();
            assert(options != null);
            validateTopologyName(topoName);
            checkAuthorization(topoName, null, "submitTopology");
            assertTopoActive(topoName, false);
            @SuppressWarnings("unchecked")
            Map<String, Object> topoConf = (Map<String, Object>) JSONValue.parse(jsonConf);
            try {
                ConfigValidation.validateFields(topoConf);
            } catch (IllegalArgumentException ex) {
                throw new InvalidTopologyException(ex.getMessage());
            }
            validator.validate(topoName, topoConf, topology);
            if ((boolean)conf.getOrDefault(Config.DISABLE_SYMLINKS, false)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> blobMap = (Map<String, Object>) topoConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
                if (blobMap != null && !blobMap.isEmpty()) {
                    throw new InvalidTopologyException("symlinks are disabled so blobs are not supported but " +
                  Config.TOPOLOGY_BLOBSTORE_MAP + " = " + blobMap);
                }
            }
            validateTopologyWorkerMaxHeapSizeConfigs(topoConf, topology);
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
            
            ReqContext req = ReqContext.context();
            Principal principal = req.principal();
            String submitterPrincipal = principal == null ? null : principal.toString();
            String submitterUser = principalToLocal.toLocal(principal);
            String systemUser = System.getProperty("user.name");
            @SuppressWarnings("unchecked")
            Set<String> topoAcl = new HashSet<>((List<String>)topoConf.getOrDefault(Config.TOPOLOGY_USERS, Collections.emptyList()));
            topoAcl.add(submitterPrincipal);
            topoAcl.add(submitterUser);

            String topologyPrincipal = Utils.OR(submitterPrincipal, "");
            topoConf.put(Config.TOPOLOGY_SUBMITTER_PRINCIPAL, topologyPrincipal);
            String topologyOwner = Utils.OR(submitterUser, systemUser);
            topoConf.put(Config.TOPOLOGY_SUBMITTER_USER, topologyOwner); //Don't let the user set who we launch as
            topoConf.put(Config.TOPOLOGY_USERS, new ArrayList<>(topoAcl));
            topoConf.put(Config.STORM_ZOOKEEPER_SUPERACL, conf.get(Config.STORM_ZOOKEEPER_SUPERACL));
            if (!Utils.isZkAuthenticationConfiguredStormServer(conf)) {
                topoConf.remove(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME);
                topoConf.remove(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD);
            }
            if (!(Boolean)conf.getOrDefault(DaemonConfig.STORM_TOPOLOGY_CLASSPATH_BEGINNING_ENABLED, false)) {
                topoConf.remove(Config.TOPOLOGY_CLASSPATH_BEGINNING);
            }
            String topoVersionString = topology.get_storm_version();
            if (topoVersionString == null) {
                topoVersionString = (String)conf.getOrDefault(Config.SUPERVISOR_WORKER_DEFAULT_VERSION, VersionInfo.getVersion());
            }
            //Check if we can run a topology with that version of storm.
            SimpleVersion topoVersion = new SimpleVersion(topoVersionString);
            List<String> cp = Utils.getCompatibleVersion(supervisorClasspaths, topoVersion, "classpath", null);
            if (cp == null) {
                throw new InvalidTopologyException("Topology submitted with storm version " + topoVersionString 
                        + " but could not find a configured compatible version to use " + supervisorClasspaths.keySet());
            }
            Map<String, Object> otherConf = Utils.getConfigFromClasspath(cp, conf);
            Map<String, Object> totalConfToSave = merge(otherConf, topoConf);
            Map<String, Object> totalConf = merge(totalConfToSave, conf);
            //When reading the conf in nimbus we want to fall back to our own settings
            // if the other config does not have it set.
            topology = normalizeTopology(totalConf, topology);
            IStormClusterState state = stormClusterState;
            
            if (creds != null) {
                Map<String, Object> finalConf = Collections.unmodifiableMap(topoConf);
                for (INimbusCredentialPlugin autocred: nimbusAutocredPlugins) {
                    autocred.populateCredentials(creds, finalConf);
                }
            }
            
            if (ObjectReader.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false) &&
                    (submitterUser == null || submitterUser.isEmpty())) {
                throw new AuthorizationException("Could not determine the user to run this topology as.");
            }
            StormCommon.systemTopology(totalConf, topology); //this validates the structure of the topology
            validateTopologySize(topoConf, conf, topology);
            if (Utils.isZkAuthenticationConfiguredStormServer(conf) &&
                    !Utils.isZkAuthenticationConfiguredTopology(topoConf)) {
                throw new IllegalArgumentException("The cluster is configured for zookeeper authentication, but no payload was provided.");
            }
            LOG.info("Received topology submission for {} (storm-{} JDK-{}) with conf {}", topoName, 
                    topoVersionString, topology.get_jdk_version(),
                    Utils.redactValue(topoConf, Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD));
            
            // lock protects against multiple topologies being submitted at once and
            // cleanup thread killing topology in b/w assignment and starting the topology
            synchronized(submitLock) {
                assertTopoActive(topoName, false);
                //cred-update-lock is not needed here because creds are being added for the first time.
                if (creds != null) {
                    state.setCredentials(topoId, new Credentials(creds), topoConf);
                }
                LOG.info("uploadedJar {}", uploadedJarLocation);
                setupStormCode(conf, topoId, uploadedJarLocation, totalConfToSave, topology);
                waitForDesiredCodeReplication(totalConf, topoId);
                state.setupHeatbeats(topoId);
                if (ObjectReader.getBoolean(totalConf.get(Config.TOPOLOGY_BACKPRESSURE_ENABLE), false)) {
                    state.setupBackpressure(topoId);
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
                startTopology(topoName, topoId, status, topologyOwner, topologyPrincipal);
            }
        } catch (Exception e) {
            LOG.warn("Topology submission exception. (topology name='{}')", topoName, e);
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
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
            topoConf = merge(conf, topoConf);
            final String operation = "killTopology";
            checkAuthorization(topoName, topoConf, operation);
            Integer waitAmount = null;
            if (options.is_set_wait_secs()) {
                waitAmount = options.get_wait_secs();
            }
            transitionName(topoName, TopologyActions.KILL, waitAmount, true);
            notifyTopologyActionListener(topoName, operation);
            addTopoToHistoryLog((String)topoConf.get(Config.STORM_ID), topoConf);
        } catch (Exception e) {
            LOG.warn("Kill topology exception. (topology name='{}')", topoName, e);
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void activate(String topoName) throws NotAliveException, AuthorizationException, TException {
        activateCalls.mark();
        try {
            Map<String, Object> topoConf = tryReadTopoConfFromName(topoName);
            topoConf = merge(conf, topoConf);
            final String operation = "activate";
            checkAuthorization(topoName, topoConf, operation);
            transitionName(topoName, TopologyActions.ACTIVATE, null, true);
            notifyTopologyActionListener(topoName, operation);
        } catch (Exception e) {
            LOG.warn("Activate topology exception. (topology name='{}')", topoName, e);
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deactivate(String topoName) throws NotAliveException, AuthorizationException, TException {
        deactivateCalls.mark();
        try {
            Map<String, Object> topoConf = tryReadTopoConfFromName(topoName);
            topoConf = merge(conf, topoConf);
            final String operation = "deactivate";
            checkAuthorization(topoName, topoConf, operation);
            transitionName(topoName, TopologyActions.INACTIVATE, null, true);
            notifyTopologyActionListener(topoName, operation);
        } catch (Exception e) {
            LOG.warn("Deactivate topology exception. (topology name='{}')", topoName, e);
            if (e instanceof TException) {
                throw (TException)e;
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
            topoConf = merge(conf, topoConf);
            final String operation = "rebalance";
            checkAuthorization(topoName, topoConf, operation);
            Map<String, Integer> execOverrides = options.is_set_num_executors() ? options.get_num_executors() : Collections.emptyMap();
            for (Integer value : execOverrides.values()) {
                if (value == null || value <= 0) {
                    throw new InvalidTopologyException("Number of executors must be greater than 0");
                }
            }
            transitionName(topoName, TopologyActions.REBALANCE, options, true);
            notifyTopologyActionListener(topoName, operation);
        } catch (Exception e) {
            LOG.warn("rebalance topology exception. (topology name='{}')", topoName, e);
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setLogConfig(String topoId, LogConfig config) throws TException {
        try {
            setLogConfigCalls.mark();
            Map<String, Object> topoConf = tryReadTopoConf(topoId, topoCache);
            topoConf = merge(conf, topoConf);
            String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);
            checkAuthorization(topoName, topoConf, "setLogConfig");
            IStormClusterState state = stormClusterState;
            LogConfig mergedLogConfig = state.topologyLogConfig(topoId, null);
            if (mergedLogConfig == null) {
                mergedLogConfig = new LogConfig();
            }

            if (mergedLogConfig.is_set_named_logger_level()) {
                Map<String, LogLevel> namedLoggers = mergedLogConfig.get_named_logger_level();
                for (LogLevel level: namedLoggers.values()) {
                    level.set_action(LogLevelAction.UNCHANGED);
                }
            }

            if (config.is_set_named_logger_level()) {
                for (Entry<String, LogLevel> entry: config.get_named_logger_level().entrySet()) {
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
            state.setTopologyLogConfig(topoId, mergedLogConfig);
        } catch (Exception e) {
            LOG.warn("set log config topology exception. (topology id='{}')", topoId, e);
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public LogConfig getLogConfig(String topoId) throws TException {
        try {
            getLogConfigCalls.mark();
            Map<String, Object> topoConf = tryReadTopoConf(topoId, topoCache);
            topoConf = merge(conf, topoConf);
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
                throw (TException)e;
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
            topoConf = merge(conf, topoConf);
            // make sure samplingPct is within bounds.
            double spct = Math.max(Math.min(samplingPercentage, 100.0), 0.0);
            // while disabling we retain the sampling pct.
            checkAuthorization(topoName, topoConf, "debug");
            if (topoId == null) {
                throw new NotAliveException(topoName);
            }
            boolean hasCompId = componentId != null && !componentId.isEmpty();
            
            DebugOptions options = new DebugOptions();
            options.set_enable(enable);
            if (enable) {
                options.set_samplingpct(spct);
            }
            StormBase updates = new StormBase();
            //For backwards compatability
            updates.set_component_executors(Collections.emptyMap());
            String key = hasCompId ? componentId : topoId;
            updates.put_to_component_debug(key, options);
            
            LOG.info("Nimbus setting debug to {} for storm-name '{}' storm-id '{}' sanpling pct '{}'" + 
                    (hasCompId ? " component-id '" + componentId + "'" : ""),
                    enable, topoName, topoId, spct);
            synchronized(submitLock) {
                state.updateStorm(topoId, updates);
            }
        } catch (Exception e) {
            LOG.warn("debug topology exception. (topology name='{}')", topoName, e);
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void setWorkerProfiler(String topoId, ProfileRequest profileRequest) throws TException {
        try {
            setWorkerProfilerCalls.mark();
            Map<String, Object> topoConf = tryReadTopoConf(topoId, topoCache);
            topoConf = merge(conf, topoConf);
            String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);
            checkAuthorization(topoName, topoConf, "setWorkerProfiler");
            IStormClusterState state = stormClusterState;
            state.setWorkerProfileRequest(topoId, profileRequest);
        } catch (Exception e) {
            LOG.warn("set worker profiler topology exception. (topology id='{}')", topoId, e);
            if (e instanceof TException) {
                throw (TException)e;
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
            Map<String, String> nodeToHost = info.assignment.get_node_host();
            Map<List<? extends Number>, List<Object>> exec2hostPort = new HashMap<>();
            for (Entry<List<Long>, NodeInfo> entry: info.assignment.get_executor_node_port().entrySet()) {
                NodeInfo ni = entry.getValue();
                List<Object> hostPort = Arrays.asList(nodeToHost.get(ni.get_node()), ni.get_port_iterator().next().intValue());
                exec2hostPort.put(entry.getKey(), hostPort);
            }
            List<Map<String, Object>> nodeInfos = StatsUtil.extractNodeInfosFromHbForComp(exec2hostPort, info.taskToComponent, false, componentId);
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
                throw (TException)e;
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
                throw new NotAliveException(topoName + " is not alive");
            }
            Map<String, Object> topoConf = tryReadTopoConf(topoId, topoCache);
            topoConf = merge(conf, topoConf);
            if (credentials == null) {
                credentials = new Credentials(Collections.emptyMap());
            }
            checkAuthorization(topoName, topoConf, "uploadNewCredentials");
            synchronized(credUpdateLock) {
                state.setCredentials(topoId, credentials, topoConf);
            }
        } catch (Exception e) {
            LOG.warn("Upload Creds topology exception. (topology name='{}')", topoName, e);
            if (e instanceof TException) {
                throw (TException)e;
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
            LOG.info("Created blob for {}", key);
            return sessionId;
        } catch (Exception e) {
            LOG.warn("begin create blob exception.", e);
            if (e instanceof TException) {
                throw (TException)e;
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
                throw (TException)e;
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
                throw (TException)e;
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
        } catch (Exception e) {
            LOG.warn("finish blob upload exception.", e);
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void cancelBlobUpload(String session) throws AuthorizationException, TException {
        try {
            AtomicOutputStream os = (AtomicOutputStream)blobUploaders.get(session);
            if (os == null) {
                throw new RuntimeException("Blob for session " + session + " does not exist (or timed out)");
            }
            os.cancel();
            LOG.info("Canceled uploading blob for session {}. Closing session.", session);
            blobUploaders.remove(session);
        } catch (Exception e) {
            LOG.warn("finish blob upload exception.", e);
            if (e instanceof TException) {
                throw (TException)e;
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
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setBlobMeta(String key, SettableBlobMeta meta)
            throws AuthorizationException, KeyNotFoundException, TException {
        try {
            blobStore.setBlobMeta(key, meta, getSubject());
        } catch (Exception e) {
            LOG.warn("set blob meta exception.", e);
            if (e instanceof TException) {
                throw (TException)e;
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
                    (int) conf.getOrDefault(Config.STORM_BLOBSTORE_INPUTSTREAM_BUFFER_SIZE_BYTES, 65536)));
            LOG.info("Created download session for {}", key);
            return ret;
        } catch (Exception e) {
            LOG.warn("begin blob download exception.", e);
            if (e instanceof TException) {
                throw (TException)e;
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
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void deleteBlob(String key) throws AuthorizationException, KeyNotFoundException, TException {
        try {
            blobStore.deleteBlob(key, getSubject());
            if (blobStore instanceof LocalFsBlobStore) {
                stormClusterState.removeBlobstoreKey(key);
                stormClusterState.removeKeyVersion(key);
            }
            LOG.info("Deleted blob for key {}", key);
        } catch (Exception e) {
            LOG.warn("delete blob exception.", e);
            if (e instanceof TException) {
                throw (TException)e;
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
                throw (TException)e;
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
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public int updateBlobReplication(String key, int replication)
            throws AuthorizationException, KeyNotFoundException, TException {
        try {
            return blobStore.updateBlobReplication(key, replication, getSubject());
        } catch (Exception e) {
            LOG.warn("update blob replication exception.", e);
            if (e instanceof TException) {
                throw (TException)e;
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
                state.setupBlobstore(key, ni, getVersionForKey(key, ni, conf));
            }
            LOG.debug("Created state in zookeeper {} {} {}", state, store, ni);
        } catch (Exception e) {
            LOG.warn("Exception while creating state in zookeeper - key: " + key, e);
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public String beginFileUpload() throws AuthorizationException, TException {
        try {
            beginFileUploadCalls.mark();
            checkAuthorization(null, null, "fileUpload");
            String fileloc = getInbox() + "/stormjar-" + Utils.uuid() + ".jar";
            uploaders.put(fileloc, Channels.newChannel(new FileOutputStream(fileloc)));
            LOG.info("Uploading file from client to {}", fileloc);
            return fileloc;
        } catch (Exception e) {
            LOG.warn("Begin file upload exception", e);
            if (e instanceof TException) {
                throw (TException)e;
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
                throw (TException)e;
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
        } catch (Exception e) {
            LOG.warn("finish file upload exception.", e);
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public String beginFileDownload(String file) throws AuthorizationException, TException {
        try {
            beginFileDownloadCalls.mark();
            checkAuthorization(null, null, "fileDownload");
            BufferInputStream is = new BufferInputStream(blobStore.getBlob(file, null),
                    ObjectReader.getInt(conf.get(Config.STORM_BLOBSTORE_INPUTSTREAM_BUFFER_SIZE_BYTES), 65536));
            String id = Utils.uuid();
            downloaders.put(id, is);
            return id;
        } catch (Exception e) {
            LOG.warn("begin file download exception.", e);
            if (e instanceof TException) {
                throw (TException)e;
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
                throw (TException)e;
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
                throw (TException)e;
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
            return getTopologyInfoWithOpts(id, options);
        } catch (Exception e) {
            LOG.warn("get topology ino exception. (topology id={})", id, e);
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public TopologyInfo getTopologyInfoWithOpts(String topoId, GetInfoOptions options)
            throws NotAliveException, AuthorizationException, TException {
        try {
            getTopologyInfoWithOptsCalls.mark();
            CommonTopoInfo common = getCommonTopoInfo(topoId, "getTopologyInfo");
            if (common.base == null) {
                throw new NotAliveException(topoId);
            }
            IStormClusterState state = stormClusterState;
            NumErrorsChoice numErrChoice = Utils.OR(options.get_num_err_choice(), NumErrorsChoice.ALL);
            Map<String, List<ErrorInfo>> errors = new HashMap<>();
            for (String component: common.allComponents) {
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
                for (Entry<List<Long>, NodeInfo> entry: common.assignment.get_executor_node_port().entrySet()) {
                    NodeInfo ni = entry.getValue();
                    ExecutorInfo execInfo = toExecInfo(entry.getKey());
                    Map<String, String> nodeToHost = common.assignment.get_node_host();
                    Map<String, Object> heartbeat = common.beats.get(StatsUtil.convertExecutor(entry.getKey()));
                    if (heartbeat == null) {
                        heartbeat = Collections.emptyMap();
                    }
                    ExecutorSummary summ = new ExecutorSummary(execInfo, common.taskToComponent.get(execInfo.get_task_start()),
                            nodeToHost.get(ni.get_node()), ni.get_port_iterator().next().intValue(),
                            (Integer) heartbeat.getOrDefault("uptime", 0));

                    //heartbeats "stats"
                    Map<String, Object> hb = (Map<String, Object>)heartbeat.get("heartbeat");
                    if (hb != null) {
                        Map ex = (Map) hb.get("stats");
                        if (ex != null) {
                            ExecutorStats stats = StatsUtil.thriftifyExecutorStats(ex);
                            summ.set_stats(stats);
                        }
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
            if (resources != null) {
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
        } catch (Exception e) {
            LOG.warn("Get topo info exception. (topology id='{}')", topoId, e);
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public TopologyPageInfo getTopologyPageInfo(String topoId, String window, boolean includeSys)
            throws NotAliveException, AuthorizationException, TException {
        try {
            getTopologyPageInfoCalls.mark();
            CommonTopoInfo common = getCommonTopoInfo(topoId, "getTopologyPageInfo");
            String topoName = common.topoName;
            IStormClusterState state = stormClusterState;
            int launchTimeSecs = common.launchTimeSecs;
            Assignment assignment = common.assignment;
            Map<List<Integer>, Map<String, Object>> beats = common.beats;
            Map<Integer, String> taskToComp = common.taskToComponent;
            StormTopology topology = common.topology;
            Map<String, Object> topoConf = merge(conf, common.topoConf);
            StormBase base = common.base;
            if (base == null) {
                throw new NotAliveException(topoId);
            }
            Map<WorkerSlot, WorkerResources> workerToResources = getWorkerResourcesForTopology(topoId);
            List<WorkerSummary> workerSummaries = null;
            Map<List<Long>, List<Object>> exec2NodePort = new HashMap<>();
            if (assignment != null) {
                Map<List<Long>, NodeInfo> execToNodeInfo = assignment.get_executor_node_port();
                Map<String, String> nodeToHost = assignment.get_node_host();
                for (Entry<List<Long>, NodeInfo> entry: execToNodeInfo.entrySet()) {
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
                        true); //this is the topology page, so we know the user is authorized
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
            
            Map<String, Map<String, Double>> spoutResources = ResourceUtils.getSpoutsResources(topology, topoConf);
            for (Entry<String, ComponentAggregateStats> entry: topoPageInfo.get_id_to_spout_agg_stats().entrySet()) {
                CommonAggregateStats commonStats = entry.getValue().get_common_stats();
                commonStats.set_resources_map(setResourcesDefaultIfNotSet(spoutResources, entry.getKey(), topoConf));
            }
            
            Map<String, Map<String, Double>> boltResources = ResourceUtils.getBoltsResources(topology, topoConf);
            for (Entry<String, ComponentAggregateStats> entry: topoPageInfo.get_id_to_bolt_agg_stats().entrySet()) {
                CommonAggregateStats commonStats = entry.getValue().get_common_stats();
                commonStats.set_resources_map(setResourcesDefaultIfNotSet(boltResources, entry.getKey(), topoConf));
            }
            
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
            if (resources != null) {
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
            }
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
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public SupervisorPageInfo getSupervisorPageInfo(String superId, String host, boolean includeSys)
            throws NotAliveException, AuthorizationException, TException {
        try {
            getSupervisorPageInfoCalls.mark();
            IStormClusterState state = stormClusterState;
            Map<String, SupervisorInfo> superInfos = state.allSupervisorInfo();
            Map<String, List<String>> hostToSuperId = new HashMap<>();
            for (Entry<String, SupervisorInfo> entry: superInfos.entrySet()) {
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
            Map<String, Assignment> topoToAssignment = state.topologyAssignments();
            for (String sid: supervisorIds) {
                SupervisorInfo info = superInfos.get(sid);
                LOG.info("SIDL {} SI: {} ALL: {}", sid, info, superInfos);
                SupervisorSummary supSum = makeSupervisorSummary(sid, info);
                pageInfo.add_to_supervisor_summaries(supSum);
                List<String> superTopologies = topologiesOnSupervisor(topoToAssignment, sid);
                Set<String> userTopologies = filterAuthorized("getTopology", superTopologies);
                for (String topoId: superTopologies) {
                    CommonTopoInfo common = getCommonTopoInfo(topoId, "getSupervisorPageInfo");
                    String topoName = common.topoName;
                    Assignment assignment = common.assignment;
                    Map<List<Integer>, Map<String, Object>> beats = common.beats;
                    Map<Integer, String> taskToComp = common.taskToComponent;
                    Map<List<Long>, List<Object>> exec2NodePort = new HashMap<>();
                    Map<String, String> nodeToHost;
                    if (assignment != null) {
                        Map<List<Long>, NodeInfo> execToNodeInfo = assignment.get_executor_node_port();
                        for (Entry<List<Long>, NodeInfo> entry: execToNodeInfo.entrySet()) {
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
                    for (WorkerSummary workerSummary: StatsUtil.aggWorkerStats(topoId, topoName, taskToComp, beats, 
                            exec2NodePort, nodeToHost, workerResources, includeSys, isAllowed, sid)) {
                        pageInfo.add_to_worker_summaries(workerSummary);
                    }
                }
            }
            return pageInfo;
        } catch (Exception e) {
            LOG.warn("Get super page info exception. (super id='{}')", superId, e);
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public ComponentPageInfo getComponentPageInfo(String topoId, String componentId, String window,
            boolean includeSys) throws NotAliveException, AuthorizationException, TException {
        try {
            getComponentPageInfoCalls.mark();
            CommonTopoInfo info = getCommonTopoInfo(topoId, "getComponentPageInfo");
            if (info.base == null) {
                throw new NotAliveException(topoId);
            }
            StormTopology topology = info.topology;
            Map<String, Object> topoConf = info.topoConf;
            topoConf = merge(conf, topoConf);
            Assignment assignment = info.assignment;
            Map<List<Long>, List<Object>> exec2NodePort = new HashMap<>();
            Map<String, String> nodeToHost;
            Map<List<Long>, List<Object>> exec2HostPort = new HashMap<>();
            if (assignment != null) {
                Map<List<Long>, NodeInfo> execToNodeInfo = assignment.get_executor_node_port();
                nodeToHost = assignment.get_node_host();
                for (Entry<List<Long>, NodeInfo> entry: execToNodeInfo.entrySet()) {
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
                compPageInfo.set_resources_map(setResourcesDefaultIfNotSet(
                        ResourceUtils.getSpoutsResources(topology, topoConf), componentId, topoConf));
            } else { //bolt
                compPageInfo.set_resources_map(setResourcesDefaultIfNotSet(
                        ResourceUtils.getBoltsResources(topology, topoConf), componentId, topoConf));
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
                for (Entry<List<Long>, List<Object>> entry: exec2HostPort.entrySet()) {
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
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getTopologyConf(String id) throws NotAliveException, AuthorizationException, TException {
        try {
            getTopologyConfCalls.mark();
            Map<String, Object> topoConf = tryReadTopoConf(id, topoCache);
            Map<String, Object> checkConf = merge(conf, topoConf);
            String topoName = (String) checkConf.get(Config.TOPOLOGY_NAME);
            checkAuthorization(topoName, checkConf, "getTopologyConf");
            return JSONValue.toJSONString(topoConf);
        } catch (Exception e) {
            LOG.warn("Get topo conf exception. (topology id='{}')", id, e);
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public StormTopology getTopology(String id) throws NotAliveException, AuthorizationException, TException {
        try {
            getTopologyCalls.mark();
            Map<String, Object> topoConf = tryReadTopoConf(id, topoCache);
            topoConf = merge(conf, topoConf);
            String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);
            checkAuthorization(topoName, topoConf, "getTopology");
            return StormCommon.systemTopology(topoConf, tryReadTopology(id, topoCache));
        } catch (Exception e) {
            LOG.warn("Get topology exception. (topology id='{}')", id, e);
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public StormTopology getUserTopology(String id) throws NotAliveException, AuthorizationException, TException {
        try {
            getUserTopologyCalls.mark();
            Map<String, Object> topoConf = tryReadTopoConf(id, topoCache);
            topoConf = merge(conf, topoConf);
            String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);
            checkAuthorization(topoName, topoConf, "getUserTopology");
            return tryReadTopology(id, topoCache);
        } catch (Exception e) {
            LOG.warn("Get user topology exception. (topology id='{}')", id, e);
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public TopologyHistoryInfo getTopologyHistory(String user) throws AuthorizationException, TException {
        try {
            List<String> adminUsers = (List<String>) conf.getOrDefault(Config.NIMBUS_ADMINS, Collections.emptyList());
            IStormClusterState state = stormClusterState;
            List<String> assignedIds = state.assignments(null);
            Set<String> ret = new HashSet<>();
            boolean isAdmin = adminUsers.contains(user);
            for (String topoId: assignedIds) {
                Map<String, Object> topoConf = tryReadTopoConf(topoId, topoCache);
                topoConf = merge(conf, topoConf);
                List<String> groups = ServerConfigUtils.getTopoLogsGroups(topoConf);
                List<String> topoLogUsers = ServerConfigUtils.getTopoLogsUsers(topoConf);
                if (user == null || isAdmin ||
                        isUserPartOf(user, groups) ||
                        topoLogUsers.contains(user)) {
                    ret.add(topoId);
                }
            }
            ret.addAll(readTopologyHistory(user, adminUsers));
            return new TopologyHistoryInfo(new ArrayList<>(ret));
        } catch (Exception e) {
            LOG.warn("Get topology history. (user='{}')", user, e);
            if (e instanceof TException) {
                throw (TException)e;
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
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public NimbusSummary getLeader() throws AuthorizationException, TException {
        getLeaderCalls.mark();
        checkAuthorization(null, null, "getClusterInfo");
        List<NimbusSummary> nimbuses = stormClusterState.nimbuses();
        NimbusInfo leader = leaderElector.getLeader();
        for (NimbusSummary nimbusSummary: nimbuses) {
            if (leader.getHost().equals(nimbusSummary.get_host()) &&
                    leader.getPort() == nimbusSummary.get_port()) {
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
            checkAuthorization(name, null, "getClusterInfo");
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
            Map<String, Assignment> topoIdToAssignments = state.topologyAssignments();
            Map<String, StormBase> topoIdToBases = state.topologyBases();
            Map<String, Object> clusterSchedulerConfig = scheduler.config();

            //put [owner-> StormBase-list] mapping to ownerToBasesMap
            //if this owner (the input parameter) is null, add all the owners with stormbase and guarantees
            //else, add only this owner (the input paramter) to the map
            Map<String, List<StormBase>> ownerToBasesMap = new HashMap<>();

            if (owner == null){
                // add all the owners to the map
                for (StormBase base: topoIdToBases.values()) {
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
                for (String ownerWithGuarantees: ownersWithGuarantees) {
                    if (!ownerToBasesMap.containsKey(ownerWithGuarantees)) {
                        ownerToBasesMap.put(ownerWithGuarantees, new ArrayList<>());
                    }
                }
            } else {
                //only put this owner to the map
                List<StormBase> stormbases = new ArrayList<>();
                for (StormBase base: topoIdToBases.values()) {
                    if (owner.equals(base.get_owner())) {
                        stormbases.add(base);
                    }
                }
                ownerToBasesMap.put(owner, stormbases);
            }

            List<OwnerResourceSummary> ret = new ArrayList<>();

            //for each owner, get resources, configs, and aggregate
            for (Entry<String, List<StormBase>> ownerToBasesEntry: ownerToBasesMap.entrySet()) {
                String theOwner = ownerToBasesEntry.getKey();
                TopologyResources totalResourcesAggregate = new TopologyResources();

                int totalExecutors = 0;
                int totalWorkers = 0;
                int totalTasks = 0;

                for (StormBase base: ownerToBasesEntry.getValue()) {
                    try {
                        String topoId = state.getTopoId(base.get_name())
                                .orElseThrow(() -> new NotAliveException(base.get_name() + " is not alive"));
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
                    if (scheduler instanceof ResourceAwareScheduler) {
                        Map<String, Object> schedulerConfig = (Map) clusterSchedulerConfig.get(theOwner);
                        if (schedulerConfig != null) {
                            ownerResourceSummary.set_memory_guarantee((double)schedulerConfig.getOrDefault("memory", 0));
                            ownerResourceSummary.set_cpu_guarantee((double)schedulerConfig.getOrDefault("cpu", 0));
                            ownerResourceSummary.set_memory_guarantee_remaining(ownerResourceSummary.get_memory_guarantee()
                                    - ownerResourceSummary.get_memory_usage());
                            ownerResourceSummary.set_cpu_guarantee_remaining(ownerResourceSummary.get_cpu_guarantee()
                                    - ownerResourceSummary.get_cpu_usage());
                        }
                    } else if (scheduler instanceof  MultitenantScheduler) {
                        ownerResourceSummary.set_isolated_node_guarantee((int) clusterSchedulerConfig.getOrDefault(theOwner, 0));
                    }
                }

                LOG.debug("{}",ownerResourceSummary.toString());
                ret.add(ownerResourceSummary);
            }

            return ret;
        } catch (Exception e) {
            LOG.warn("Get owner resource summaries exception. (owner = '{}')", owner);
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }
    
    // Shutdownable methods
    
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
            blobStore.shutdown();
            leaderElector.close();
            ITopologyActionNotifierPlugin actionNotifier = nimbusTopologyActionNotifier;
            if (actionNotifier != null) {
                actionNotifier.cleanup();
            }
            LOG.info("Shut down master");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //Daemon common methods
    
    @Override
    public boolean isWaiting() {
        return timer.isTimerWaiting();
    }
}
