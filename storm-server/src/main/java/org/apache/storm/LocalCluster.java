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

package org.apache.storm;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.IStateStorage;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.Acker;
import org.apache.storm.daemon.DaemonCommon;
import org.apache.storm.daemon.Shutdownable;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.daemon.nimbus.Nimbus;
import org.apache.storm.daemon.nimbus.Nimbus.StandaloneINimbus;
import org.apache.storm.daemon.supervisor.ReadClusterState;
import org.apache.storm.daemon.supervisor.StandaloneSupervisor;
import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.executor.LocalExecutor;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.BeginDownloadResult;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ComponentPageInfo;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.GetInfoOptions;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.ListBlobsResult;
import org.apache.storm.generated.LogConfig;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.OwnerResourceSummary;
import org.apache.storm.generated.ProfileAction;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.generated.SupervisorPageInfo;
import org.apache.storm.generated.TopologyHistoryInfo;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologyPageInfo;
import org.apache.storm.generated.Nimbus.Iface;
import org.apache.storm.generated.Nimbus.Processor;
import org.apache.storm.messaging.IContext;
import org.apache.storm.messaging.local.Context;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.ISupervisor;
import org.apache.storm.security.auth.IGroupMappingServiceProvider;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.storm.security.auth.ThriftServer;
import org.apache.storm.task.IBolt;
import org.apache.storm.testing.InProcessZookeeper;
import org.apache.storm.testing.NonRichBoltTracker;
import org.apache.storm.testing.TmpPath;
import org.apache.storm.testing.TrackedTopology;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.RegisteredGlobalState;
import org.apache.storm.utils.StormCommonInstaller;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.apache.thrift.TException;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A stand alone storm cluster that runs inside a single process.
 * It is intended to be used for testing.  Both internal testing for
 * Apache Storm itself and for people building storm topologies.
 * 
 * LocalCluster is an AutoCloseable so if you are using it in tests you can use
 * a try block to be sure it is shut down.
 * 
 * try (LocalCluster cluster = new LocalCluster()) {
 *     // Do some tests
 * }
 * // The cluster has been shut down.
 */
public class LocalCluster implements ILocalClusterTrackedTopologyAware, Iface {
    private static final Logger LOG = LoggerFactory.getLogger(LocalCluster.class);
    
    private static ThriftServer startNimbusDaemon(Map<String, Object> conf, Nimbus nimbus) {
        ThriftServer ret = new ThriftServer(conf, new Processor<>(nimbus), ThriftConnectionType.NIMBUS);
        LOG.info("Starting Nimbus server...");
        new Thread(() -> ret.serve()).start();
        return ret;
    }
    
    /**
     * Simple way to configure a LocalCluster to meet your needs.
     */
    public static class Builder {
        private int supervisors = 2;
        private int portsPerSupervisor = 3;
        private Map<String, Object> daemonConf = new HashMap<>();
        private INimbus inimbus = null;
        private IGroupMappingServiceProvider groupMapper = null;
        private int supervisorSlotPortMin = 1024;
        private boolean nimbusDaemon = false;
        private UnaryOperator<Nimbus> nimbusWrapper = null;
        private BlobStore store = null;
        private IStormClusterState clusterState = null;
        private ILeaderElector leaderElector = null;
        private String trackId = null;
        private boolean simulateTime = false;
        
        /**
         * Set the number of supervisors the cluster should have.
         */
        public Builder withSupervisors(int supervisors) {
            if (supervisors < 0) {
                throw new IllegalArgumentException("supervisors cannot be negative");
            }
            this.supervisors = supervisors;
            return this;
        }
        
        /**
         * Set the number of slots/ports each supervisor should have
         */
        public Builder withPortsPerSupervisor(int portsPerSupervisor) {
            if (portsPerSupervisor < 0) {
                throw new IllegalArgumentException("supervisor ports cannot be negative");
            }
            this.portsPerSupervisor = portsPerSupervisor;
            return this;
        }
        
        /**
         * Set the base config that the daemons should use.
         */
        public Builder withDaemonConf(Map<String, Object> conf) {
            if (conf != null) {
                this.daemonConf = new HashMap<>(conf);
            }
            return this;
        }
        
        /**
         * Add an single key/value config to the daemon conf
         */
        public Builder withDaemonConf(String key, Object value) {
            this.daemonConf.put(key, value);
            return this;
        }
        
        /**
         * Override the INimbus instance that nimbus will use.
         */
        public Builder withINimbus(INimbus inimbus) {
            this.inimbus = inimbus;
            return this;
        }
        
        /**
         * Override the code that maps users to groups for authorization.
         */
        public Builder withGroupMapper(IGroupMappingServiceProvider groupMapper) {
            this.groupMapper = groupMapper;
            return this;
        }
        
        /**
         * When assigning ports to worker slots start at minPort.
         */
        public Builder withSupervisorSlotPortMin(Number minPort) {
            int port = 1024;
            if (minPort == null) {
                LOG.warn("Number is null... {}", minPort);
            } else {
                port = minPort.intValue();
            }
            if (port <= 0) {
                throw new IllegalArgumentException("port must be positive");
            }
            this.supervisorSlotPortMin = port;
            return this;
        }
        
        /**
         * Have the local nimbus actually launch a thrift server.  This is intended to
         * be used mostly for internal storm testing. 
         */
        public Builder withNimbusDaemon() {
            return withNimbusDaemon(true);
        }

        /**
         * If nimbusDaemon is true the local nimbus will launch a thrift server.  This is intended to
         * be used mostly for internal storm testing. 
         */
        public Builder withNimbusDaemon(Boolean nimbusDaemon) {
            if (nimbusDaemon == null) {
                nimbusDaemon = false;
                LOG.warn("nimbusDaemon is null");
            }
            this.nimbusDaemon = nimbusDaemon;
            return this;
        }
        
        /**
         * Turn on simulated time in the cluster.  This allows someone to simulate long periods of
         * time for timeouts etc when testing nimbus/supervisors themselves.  NOTE: that this only
         * works for code that uses the {@link org.apache.storm.utils.Time} class for time management
         * so it will not work in all cases.
         */
        public Builder withSimulatedTime() {
            return withSimulatedTime(true);
        }

        /**
         * Turn on simulated time in the cluster.  This allows someone to simulate long periods of
         * time for timeouts etc when testing nimbus/supervisors themselves.  NOTE: that this only
         * works for code that uses the {@link org.apache.storm.utils.Time} class for time management
         * so it will not work in all cases.
         */
        public Builder withSimulatedTime(boolean simulateTime) {
            this.simulateTime = simulateTime;
            return this;
        }
        
        /**
         * Before nimbus is created/used call nimbusWrapper on it first and use the
         * result instead.  This is intended for internal testing only, and it here to
         * allow a mocking framework to spy on the nimbus class.
         */
        public Builder withNimbusWrapper(UnaryOperator<Nimbus> nimbusWrapper) {
            this.nimbusWrapper = nimbusWrapper;
            return this;
        }
        
        /**
         * Use the following blobstore instead of the one in the config.
         * This is intended mostly for internal testing with Mocks.
         */
        public Builder withBlobStore(BlobStore store) {
            this.store = store;
            return this;
        }
        
        /**
         * Use the following clusterState instead of the one in the config.
         * This is intended mostly for internal testing with Mocks.
         */
        public Builder withClusterState(IStormClusterState clusterState) {
            this.clusterState = clusterState;
            return this;
        }
        
        /**
         * Use the following leaderElector instead of the one in the config.
         * This is intended mostly for internal testing with Mocks.
         */
        public Builder withLeaderElector(ILeaderElector leaderElector) {
            this.leaderElector = leaderElector;
            return this;
        }
        
        /**
         * A tracked cluster can run tracked topologies.
         * See {@link org.apache.storm.testing.TrackedTopology} for more information
         * on tracked topologies.
         * @param trackId an arbitrary unique id that is used to keep track of tracked topologies 
         */
        public Builder withTracked(String trackId) {
            this.trackId = trackId;
            return this;
        }
        
        /**
         * A tracked cluster can run tracked topologies.
         * See {@link org.apache.storm.testing.TrackedTopology} for more information
         * on tracked topologies.
         */
        public Builder withTracked() {
            this.trackId = Utils.uuid();
            return this;
        }
        
        /**
         * @return the LocalCluster
         * @throws Exception on any one of many different errors.
         * This is intended for testing so yes it is ugly and throws Exception...
         */
        public LocalCluster build() throws Exception {
            return new LocalCluster(this);
        }
    }
    
    private static class TrackedStormCommon extends StormCommon {
        private final String id;
        public TrackedStormCommon(String id) {
            this.id = id;
        }
        
        @Override
        public IBolt makeAckerBoltImpl() {
            return new NonRichBoltTracker(new Acker(), id);
        }
    }
    
    private final Nimbus nimbus;
    //This is very private and does not need to be exposed
    private final AtomicInteger portCounter;
    private final Map<String, Object> daemonConf;
    private final List<Supervisor> supervisors;
    private final IStateStorage state;
    private final IStormClusterState clusterState;
    private final List<TmpPath> tmpDirs;
    private final InProcessZookeeper zookeeper;
    private final IContext sharedContext;
    private final ThriftServer thriftServer;
    private final String trackId;
    private final StormCommonInstaller commonInstaller;
    private final SimulatedTime time;
    
    /**
     * Create a default LocalCluster 
     * @throws Exception on any error
     */
    public LocalCluster() throws Exception {
        this(new Builder().withDaemonConf(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true));
    }
    
    /**
     * Create a LocalCluster that connects to an existing Zookeeper instance
     * @param zkHost the host for ZK
     * @param zkPort the port for ZK
     * @throws Exception on any error
     */
    public LocalCluster(String zkHost, Long zkPort) throws Exception {
        this(new Builder().withDaemonConf(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true)
                .withDaemonConf(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(zkHost))
                .withDaemonConf(Config.STORM_ZOOKEEPER_PORT, zkPort));
    }
    
    @SuppressWarnings("deprecation")
    private LocalCluster(Builder builder) throws Exception {
        if (builder.simulateTime) {
            time = new SimulatedTime();
        } else {
            time = null;
        }
        boolean success = false;
        try {
            this.trackId = builder.trackId;
            if (trackId != null) {
                ConcurrentHashMap<String, AtomicInteger> metrics = new ConcurrentHashMap<>();
                metrics.put("spout-emitted", new AtomicInteger(0));
                metrics.put("transferred", new AtomicInteger(0));
                metrics.put("processed", new AtomicInteger(0));
                this.commonInstaller = new StormCommonInstaller(new TrackedStormCommon(this.trackId));
                LOG.warn("Adding tracked metrics for ID {}", this.trackId);
                RegisteredGlobalState.setState(this.trackId, metrics);
                LocalExecutor.setTrackId(this.trackId);
            } else {
                this.commonInstaller = null;
            }
        
            this.tmpDirs = new ArrayList<>();
            this.supervisors = new ArrayList<>();
            TmpPath nimbusTmp = new TmpPath();
            this.tmpDirs.add(nimbusTmp);
            Map<String, Object> conf = ConfigUtils.readStormConfig();
            conf.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, true);
            conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, false);
            conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 50);
            conf.put(Config.STORM_CLUSTER_MODE, "local");
            conf.put(Config.BLOBSTORE_SUPERUSER, System.getProperty("user.name"));
            conf.put(Config.BLOBSTORE_DIR, nimbusTmp.getPath());
        
            InProcessZookeeper zookeeper = null;
            if (!builder.daemonConf.containsKey(Config.STORM_ZOOKEEPER_SERVERS)) {
                zookeeper = new InProcessZookeeper();
                conf.put(Config.STORM_ZOOKEEPER_PORT, zookeeper.getPort());
                conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("localhost"));
            }
            this.zookeeper = zookeeper;
            conf.putAll(builder.daemonConf);
            this.daemonConf = new HashMap<>(conf);
        
            this.portCounter = new AtomicInteger(builder.supervisorSlotPortMin);
            ClusterStateContext cs = new ClusterStateContext();
            this.state = ClusterUtils.mkStateStorage(this.daemonConf, null, null, cs);
            if (builder.clusterState == null) {
                clusterState = ClusterUtils.mkStormClusterState(this.daemonConf, null, cs);
            } else {
                this.clusterState = builder.clusterState;
            }
            //Set it for nimbus only
            conf.put(Config.STORM_LOCAL_DIR, nimbusTmp.getPath());
            Nimbus nimbus = new Nimbus(conf, builder.inimbus == null ? new StandaloneINimbus() : builder.inimbus, 
                this.getClusterState(), null, builder.store, builder.leaderElector, builder.groupMapper);
            if (builder.nimbusWrapper != null) {
                nimbus = builder.nimbusWrapper.apply(nimbus);
            }
            this.nimbus = nimbus;
            this.nimbus.launchServer();
            IContext context = null;
            if (!ObjectReader.getBoolean(this.daemonConf.get(Config.STORM_LOCAL_MODE_ZMQ), false)) {
                context = new Context();
                context.prepare(this.daemonConf);
            }
            this.sharedContext = context;
            this.thriftServer = builder.nimbusDaemon ? startNimbusDaemon(this.daemonConf, this.nimbus) : null;
        
            for (int i = 0; i < builder.supervisors; i++) {
                addSupervisor(builder.portsPerSupervisor, null, null);
            }
        
            //Wait for a leader to be elected (or topology submission can be rejected)
            try {
                long timeoutAfter = System.currentTimeMillis() + 10_000;
                while (!hasLeader()) {
                    if (timeoutAfter > System.currentTimeMillis()) {
                        throw new IllegalStateException("Timed out waiting for nimbus to become the leader");
                    }
                    Thread.sleep(1);
                }
            } catch (Exception e) {
                //Ignore any exceptions we might be doing a test for authentication 
            }
            success = true;
        } finally {
            if (!success) {
                close();
            }
        }
    }
    
    private boolean hasLeader() throws AuthorizationException, TException {
        ClusterSummary summary = getNimbus().getClusterInfo();
        if (summary.is_set_nimbuses()) {
            for (NimbusSummary sum: summary.get_nimbuses()) {
                if (sum.is_isLeader()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @return Nimbus itself so you can interact with it directly, if needed.
     */
    public Nimbus getNimbus() {
        return nimbus;
    }
    
    /**
     * @return the base config for the daemons.
     */
    public Map<String, Object> getDaemonConf() {
        return new HashMap<>(daemonConf);
    }
    
    public static final KillOptions KILL_NOW = new KillOptions();
    static {
        KILL_NOW.set_wait_secs(0);
    }
    
    /**
     * When running a topology locally, for tests etc.  It is helpful to be sure
     * that the topology is dead before the test exits.  This is an AutoCloseable
     * topology that not only gives you access to the compiled StormTopology
     * but also will kill the topology when it closes.
     * 
     * try (LocalTopology testTopo = cluster.submitTopology("testing", ...)) {
     *   // Run Some test
     * }
     * // The topology has been killed
     */
    public class LocalTopology extends StormTopology implements ILocalTopology {
        private static final long serialVersionUID = 6145919776650637748L;
        private final String topoName;
        
        public LocalTopology(String topoName, StormTopology topo) {
            super(topo);
            this.topoName = topoName;
        }

        @Override
        public void close() throws TException {
            killTopologyWithOpts(topoName, KILL_NOW);
        }
    }
    
    @Override
    public LocalTopology submitTopology(String topologyName, Map<String, Object> conf, StormTopology topology)
            throws TException {
        if (!Utils.isValidConf(conf)) {
            throw new IllegalArgumentException("Topology conf is not json-serializable");
        }
        getNimbus().submitTopology(topologyName, null, JSONValue.toJSONString(conf), Utils.addVersions(topology));
        
        ISubmitterHook hook = (ISubmitterHook) Utils.getConfiguredClass(conf, Config.STORM_TOPOLOGY_SUBMISSION_NOTIFIER_PLUGIN);
        if (hook != null) {
            TopologyInfo topologyInfo = Utils.getTopologyInfo(topologyName, null, conf);
            try {
                hook.notify(topologyInfo, conf, topology);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        return new LocalTopology(topologyName, topology);
    }

    @Override
    public LocalTopology submitTopologyWithOpts(String topologyName, Map<String, Object> conf, StormTopology topology, SubmitOptions submitOpts)
            throws TException {
        if (!Utils.isValidConf(conf)) {
            throw new IllegalArgumentException("Topology conf is not json-serializable");
        }
        getNimbus().submitTopologyWithOpts(topologyName, null, JSONValue.toJSONString(conf),  Utils.addVersions(topology), submitOpts);
        return new LocalTopology(topologyName, topology);
    }

    @Override
    public LocalTopology submitTopology(String topologyName, Map<String, Object> conf, TrackedTopology topology)
            throws TException {
        return submitTopology(topologyName, conf, topology.getTopology());
    }

    @Override
    public LocalTopology submitTopologyWithOpts(String topologyName, Map<String, Object> conf, TrackedTopology topology, SubmitOptions submitOpts)
            throws TException {
            return submitTopologyWithOpts(topologyName, conf, topology.getTopology(), submitOpts);
    }
    
    @Override
    public void uploadNewCredentials(String topologyName, Credentials creds) throws TException {
        getNimbus().uploadNewCredentials(topologyName, creds);
    }

    @Override
    public void killTopology(String topologyName) throws TException {
        getNimbus().killTopology(topologyName);
    }

    @Override
    public void killTopologyWithOpts(String name, KillOptions options) throws TException {
        getNimbus().killTopologyWithOpts(name, options);
    }


    @Override
    public void activate(String topologyName) throws TException {
        getNimbus().activate(topologyName);
    }


    @Override
    public void deactivate(String topologyName) throws TException {
        getNimbus().deactivate(topologyName);
    }


    @Override
    public void rebalance(String name, RebalanceOptions options) throws TException {
        getNimbus().rebalance(name, options);
    }

    @Override
    public void shutdown() {
        try {
            close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getTopologyConf(String id) throws TException {
        return getNimbus().getTopologyConf(id);
    }

    @Override
    public StormTopology getTopology(String id) throws TException {
        return getNimbus().getTopology(id);
    }

    @Override
    public ClusterSummary getClusterInfo() throws TException {
        return getNimbus().getClusterInfo();
    }

    @Override
    public TopologyInfo getTopologyInfo(String id) throws TException {
        return getNimbus().getTopologyInfo(id);
    }

    public int getThriftServerPort() {
        return thriftServer.getPort();
    }

    @Override
    public synchronized void close() throws Exception {
        if (nimbus != null) {
            nimbus.shutdown();
        }
        if (thriftServer != null) {
            LOG.info("shutting down thrift server");
            try {
                thriftServer.stop();
            } catch (Exception e) {
                LOG.info("failed to stop thrift", e);
            }
        }
        if (state != null) {
            state.close();
        }
        if (getClusterState() != null) {
            getClusterState().disconnect();
        }
        for (Supervisor s: supervisors) {
            s.shutdownAllWorkers(null, ReadClusterState.THREAD_DUMP_ON_ERROR);
            s.close();
        }
        ProcessSimulator.killAllProcesses();
        if (zookeeper != null) {
            LOG.info("Shutting down in process zookeeper");
            zookeeper.close();
            LOG.info("Done shutting down in process zookeeper");
        }
        
        for (TmpPath p: tmpDirs) {
            p.close();
        }
        
        if (this.trackId != null) {
            LOG.warn("Clearing tracked metrics for ID {}", this.trackId);
            LocalExecutor.clearTrackId();
            RegisteredGlobalState.clearState(this.trackId);
        }
        
        if (this.commonInstaller != null) {
            this.commonInstaller.close();
        }
        
        if (time != null) {
            time.close();
        }
    }
    
    /**
     * Get a specific Supervisor.  This is intended mostly for internal testing.
     * @param id the id of the supervisor
     */
    public synchronized Supervisor getSupervisor(String id) {
        for (Supervisor s: supervisors) {
            if (id.equals(s.getId())) {
                return s;
            }
        }
        return null;
    }
    
    /**
     * Kill a specific supervisor.  This is intended mostly for internal testing.
     * @param id the id of the supervisor
     */
    public synchronized void killSupervisor(String id) {
        for (Iterator<Supervisor> it = supervisors.iterator(); it.hasNext();) {
            Supervisor s = it.next();
            if (id.equals(s.getId())) {
                it.remove();
                s.close();
                //tmpDir will be handled separately
                return;
            }
        }
    }
    
    /**
     * Add another supervisor to the topology.  This is intended mostly for internal testing.
     */
    public Supervisor addSupervisor() throws Exception {
        return addSupervisor(null, null, null);
    }

    /**
     * Add another supervisor to the topology.  This is intended mostly for internal testing.
     * @param ports the number of ports/slots the supervisor should have
     */
    public Supervisor addSupervisor(Number ports) throws Exception {
        return addSupervisor(ports, null, null);
    }
    
    /**
     * Add another supervisor to the topology.  This is intended mostly for internal testing.
     * @param ports the number of ports/slots the supervisor should have
     * @param id the id of the new supervisor, so you can find it later.
     */
    public Supervisor addSupervisor(Number ports, String id) throws Exception {
        return addSupervisor(ports, null, id);
    }
    
    /**
     * Add another supervisor to the topology.  This is intended mostly for internal testing.
     * @param ports the number of ports/slots the supervisor should have
     * @param conf any config values that should be added/over written in the daemon conf of the cluster.
     * @param id the id of the new supervisor, so you can find it later.
     */
    public synchronized Supervisor addSupervisor(Number ports, Map<String, Object> conf, String id) throws Exception {
        if (ports == null) {
            ports = 2;
        }
        TmpPath tmpDir = new TmpPath();
        tmpDirs.add(tmpDir);
        
        List<Integer> portNumbers = new ArrayList<>(ports.intValue());
        for (int i = 0; i < ports.intValue(); i++) {
            portNumbers.add(portCounter.getAndIncrement());
        }
        
        Map<String, Object> superConf = new HashMap<>(daemonConf);
        if (conf != null) {
            superConf.putAll(conf);
        }
        superConf.put(Config.STORM_LOCAL_DIR, tmpDir.getPath());
        superConf.put(DaemonConfig.SUPERVISOR_SLOTS_PORTS, portNumbers);
        
        final String superId = id == null ? Utils.uuid() : id;
        ISupervisor isuper = new StandaloneSupervisor() {
            @Override
            public String generateSupervisorId() {
                return superId;
            }
        };
        if (!ConfigUtils.isLocalMode(superConf)) {
            throw new IllegalArgumentException("Cannot start server in distrubuted mode!");
        }
        
        Supervisor s = new Supervisor(superConf, sharedContext, isuper);
        s.launch();
        supervisors.add(s);
        return s;
    }
    
    private boolean areAllSupervisorsWaiting() {
        boolean ret = true;
        for (Supervisor s: supervisors) {
            ret = ret && s.isWaiting();
        }
        return ret;
    }
    
    private static boolean areAllWorkersWaiting() {
        boolean ret = true;
        for (Shutdownable s: ProcessSimulator.getAllProcessHandles()) {
            if (s instanceof DaemonCommon) {
                ret = ret && ((DaemonCommon)s).isWaiting();
            }
        }
        return ret;
    }
    
    /**
     * Wait for the cluster to be idle.  This is intended to be used with
     * Simulated time and is for internal testing.
     * @throws InterruptedException if interrupted while waiting.
     * @throws AssertionError if the cluster did not come to an idle point with
     * a timeout.
     */
    public void waitForIdle() throws InterruptedException {
        waitForIdle(Testing.TEST_TIMEOUT_MS);
    }
    
    /**
     * Wait for the cluster to be idle.  This is intended to be used with
     * Simulated time and is for internal testing.
     * @param timeoutMs the number of ms to wait before throwing an error.
     * @throws InterruptedException if interrupted while waiting.
     * @throws AssertionError if the cluster did not come to an idle point with
     * a timeout.
     */
    public void waitForIdle(long timeoutMs) throws InterruptedException {
        Random rand = ThreadLocalRandom.current();
        //wait until all workers, supervisors, and nimbus is waiting
        final long endTime = System.currentTimeMillis() + timeoutMs;
        while (!(nimbus.isWaiting() &&
                areAllSupervisorsWaiting() &&
                areAllWorkersWaiting())) {
            if (System.currentTimeMillis() >= endTime) {
                LOG.info("Cluster was not idle in {} ms", timeoutMs);
                LOG.info(Utils.threadDump());
                throw new AssertionError("Test timed out (" + timeoutMs + "ms) cluster not idle");
            }
            Thread.sleep(rand.nextInt(20));
        }
    }
    
    @Override
    public void advanceClusterTime(int secs) throws InterruptedException {
        advanceClusterTime(secs, 1);
    }
    
    @Override
    public void advanceClusterTime(int secs, int incSecs) throws InterruptedException {
        for (int amountLeft = secs; amountLeft > 0; amountLeft -= incSecs) {
            int diff = Math.min(incSecs, amountLeft);
            Time.advanceTimeSecs(diff);
            waitForIdle();
        }
    }

    @Override
    public IStormClusterState getClusterState() {
        return clusterState;
    }
    
    @Override
    public String getTrackedId() {
        return trackId;
    }

    //Nimbus Compatibility
    
    @Override
    public void submitTopology(String name, String uploadedJarLocation, String jsonConf, StormTopology topology)
            throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, TException {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> conf = (Map<String, Object>) JSONValue.parseWithException(jsonConf);
            submitTopology(name, conf, topology);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void submitTopologyWithOpts(String name, String uploadedJarLocation, String jsonConf, StormTopology topology,
            SubmitOptions options)
            throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, TException {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> conf = (Map<String, Object>) JSONValue.parseWithException(jsonConf);
            submitTopologyWithOpts(name, conf, topology, options);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setLogConfig(String name, LogConfig config) throws TException {
        // TODO Auto-generated method stub
        throw new RuntimeException("NOT IMPLEMENTED YET");
    }

    @Override
    public LogConfig getLogConfig(String name) throws TException {
        // TODO Auto-generated method stub
        throw new RuntimeException("NOT IMPLEMENTED YET");
    }

    @Override
    public void debug(String name, String component, boolean enable, double samplingPercentage)
            throws NotAliveException, AuthorizationException, TException {
        // TODO Auto-generated method stub
        throw new RuntimeException("NOT IMPLEMENTED YET");
    }

    @Override
    public void setWorkerProfiler(String id, ProfileRequest profileRequest) throws TException {
        // TODO Auto-generated method stub
        throw new RuntimeException("NOT IMPLEMENTED YET");
    }

    @Override
    public List<ProfileRequest> getComponentPendingProfileActions(String id, String component_id, ProfileAction action)
            throws TException {
        // TODO Auto-generated method stub
        throw new RuntimeException("NOT IMPLEMENTED YET");
    }

    @Override
    public String beginCreateBlob(String key, SettableBlobMeta meta)
            throws AuthorizationException, KeyAlreadyExistsException, TException {
        throw new RuntimeException("BLOBS NOT SUPPORTED IN LOCAL MODE");
    }

    @Override
    public String beginUpdateBlob(String key) throws AuthorizationException, KeyNotFoundException, TException {
        throw new KeyNotFoundException("BLOBS NOT SUPPORTED IN LOCAL MODE");
    }

    @Override
    public void uploadBlobChunk(String session, ByteBuffer chunk) throws AuthorizationException, TException {
        throw new RuntimeException("BLOBS NOT SUPPORTED IN LOCAL MODE");
    }

    @Override
    public void finishBlobUpload(String session) throws AuthorizationException, TException {
        throw new RuntimeException("BLOBS NOT SUPPORTED IN LOCAL MODE");
    }

    @Override
    public void cancelBlobUpload(String session) throws AuthorizationException, TException {
        throw new RuntimeException("BLOBS NOT SUPPORTED IN LOCAL MODE");
    }

    @Override
    public ReadableBlobMeta getBlobMeta(String key) throws AuthorizationException, KeyNotFoundException, TException {
        throw new KeyNotFoundException("BLOBS NOT SUPPORTED IN LOCAL MODE");
    }

    @Override
    public void setBlobMeta(String key, SettableBlobMeta meta)
            throws AuthorizationException, KeyNotFoundException, TException {
        throw new KeyNotFoundException("BLOBS NOT SUPPORTED IN LOCAL MODE");
    }

    @Override
    public BeginDownloadResult beginBlobDownload(String key)
            throws AuthorizationException, KeyNotFoundException, TException {
        throw new KeyNotFoundException("BLOBS NOT SUPPORTED IN LOCAL MODE");
    }

    @Override
    public ByteBuffer downloadBlobChunk(String session) throws AuthorizationException, TException {
        throw new RuntimeException("BLOBS NOT SUPPORTED IN LOCAL MODE");
    }

    @Override
    public void deleteBlob(String key) throws AuthorizationException, KeyNotFoundException, TException {
        throw new KeyNotFoundException("BLOBS NOT SUPPORTED IN LOCAL MODE");
    }

    @Override
    public ListBlobsResult listBlobs(String session) throws TException {
        //Blobs are not supported in local mode.  Return nothing
        ListBlobsResult ret = new ListBlobsResult();
        ret.set_keys(new ArrayList<>());
        return ret;
    }

    @Override
    public int getBlobReplication(String key) throws AuthorizationException, KeyNotFoundException, TException {
        throw new KeyNotFoundException("BLOBS NOT SUPPORTED IN LOCAL MODE");
    }

    @Override
    public int updateBlobReplication(String key, int replication)
            throws AuthorizationException, KeyNotFoundException, TException {
        throw new KeyNotFoundException("BLOBS NOT SUPPORTED IN LOCAL MODE");
    }

    @Override
    public void createStateInZookeeper(String key) throws TException {
        // TODO Auto-generated method stub
        throw new RuntimeException("NOT IMPLEMENTED YET");
    }

    @Override
    public String beginFileUpload() throws AuthorizationException, TException {
        //Just ignore these for now.  We are going to throw it away anyways
        return Utils.uuid();
    }

    @Override
    public void uploadChunk(String location, ByteBuffer chunk) throws AuthorizationException, TException {
        //Just throw it away in local mode
    }

    @Override
    public void finishFileUpload(String location) throws AuthorizationException, TException {
        //Just throw it away in local mode
    }

    @Override
    public String beginFileDownload(String file) throws AuthorizationException, TException {
        throw new AuthorizationException("FILE DOWNLOAD NOT SUPPORTED IN LOCAL MODE");
    }

    @Override
    public ByteBuffer downloadChunk(String id) throws AuthorizationException, TException {
        throw new AuthorizationException("FILE DOWNLOAD NOT SUPPORTED IN LOCAL MODE");
    }

    @Override
    public String getNimbusConf() throws AuthorizationException, TException {
        // TODO Auto-generated method stub
        throw new RuntimeException("NOT IMPLEMENTED YET");
    }

    @Override
    public NimbusSummary getLeader() throws AuthorizationException, TException {
        return nimbus.getLeader();
    }

    @Override
    public boolean isTopologyNameAllowed(String name) throws AuthorizationException, TException {
        return nimbus.isTopologyNameAllowed(name);
    }

    @Override
    public TopologyInfo getTopologyInfoWithOpts(String id, GetInfoOptions options)
            throws NotAliveException, AuthorizationException, TException {
        // TODO Auto-generated method stub
        throw new RuntimeException("NOT IMPLEMENTED YET");
    }

    @Override
    public TopologyPageInfo getTopologyPageInfo(String id, String window, boolean is_include_sys)
            throws NotAliveException, AuthorizationException, TException {
        // TODO Auto-generated method stub
        throw new RuntimeException("NOT IMPLEMENTED YET");
    }

    @Override
    public SupervisorPageInfo getSupervisorPageInfo(String id, String host, boolean is_include_sys)
            throws NotAliveException, AuthorizationException, TException {
        // TODO Auto-generated method stub
        throw new RuntimeException("NOT IMPLEMENTED YET");
    }

    @Override
    public ComponentPageInfo getComponentPageInfo(String topology_id, String component_id, String window,
            boolean is_include_sys) throws NotAliveException, AuthorizationException, TException {
        // TODO Auto-generated method stub
        throw new RuntimeException("NOT IMPLEMENTED YET");
    }

    @Override
    public StormTopology getUserTopology(String id) throws NotAliveException, AuthorizationException, TException {
        // TODO Auto-generated method stub
        throw new RuntimeException("NOT IMPLEMENTED YET");
    }

    @Override
    public TopologyHistoryInfo getTopologyHistory(String user) throws AuthorizationException, TException {
        // TODO Auto-generated method stub
        throw new RuntimeException("NOT IMPLEMENTED YET");
    }
    
    /**
     * Run c with a local mode cluster overriding the NimbusClient and DRPCClient calls.
     * @param c the callable to run in this mode
     * @param ttlSec the number of seconds to let the cluster run after c has completed
     * @return the result of calling C
     * @throws Exception on any Exception.
     */
    public static <T> T withLocalModeOverride(Callable<T> c, long ttlSec) throws Exception {
        LOG.info("\n\n\t\tSTARTING LOCAL MODE CLUSTER\n\n");
        try (LocalCluster local = new LocalCluster();
                NimbusClient.LocalOverride nimbusOverride = new NimbusClient.LocalOverride(local);
                LocalDRPC drpc = new LocalDRPC();
                DRPCClient.LocalOverride drpcOverride = new DRPCClient.LocalOverride(drpc)) {

            T ret = c.call();
            LOG.info("\n\n\t\tRUNNING LOCAL CLUSTER for {} seconds.\n\n", ttlSec);
            Thread.sleep(ttlSec * 1000);
            
            LOG.info("\n\n\t\tSTOPPING LOCAL MODE CLUSTER\n\n");
            return ret;
        }
    }

    @Override
    public List<OwnerResourceSummary> getOwnerResourceSummaries(String owner) throws AuthorizationException, TException {
        // TODO Auto-generated method stub
        throw new RuntimeException("NOT IMPLEMENTED YET");
    }
    
    public static void main(final String [] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("No class was specified to run");
        }
        
        long ttl = 20;
        String ttlString = System.getProperty("storm.local.sleeptime", "20");
        try {
            ttl = Long.valueOf(ttlString);
        } catch (NumberFormatException e) {
            LOG.warn("could not parse the sleep time defaulting to {} seconds", ttl);
        }
        
        withLocalModeOverride(() -> {
            String klass = args[0];
            String [] newArgs = Arrays.copyOfRange(args, 1, args.length); 
            Class<?> c = Class.forName(klass);
            Method main = c.getDeclaredMethod("main", String[].class);
            
            LOG.info("\n\n\t\tRUNNING {} with args {}\n\n", main, Arrays.toString(newArgs));
            main.invoke(null, (Object)newArgs);
            return (Void)null;
        }, ttl);
        
        //Sometimes external things used with testing don't shut down all the way
        System.exit(0);
    }
}
