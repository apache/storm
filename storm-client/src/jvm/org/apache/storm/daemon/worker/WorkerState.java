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

package org.apache.storm.daemon.worker;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.StormTimer;
import org.apache.storm.cluster.IStateStorage;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.cluster.VersionedData;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.daemon.supervisor.AdvancedFSOps;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.DebugOptions;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.grouping.Load;
import org.apache.storm.grouping.LoadMapping;
import org.apache.storm.hooks.BaseWorkerHook;
import org.apache.storm.messaging.ConnectionWithStatus;
import org.apache.storm.messaging.DeserializingConnectionCallback;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.IContext;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.messaging.TransportFactory;
import org.apache.storm.serialization.KryoTupleSerializer;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ThriftTopologyUtils;
import org.apache.storm.utils.TransferDrainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

public class WorkerState {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerState.class);

    final Map<String, Object> conf;
    final IContext mqContext;

    public Map getConf() {
        return conf;
    }

    public IConnection getReceiver() {
        return receiver;
    }

    public String getTopologyId() {
        return topologyId;
    }

    public int getPort() {
        return port;
    }

    public String getWorkerId() {
        return workerId;
    }

    public IStateStorage getStateStorage() {
        return stateStorage;
    }

    public AtomicBoolean getIsTopologyActive() {
        return isTopologyActive;
    }

    public AtomicReference<Map<String, DebugOptions>> getStormComponentToDebug() {
        return stormComponentToDebug;
    }

    public Set<List<Long>> getExecutors() {
        return executors;
    }

    public List<Integer> getTaskIds() {
        return taskIds;
    }

    public Map getTopologyConf() {
        return topologyConf;
    }

    public StormTopology getTopology() {
        return topology;
    }

    public StormTopology getSystemTopology() {
        return systemTopology;
    }

    public Map<Integer, String> getTaskToComponent() {
        return taskToComponent;
    }

    public Map<String, Map<String, Fields>> getComponentToStreamToFields() {
        return componentToStreamToFields;
    }

    public Map<String, List<Integer>> getComponentToSortedTasks() {
        return componentToSortedTasks;
    }

    public Map<String, Long> getBlobToLastKnownVersion() {return blobToLastKnownVersion;}

    public AtomicReference<Map<NodeInfo, IConnection>> getCachedNodeToPortSocket() {
        return cachedNodeToPortSocket;
    }

    public Map<List<Long>, DisruptorQueue> getExecutorReceiveQueueMap() {
        return executorReceiveQueueMap;
    }

    public Runnable getSuicideCallback() {
        return suicideCallback;
    }

    public Utils.UptimeComputer getUptime() {
        return uptime;
    }

    public Map<String, Object> getDefaultSharedResources() {
        return defaultSharedResources;
    }

    public Map<String, Object> getUserSharedResources() {
        return userSharedResources;
    }

    final IConnection receiver;
    final String topologyId;
    final String assignmentId;
    final int port;
    final String workerId;
    final IStateStorage stateStorage;
    final IStormClusterState stormClusterState;

    // when worker bootup, worker will start to setup initial connections to
    // other workers. When all connection is ready, we will enable this flag
    // and spout and bolt will be activated.
    // used in worker only, keep it as atomic
    final AtomicBoolean isWorkerActive;
    final AtomicBoolean isTopologyActive;
    final AtomicReference<Map<String, DebugOptions>> stormComponentToDebug;

    // executors and taskIds running in this worker
    final Set<List<Long>> executors;
    final List<Integer> taskIds;
    final Map<String, Object> topologyConf;
    final StormTopology topology;
    final StormTopology systemTopology;
    final Map<Integer, String> taskToComponent;
    final Map<String, Map<String, Fields>> componentToStreamToFields;
    final Map<String, List<Integer>> componentToSortedTasks;
    final ConcurrentMap<String, Long> blobToLastKnownVersion;
    final ReentrantReadWriteLock endpointSocketLock;
    final AtomicReference<Map<Integer, NodeInfo>> cachedTaskToNodePort;
    final AtomicReference<Map<NodeInfo, IConnection>> cachedNodeToPortSocket;
    final Map<List<Long>, DisruptorQueue> executorReceiveQueueMap;
    // executor id is in form [start_task_id end_task_id]
    // short executor id is start_task_id
    final Map<Integer, DisruptorQueue> shortExecutorReceiveQueueMap;
    final Map<Integer, Integer> taskToShortExecutor;
    final Runnable suicideCallback;
    final Utils.UptimeComputer uptime;
    final Map<String, Object> defaultSharedResources;
    final Map<String, Object> userSharedResources;
    final LoadMapping loadMapping;
    final AtomicReference<Map<String, VersionedData<Assignment>>> assignmentVersions;
    // Whether this worker is going slow
    final AtomicBoolean backpressure = new AtomicBoolean(false);
    // If the transfer queue is backed-up
    final AtomicBoolean transferBackpressure = new AtomicBoolean(false);
    // a trigger for synchronization with executors
    final AtomicBoolean backpressureTrigger = new AtomicBoolean(false);
    // whether the throttle is activated for spouts
    final AtomicBoolean throttleOn = new AtomicBoolean(false);

    public LoadMapping getLoadMapping() {
        return loadMapping;
    }

    public AtomicReference<Map<String, VersionedData<Assignment>>> getAssignmentVersions() {
        return assignmentVersions;
    }

    public AtomicBoolean getBackpressureTrigger() {
        return backpressureTrigger;
    }

    public AtomicBoolean getThrottleOn() {
        return throttleOn;
    }

    public DisruptorQueue getTransferQueue() {
        return transferQueue;
    }

    public StormTimer getUserTimer() {
        return userTimer;
    }

    final DisruptorQueue transferQueue;

    // Timers
    final StormTimer heartbeatTimer = mkHaltingTimer("heartbeat-timer");
    final StormTimer refreshLoadTimer = mkHaltingTimer("refresh-load-timer");
    final StormTimer refreshConnectionsTimer = mkHaltingTimer("refresh-connections-timer");
    final StormTimer refreshCredentialsTimer = mkHaltingTimer("refresh-credentials-timer");
    final StormTimer checkForUpdatedBlobsTimer = mkHaltingTimer("check-for-updated-blobs-timer");
    final StormTimer resetLogLevelsTimer = mkHaltingTimer("reset-log-levels-timer");
    final StormTimer refreshActiveTimer = mkHaltingTimer("refresh-active-timer");
    final StormTimer executorHeartbeatTimer = mkHaltingTimer("executor-heartbeat-timer");
    final StormTimer refreshBackpressureTimer = mkHaltingTimer("refresh-backpressure-timer");
    final StormTimer userTimer = mkHaltingTimer("user-timer");

    // global variables only used internally in class
    private final Set<Integer> outboundTasks;
    private final AtomicLong nextUpdate = new AtomicLong(0);
    private final boolean trySerializeLocal;
    private final TransferDrainer drainer;

    private static final long LOAD_REFRESH_INTERVAL_MS = 5000L;

    public WorkerState(Map<String, Object> conf, IContext mqContext, String topologyId, String assignmentId, int port, String workerId,
        Map<String, Object> topologyConf, IStateStorage stateStorage, IStormClusterState stormClusterState)
        throws IOException, InvalidTopologyException {
        this.executors = new HashSet<>(readWorkerExecutors(stormClusterState, topologyId, assignmentId, port));
        this.transferQueue = new DisruptorQueue("worker-transfer-queue",
            ObjectReader.getInt(topologyConf.get(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE)),
            (long) topologyConf.get(Config.TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT_MILLIS),
            ObjectReader.getInt(topologyConf.get(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE)),
            (long) topologyConf.get(Config.TOPOLOGY_DISRUPTOR_BATCH_TIMEOUT_MILLIS));

        this.conf = conf;
        this.mqContext = (null != mqContext) ? mqContext : TransportFactory.makeContext(topologyConf);
        this.receiver = this.mqContext.bind(topologyId, port);
        this.topologyId = topologyId;
        this.assignmentId = assignmentId;
        this.port = port;
        this.workerId = workerId;
        this.stateStorage = stateStorage;
        this.stormClusterState = stormClusterState;
        this.isWorkerActive = new AtomicBoolean(false);
        this.isTopologyActive = new AtomicBoolean(false);
        this.stormComponentToDebug = new AtomicReference<>();
        this.executorReceiveQueueMap = mkReceiveQueueMap(topologyConf, executors);
        this.shortExecutorReceiveQueueMap = new HashMap<>();
        this.taskIds = new ArrayList<>();
        this.blobToLastKnownVersion = new ConcurrentHashMap<>();
        for (Map.Entry<List<Long>, DisruptorQueue> entry : executorReceiveQueueMap.entrySet()) {
            this.shortExecutorReceiveQueueMap.put(entry.getKey().get(0).intValue(), entry.getValue());
            this.taskIds.addAll(StormCommon.executorIdToTasks(entry.getKey()));
        }
        Collections.sort(taskIds);
        this.topologyConf = topologyConf;
        this.topology = ConfigUtils.readSupervisorTopology(conf, topologyId, AdvancedFSOps.make(conf));
        this.systemTopology = StormCommon.systemTopology(topologyConf, topology);
        this.taskToComponent = StormCommon.stormTaskInfo(topology, topologyConf);
        this.componentToStreamToFields = new HashMap<>();
        for (String c : ThriftTopologyUtils.getComponentIds(systemTopology)) {
            Map<String, Fields> streamToFields = new HashMap<>();
            for (Map.Entry<String, StreamInfo> stream : ThriftTopologyUtils.getComponentCommon(systemTopology, c).get_streams().entrySet()) {
                streamToFields.put(stream.getKey(), new Fields(stream.getValue().get_output_fields()));
            }
            componentToStreamToFields.put(c, streamToFields);
        }
        this.componentToSortedTasks = Utils.reverseMap(taskToComponent);
        this.componentToSortedTasks.values().forEach(Collections::sort);
        this.endpointSocketLock = new ReentrantReadWriteLock();
        this.cachedNodeToPortSocket = new AtomicReference<>(new HashMap<>());
        this.cachedTaskToNodePort = new AtomicReference<>(new HashMap<>());
        this.taskToShortExecutor = new HashMap<>();
        for (List<Long> executor : this.executors) {
            for (Integer task : StormCommon.executorIdToTasks(executor)) {
                taskToShortExecutor.put(task, executor.get(0).intValue());
            }
        }
        this.suicideCallback = Utils.mkSuicideFn();
        this.uptime = Utils.makeUptimeComputer();
        this.defaultSharedResources = makeDefaultResources();
        this.userSharedResources = makeUserResources();
        this.loadMapping = new LoadMapping();
        this.assignmentVersions = new AtomicReference<>(new HashMap<>());
        this.outboundTasks = workerOutboundTasks();
        this.trySerializeLocal = topologyConf.containsKey(Config.TOPOLOGY_TESTING_ALWAYS_TRY_SERIALIZE)
            && (Boolean) topologyConf.get(Config.TOPOLOGY_TESTING_ALWAYS_TRY_SERIALIZE);
        if (trySerializeLocal) {
            LOG.warn("WILL TRY TO SERIALIZE ALL TUPLES (Turn off {} for production", Config.TOPOLOGY_TESTING_ALWAYS_TRY_SERIALIZE);
        }
        this.drainer = new TransferDrainer();
    }

    public void refreshConnections() {
        try {
            refreshConnections(() -> refreshConnectionsTimer.schedule(0, this::refreshConnections));
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public void refreshConnections(Runnable callback) throws Exception {
        Integer version = stormClusterState.assignmentVersion(topologyId, callback);
        version = (null == version) ? 0 : version;
        VersionedData<Assignment> assignmentVersion = assignmentVersions.get().get(topologyId);
        Assignment assignment;
        if (null != assignmentVersion && (assignmentVersion.getVersion() == version)) {
            assignment = assignmentVersion.getData();
        } else {
            VersionedData<Assignment>
                newAssignmentVersion = new VersionedData<>(version,
                stormClusterState.assignmentInfoWithVersion(topologyId, callback).getData());
            assignmentVersions.getAndUpdate(prev -> {
                Map<String, VersionedData<Assignment>> next = new HashMap<>(prev);
                next.put(topologyId, newAssignmentVersion);
                return next;
            });
            assignment = newAssignmentVersion.getData();
        }

        Set<NodeInfo> neededConnections = new HashSet<>();
        Map<Integer, NodeInfo> newTaskToNodePort = new HashMap<>();
        if (null != assignment) {
            Map<Integer, NodeInfo> taskToNodePort = StormCommon.taskToNodeport(assignment.get_executor_node_port());
            for (Map.Entry<Integer, NodeInfo> taskToNodePortEntry : taskToNodePort.entrySet()) {
                Integer task = taskToNodePortEntry.getKey();
                if (outboundTasks.contains(task)) {
                    newTaskToNodePort.put(task, taskToNodePortEntry.getValue());
                    if (!taskIds.contains(task)) {
                        neededConnections.add(taskToNodePortEntry.getValue());
                    }
                }
            }
        }

        Set<NodeInfo> currentConnections = cachedNodeToPortSocket.get().keySet();
        Set<NodeInfo> newConnections = Sets.difference(neededConnections, currentConnections);
        Set<NodeInfo> removeConnections = Sets.difference(currentConnections, neededConnections);

        // Add new connections atomically
        cachedNodeToPortSocket.getAndUpdate(prev -> {
            Map<NodeInfo, IConnection> next = new HashMap<>(prev);
            for (NodeInfo nodeInfo : newConnections) {
                next.put(nodeInfo,
                    mqContext.connect(
                        topologyId,
                        assignment.get_node_host().get(nodeInfo.get_node()),    // Host
                        nodeInfo.get_port().iterator().next().intValue()));     // Port
            }
            return next;
        });


        try {
            endpointSocketLock.writeLock().lock();
            cachedTaskToNodePort.set(newTaskToNodePort);
        } finally {
            endpointSocketLock.writeLock().unlock();
        }

        for (NodeInfo nodeInfo : removeConnections) {
            cachedNodeToPortSocket.get().get(nodeInfo).close();
        }

        // Remove old connections atomically
        cachedNodeToPortSocket.getAndUpdate(prev -> {
            Map<NodeInfo, IConnection> next = new HashMap<>(prev);
            removeConnections.forEach(next::remove);
            return next;
        });

    }

    public void refreshStormActive() {
        refreshStormActive(() -> refreshActiveTimer.schedule(0, this::refreshStormActive));
    }

    public void refreshStormActive(Runnable callback) {
        StormBase base = stormClusterState.stormBase(topologyId, callback);
        isTopologyActive.set(
            (null != base) &&
            (base.get_status() == TopologyStatus.ACTIVE) &&
            (isWorkerActive.get()));
        if (null != base) {
            Map<String, DebugOptions> debugOptionsMap = new HashMap<>(base.get_component_debug());
            for (DebugOptions debugOptions : debugOptionsMap.values()) {
                if (!debugOptions.is_set_samplingpct()) {
                    debugOptions.set_samplingpct(10);
                }
                if (!debugOptions.is_set_enable()) {
                    debugOptions.set_enable(false);
                }
            }
            stormComponentToDebug.set(debugOptionsMap);
            LOG.debug("Events debug options {}", stormComponentToDebug.get());
        }
    }

    public void refreshThrottle() {
        boolean backpressure = stormClusterState.topologyBackpressure(topologyId, this::refreshThrottle);
        this.throttleOn.set(backpressure);
    }

    public void refreshLoad() {
        Set<Integer> remoteTasks = Sets.difference(new HashSet<Integer>(outboundTasks), new HashSet<>(taskIds));
        Long now = System.currentTimeMillis();
        Map<Integer, Double> localLoad = shortExecutorReceiveQueueMap.entrySet().stream().collect(Collectors.toMap(
            (Function<Map.Entry<Integer, DisruptorQueue>, Integer>) Map.Entry::getKey,
            (Function<Map.Entry<Integer, DisruptorQueue>, Double>) entry -> {
                DisruptorQueue.QueueMetrics qMetrics = entry.getValue().getMetrics();
                return ( (double) qMetrics.population()) / qMetrics.capacity();
            }));

        Map<Integer, Load> remoteLoad = new HashMap<>();
        cachedNodeToPortSocket.get().values().stream().forEach(conn -> remoteLoad.putAll(conn.getLoad(remoteTasks)));
        loadMapping.setLocal(localLoad);
        loadMapping.setRemote(remoteLoad);

        if (now > nextUpdate.get()) {
            receiver.sendLoadMetrics(localLoad);
            nextUpdate.set(now + LOAD_REFRESH_INTERVAL_MS);
        }
    }

    /**
     * we will wait all connections to be ready and then activate the spout/bolt
     * when the worker bootup
     */
    public void activateWorkerWhenAllConnectionsReady() {
        int delaySecs = 0;
        int recurSecs = 1;
        refreshActiveTimer.schedule(delaySecs, new Runnable() {
            @Override public void run() {
                if (areAllConnectionsReady()) {
                    LOG.info("All connections are ready for worker {}:{} with id", assignmentId, port, workerId);
                    isWorkerActive.set(Boolean.TRUE);
                } else {
                    refreshActiveTimer.schedule(recurSecs, () -> activateWorkerWhenAllConnectionsReady(), false, 0);
                }
            }
        });
    }

    public void registerCallbacks() {
        LOG.info("Registering IConnectionCallbacks for {}:{}", assignmentId, port);
        receiver.registerRecv(new DeserializingConnectionCallback(topologyConf,
            getWorkerTopologyContext(),
            this::transferLocal));
    }

    public void transferLocal(List<AddressedTuple> tupleBatch) {
        Map<Integer, List<AddressedTuple>> grouped = new HashMap<>();
        for (AddressedTuple tuple : tupleBatch) {
            Integer executor = taskToShortExecutor.get(tuple.dest);
            if (null == executor) {
                LOG.warn("Received invalid messages for unknown tasks. Dropping... ");
                continue;
            }
            List<AddressedTuple> current = grouped.get(executor);
            if (null == current) {
                current = new ArrayList<>();
                grouped.put(executor, current);
            }
            current.add(tuple);
        }

        for (Map.Entry<Integer, List<AddressedTuple>> entry : grouped.entrySet()) {
            DisruptorQueue queue = shortExecutorReceiveQueueMap.get(entry.getKey());
            if (null != queue) {
                queue.publish(entry.getValue());
            } else {
                LOG.warn("Received invalid messages for unknown tasks. Dropping... ");
            }
        }
    }

    public void transfer(KryoTupleSerializer serializer, List<AddressedTuple> tupleBatch) {
        if (trySerializeLocal) {
            assertCanSerialize(serializer, tupleBatch);
        }
        List<AddressedTuple> local = new ArrayList<>();
        Map<Integer, List<TaskMessage>> remoteMap = new HashMap<>();
        for (AddressedTuple addressedTuple : tupleBatch) {
            int destTask = addressedTuple.getDest();
            if (taskIds.contains(destTask)) {
                // Local task
                local.add(addressedTuple);
            } else {
                // Using java objects directly to avoid performance issues in java code
                if (! remoteMap.containsKey(destTask)) {
                    remoteMap.put(destTask, new ArrayList<>());
                }
                remoteMap.get(destTask).add(new TaskMessage(destTask, serializer.serialize(addressedTuple.getTuple())));
            }
        }

        if (!local.isEmpty()) {
            transferLocal(local);
        }
        if (!remoteMap.isEmpty()) {
            transferQueue.publish(remoteMap);
        }
    }

    // TODO: consider having a max batch size besides what disruptor does automagically to prevent latency issues
    public void sendTuplesToRemoteWorker(HashMap<Integer, ArrayList<TaskMessage>> packets, long seqId, boolean batchEnd) {
        drainer.add(packets);
        if (batchEnd) {
            ReentrantReadWriteLock.ReadLock readLock = endpointSocketLock.readLock();
            try {
                readLock.lock();
                drainer.send(cachedTaskToNodePort.get(), cachedNodeToPortSocket.get());
            } finally {
                readLock.unlock();
            }
            drainer.clear();
        }
    }


    private void assertCanSerialize(KryoTupleSerializer serializer, List<AddressedTuple> tuples) {
        // Check that all of the tuples can be serialized by serializing them
        for (AddressedTuple addressedTuple : tuples) {
            serializer.serialize(addressedTuple.getTuple());
        }
    }

    public WorkerTopologyContext getWorkerTopologyContext() {
        try {
            String codeDir = ConfigUtils.supervisorStormResourcesPath(ConfigUtils.supervisorStormDistRoot(conf, topologyId));
            String pidDir = ConfigUtils.workerPidsRoot(conf, topologyId);
            return new WorkerTopologyContext(systemTopology, topologyConf, taskToComponent, componentToSortedTasks,
                componentToStreamToFields, topologyId, codeDir, pidDir, port, taskIds,
                defaultSharedResources,
                userSharedResources);
        } catch (IOException e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public void runWorkerStartHooks() {
        WorkerTopologyContext workerContext = getWorkerTopologyContext();
        for (ByteBuffer hook : topology.get_worker_hooks()) {
            byte[] hookBytes = Utils.toByteArray(hook);
            BaseWorkerHook hookObject = Utils.javaDeserialize(hookBytes, BaseWorkerHook.class);
            hookObject.start(topologyConf, workerContext);

        }
    }

    public void runWorkerShutdownHooks() {
        for (ByteBuffer hook : topology.get_worker_hooks()) {
            byte[] hookBytes = Utils.toByteArray(hook);
            BaseWorkerHook hookObject = Utils.javaDeserialize(hookBytes, BaseWorkerHook.class);
            hookObject.shutdown();

        }
    }

    public void closeResources() {
        LOG.info("Shutting down default resources");
        ((ExecutorService) defaultSharedResources.get(WorkerTopologyContext.SHARED_EXECUTOR)).shutdownNow();
        LOG.info("Shut down default resources");
    }

    public boolean areAllConnectionsReady() {
        return cachedNodeToPortSocket.get().values()
            .stream()
            .map(WorkerState::isConnectionReady)
            .reduce((left, right) -> left && right)
            .orElse(true);
    }

    public static boolean isConnectionReady(IConnection connection) {
        return !(connection instanceof ConnectionWithStatus)
            || ((ConnectionWithStatus) connection).status() == ConnectionWithStatus.Status.Ready;
    }

    private List<List<Long>> readWorkerExecutors(IStormClusterState stormClusterState, String topologyId, String assignmentId,
        int port) {
        LOG.info("Reading assignments");
        List<List<Long>> executorsAssignedToThisWorker = new ArrayList<>();
        executorsAssignedToThisWorker.add(Constants.SYSTEM_EXECUTOR_ID);
        Map<List<Long>, NodeInfo> executorToNodePort =
            stormClusterState.assignmentInfo(topologyId, null).get_executor_node_port();
        for (Map.Entry<List<Long>, NodeInfo> entry : executorToNodePort.entrySet()) {
            NodeInfo nodeInfo = entry.getValue();
            if (nodeInfo.get_node().equals(assignmentId) && nodeInfo.get_port().iterator().next() == port) {
                executorsAssignedToThisWorker.add(entry.getKey());
            }
        }
        return executorsAssignedToThisWorker;
    }

    private Map<List<Long>, DisruptorQueue> mkReceiveQueueMap(Map<String, Object> topologyConf, Set<List<Long>> executors) {
        Map<List<Long>, DisruptorQueue> receiveQueueMap = new HashMap<>();
        for (List<Long> executor : executors) {
            receiveQueueMap.put(executor, new DisruptorQueue("receive-queue",
                ObjectReader.getInt(topologyConf.get(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE)),
                (long) topologyConf.get(Config.TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT_MILLIS),
                ObjectReader.getInt(topologyConf.get(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE)),
                (long) topologyConf.get(Config.TOPOLOGY_DISRUPTOR_BATCH_TIMEOUT_MILLIS)));
        }
        return receiveQueueMap;
    }
    
    private Map<String, Object> makeDefaultResources() {
        int threadPoolSize = ObjectReader.getInt(conf.get(Config.TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE));
        return ImmutableMap.of(WorkerTopologyContext.SHARED_EXECUTOR, Executors.newFixedThreadPool(threadPoolSize));
    }
    
    private Map<String, Object> makeUserResources() {
        /* TODO: need to invoke a hook provided by the topology, giving it a chance to create user resources.
        * this would be part of the initialization hook
        * need to separate workertopologycontext into WorkerContext and WorkerUserContext.
        * actually just do it via interfaces. just need to make sure to hide setResource from tasks
        */
        return new HashMap<>();
    }

    private StormTimer mkHaltingTimer(String name) {
        return new StormTimer(name, (thread, exception) -> {
            LOG.error("Error when processing event", exception);
            Utils.exitProcess(20, "Error when processing an event");
        });
    }

    /**
     *
     * @return seq of task ids that receive messages from this worker
     */
    private Set<Integer> workerOutboundTasks() {
        WorkerTopologyContext context = getWorkerTopologyContext();
        Set<String> components = new HashSet<>();
        for (Integer taskId : taskIds) {
            for (Map<String, Grouping> value : context.getTargets(context.getComponentId(taskId)).values()) {
                components.addAll(value.keySet());
            }
        }

        Set<Integer> outboundTasks = new HashSet<>();

        for (Map.Entry<String, List<Integer>> entry : Utils.reverseMap(taskToComponent).entrySet()) {
            if (components.contains(entry.getKey())) {
                outboundTasks.addAll(entry.getValue());
            }
        }
        return outboundTasks;
    }

    public interface ILocalTransferCallback {
        void transfer(List<AddressedTuple> tupleBatch);
    }
}
