/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.cluster;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.assignments.ILocalAssignmentsBackend;
import org.apache.storm.callback.ZKStateChangedCallback;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.ClusterWorkerHeartbeat;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.DebugOptions;
import org.apache.storm.generated.ErrorInfo;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.LogConfig;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.PrivateWorkerKey;
import org.apache.storm.generated.ProfileAction;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.generated.WorkerTokenServiceType;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.org.apache.curator.framework.state.ConnectionState;
import org.apache.storm.shade.org.apache.zookeeper.KeeperException;
import org.apache.storm.shade.org.apache.zookeeper.Watcher;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormClusterStateImpl implements IStormClusterState {

    private static Logger LOG = LoggerFactory.getLogger(StormClusterStateImpl.class);
    private final List<ACL> defaultAcls;
    private final String stateId;
    private final boolean shouldCloseStateStorageOnDisconnect;
    private final ClusterStateContext context;
    private IStateStorage stateStorage;
    private ILocalAssignmentsBackend assignmentsBackend;
    private ConcurrentHashMap<String, Runnable> assignmentInfoCallback;
    private ConcurrentHashMap<String, Runnable> assignmentInfoWithVersionCallback;
    private ConcurrentHashMap<String, Runnable> assignmentVersionCallback;
    private AtomicReference<Runnable> supervisorsCallback;
    // we want to register a topo directory getChildren callback for all workers of this dir
    private ConcurrentHashMap<String, Runnable> backPressureCallback;
    private AtomicReference<Runnable> leaderInfoCallback;
    private AtomicReference<Runnable> assignmentsCallback;
    private ConcurrentHashMap<String, Runnable> stormBaseCallback;
    private AtomicReference<Runnable> blobstoreCallback;
    private ConcurrentHashMap<String, Runnable> credentialsCallback;
    private ConcurrentHashMap<String, Runnable> logConfigCallback;

    public StormClusterStateImpl(IStateStorage stateStorage, ILocalAssignmentsBackend assignmentsassignmentsBackend,
                                 ClusterStateContext context, boolean shouldCloseStateStorageOnDisconnect) throws Exception {

        this.stateStorage = stateStorage;
        this.shouldCloseStateStorageOnDisconnect = shouldCloseStateStorageOnDisconnect;
        this.defaultAcls = context.getDefaultZkAcls();
        this.context = context;
        this.assignmentsBackend = assignmentsassignmentsBackend;
        assignmentInfoCallback = new ConcurrentHashMap<>();
        assignmentInfoWithVersionCallback = new ConcurrentHashMap<>();
        assignmentVersionCallback = new ConcurrentHashMap<>();
        supervisorsCallback = new AtomicReference<>();
        backPressureCallback = new ConcurrentHashMap<>();
        leaderInfoCallback = new AtomicReference<>();
        assignmentsCallback = new AtomicReference<>();
        stormBaseCallback = new ConcurrentHashMap<>();
        credentialsCallback = new ConcurrentHashMap<>();
        logConfigCallback = new ConcurrentHashMap<>();
        blobstoreCallback = new AtomicReference<>();

        stateId = this.stateStorage.register(new ZKStateChangedCallback() {

            public void changed(Watcher.Event.EventType type, String path) {
                List<String> toks = tokenizePath(path);
                int size = toks.size();
                if (size >= 1) {
                    String root = toks.get(0);
                    if (root.equals(ClusterUtils.ASSIGNMENTS_ROOT)) {
                        if (size == 1) {
                            // set null and get the old value
                            issueCallback(assignmentsCallback);
                        } else {
                            issueMapCallback(assignmentInfoCallback, toks.get(1));
                            issueMapCallback(assignmentVersionCallback, toks.get(1));
                            issueMapCallback(assignmentInfoWithVersionCallback, toks.get(1));
                        }

                    } else if (root.equals(ClusterUtils.SUPERVISORS_ROOT)) {
                        issueCallback(supervisorsCallback);
                    } else if (root.equals(ClusterUtils.BLOBSTORE_ROOT)) {
                        issueCallback(blobstoreCallback);
                    } else if (root.equals(ClusterUtils.STORMS_ROOT) && size > 1) {
                        issueMapCallback(stormBaseCallback, toks.get(1));
                    } else if (root.equals(ClusterUtils.CREDENTIALS_ROOT) && size > 1) {
                        issueMapCallback(credentialsCallback, toks.get(1));
                    } else if (root.equals(ClusterUtils.LOGCONFIG_ROOT) && size > 1) {
                        issueMapCallback(logConfigCallback, toks.get(1));
                    } else if (root.equals(ClusterUtils.BACKPRESSURE_ROOT) && size > 1) {
                        issueMapCallback(backPressureCallback, toks.get(1));
                    } else if (root.equals(ClusterUtils.LEADERINFO_ROOT)) {
                        issueCallback(leaderInfoCallback);
                    } else {
                        LOG.error("{} Unknown callback for subtree {}", new RuntimeException("Unknown callback for this path"), path);
                        Runtime.getRuntime().exit(30);
                    }

                }

                return;
            }

        });

        String[] pathlist = {
            ClusterUtils.ASSIGNMENTS_SUBTREE,
            ClusterUtils.STORMS_SUBTREE,
            ClusterUtils.SUPERVISORS_SUBTREE,
            ClusterUtils.WORKERBEATS_SUBTREE,
            ClusterUtils.ERRORS_SUBTREE,
            ClusterUtils.BLOBSTORE_SUBTREE,
            ClusterUtils.NIMBUSES_SUBTREE,
            ClusterUtils.LOGCONFIG_SUBTREE,
            ClusterUtils.BACKPRESSURE_SUBTREE
        };
        for (String path : pathlist) {
            this.stateStorage.mkdirs(path, defaultAcls);
        }

    }

    private static List<String> tokenizePath(String path) {
        String[] toks = path.split("/");
        java.util.ArrayList<String> rtn = new ArrayList<>();
        for (String str : toks) {
            if (!str.isEmpty()) {
                rtn.add(str);
            }
        }
        return rtn;
    }

    protected void issueCallback(AtomicReference<Runnable> cb) {
        Runnable callback = cb.getAndSet(null);
        if (callback != null) {
            callback.run();
        }
    }

    protected void issueMapCallback(ConcurrentHashMap<String, Runnable> callbackConcurrentHashMap, String key) {
        Runnable callback = callbackConcurrentHashMap.remove(key);
        if (callback != null) {
            callback.run();
        }
    }

    @Override
    public List<String> assignments(Runnable callback) {
        if (callback != null) {
            assignmentsCallback.set(callback);
        }
        return this.assignmentsBackend.assignments();
    }

    @Override
    public Assignment assignmentInfo(String stormId, Runnable callback) {
        if (callback != null) {
            assignmentInfoCallback.put(stormId, callback);
        }
        return this.assignmentsBackend.getAssignment(stormId);
    }

    @Override
    public Assignment remoteAssignmentInfo(String stormId, Runnable callback) {
        if (callback != null) {
            assignmentInfoCallback.put(stormId, callback);
        }
        byte[] serialized = stateStorage.get_data(ClusterUtils.assignmentPath(stormId), callback != null);
        return ClusterUtils.maybeDeserialize(serialized, Assignment.class);
    }

    @Override
    public Map<String, Assignment> assignmentsInfo() {
        return this.assignmentsBackend.assignmentsInfo();
    }

    @Override
    public void syncRemoteAssignments(Map<String, byte[]> remote) {
        if (null != remote) {
            this.assignmentsBackend.syncRemoteAssignments(remote);
        } else {
            Map<String, byte[]> tmp = new HashMap<>();
            List<String> stormIds = this.stateStorage.get_children(ClusterUtils.ASSIGNMENTS_SUBTREE, false);
            for (String stormId : stormIds) {
                byte[] assignment = this.stateStorage.get_data(ClusterUtils.assignmentPath(stormId), false);
                tmp.put(stormId, assignment);
            }
            this.assignmentsBackend.syncRemoteAssignments(tmp);
        }
    }

    @Override
    public boolean isAssignmentsBackendSynchronized() {
        return this.assignmentsBackend.isSynchronized();
    }

    @Override
    public boolean isPacemakerStateStore() {
        if (stateStorage == null) {
            return false;
        }
        return stateStorage instanceof PaceMakerStateStorage;
    }

    @Override
    public void setAssignmentsBackendSynchronized() {
        this.assignmentsBackend.setSynchronized();
    }

    @Override
    public VersionedData<Assignment> assignmentInfoWithVersion(String stormId, Runnable callback) {
        if (callback != null) {
            assignmentInfoWithVersionCallback.put(stormId, callback);
        }
        Assignment assignment = null;
        Integer version = 0;
        VersionedData<byte[]> dataWithVersion = stateStorage.get_data_with_version(ClusterUtils.assignmentPath(stormId), callback != null);
        if (dataWithVersion != null) {
            assignment = ClusterUtils.maybeDeserialize(dataWithVersion.getData(), Assignment.class);
            version = dataWithVersion.getVersion();
        }
        return new VersionedData<Assignment>(version, assignment);
    }

    @Override
    public Integer assignmentVersion(String stormId, Runnable callback) throws Exception {
        if (callback != null) {
            assignmentVersionCallback.put(stormId, callback);
        }
        return stateStorage.get_version(ClusterUtils.assignmentPath(stormId), callback != null);
    }

    // blobstore state
    @Override
    public List<String> blobstoreInfo(String blobKey) {
        String path = ClusterUtils.blobstorePath(blobKey);
        stateStorage.sync_path(path);
        return stateStorage.get_children(path, false);
    }

    @Override
    public List<NimbusSummary> nimbuses() {
        List<NimbusSummary> nimbusSummaries = new ArrayList<>();
        List<String> nimbusIds = stateStorage.get_children(ClusterUtils.NIMBUSES_SUBTREE, false);
        for (String nimbusId : nimbusIds) {
            byte[] serialized = stateStorage.get_data(ClusterUtils.nimbusPath(nimbusId), false);
            // check for null which can exist because of a race condition in which nimbus nodes in zk may have been
            // removed when connections are reconnected after getting children in the above line
            if (serialized != null) {
                NimbusSummary nimbusSummary = ClusterUtils.maybeDeserialize(serialized, NimbusSummary.class);
                nimbusSummaries.add(nimbusSummary);
            }
        }
        return nimbusSummaries;
    }

    @Override
    public void addNimbusHost(final String nimbusId, final NimbusSummary nimbusSummary) {
        // explicit delete for ephmeral node to ensure this session creates the entry.
        stateStorage.delete_node(ClusterUtils.nimbusPath(nimbusId));
        stateStorage.add_listener((curatorFramework, connectionState) -> {
            LOG.info("Connection state listener invoked, zookeeper connection state has changed to {}", connectionState);
            if (connectionState.equals(ConnectionState.RECONNECTED)) {
                LOG.info("Connection state has changed to reconnected so setting nimbuses entry one more time");
                // explicit delete for ephemeral node to ensure this session creates the entry.
                stateStorage.delete_node(ClusterUtils.nimbusPath(nimbusId));
                stateStorage.set_ephemeral_node(ClusterUtils.nimbusPath(nimbusId), Utils.serialize(nimbusSummary), defaultAcls);
            }
        });

        stateStorage.set_ephemeral_node(ClusterUtils.nimbusPath(nimbusId), Utils.serialize(nimbusSummary), defaultAcls);
    }

    @Override
    public List<String> activeStorms() {
        return stateStorage.get_children(ClusterUtils.STORMS_SUBTREE, false);
    }

    @Override
    public StormBase stormBase(String stormId, Runnable callback) {
        if (callback != null) {
            stormBaseCallback.put(stormId, callback);
        }
        return ClusterUtils.maybeDeserialize(stateStorage.get_data(ClusterUtils.stormPath(stormId), callback != null), StormBase.class);
    }

    @Override
    public String stormId(String stormName) {
        return this.assignmentsBackend.getStormId(stormName);
    }

    @Override
    public void syncRemoteIds(Map<String, String> remote) {
        if (null != remote) {
            this.assignmentsBackend.syncRemoteIds(remote);
        } else {
            Map<String, String> tmp = new HashMap<>();
            List<String> activeStorms = activeStorms();
            for (String stormId : activeStorms) {
                tmp.put(stormId, stormBase(stormId, null).get_name());
            }
            this.assignmentsBackend.syncRemoteIds(tmp);
        }
    }

    @Override
    public ClusterWorkerHeartbeat getWorkerHeartbeat(String stormId, String node, Long port) {
        byte[] bytes = stateStorage.get_worker_hb(ClusterUtils.workerbeatPath(stormId, node, port), false);
        return ClusterUtils.maybeDeserialize(bytes, ClusterWorkerHeartbeat.class);

    }

    @Override
    public List<ProfileRequest> getWorkerProfileRequests(String stormId, NodeInfo nodeInfo) {
        List<ProfileRequest> requests = new ArrayList<>();
        List<ProfileRequest> profileRequests = getTopologyProfileRequests(stormId);
        for (ProfileRequest profileRequest : profileRequests) {
            NodeInfo nodeInfo1 = profileRequest.get_nodeInfo();
            if (nodeInfo1.equals(nodeInfo)) {
                requests.add(profileRequest);
            }
        }
        return requests;
    }

    @Override
    public List<ProfileRequest> getTopologyProfileRequests(String stormId) {
        List<ProfileRequest> profileRequests = new ArrayList<>();
        String path = ClusterUtils.profilerConfigPath(stormId);
        if (stateStorage.node_exists(path, false)) {
            List<String> strs = stateStorage.get_children(path, false);
            for (String str : strs) {
                String childPath = path + ClusterUtils.ZK_SEPERATOR + str;
                byte[] raw = stateStorage.get_data(childPath, false);
                ProfileRequest request = ClusterUtils.maybeDeserialize(raw, ProfileRequest.class);
                if (request != null) {
                    profileRequests.add(request);
                }
            }
        }
        return profileRequests;
    }

    @Override
    public void setWorkerProfileRequest(String stormId, ProfileRequest profileRequest) {
        ProfileAction profileAction = profileRequest.get_action();
        String host = profileRequest.get_nodeInfo().get_node();
        Long port = profileRequest.get_nodeInfo().get_port_iterator().next();
        String path = ClusterUtils.profilerConfigPath(stormId, host, port, profileAction);
        stateStorage.set_data(path, Utils.serialize(profileRequest), defaultAcls);
    }

    @Override
    public void deleteTopologyProfileRequests(String stormId, ProfileRequest profileRequest) {
        ProfileAction profileAction = profileRequest.get_action();
        String host = profileRequest.get_nodeInfo().get_node();
        Long port = profileRequest.get_nodeInfo().get_port_iterator().next();
        String path = ClusterUtils.profilerConfigPath(stormId, host, port, profileAction);
        stateStorage.delete_node(path);
    }

    /**
     * need to take executor->node+port in explicitly so that we don't run into a situation where a long dead worker with a skewed clock
     * overrides all the timestamps. By only checking heartbeats with an assigned node+port, and only reading executors from that heartbeat
     * that are actually assigned, we avoid situations like that.
     *
     * @param stormId          topology id
     * @param executorNodePort executor id -> node + port
     * @return mapping of executorInfo -> executor beat
     */
    @Override
    public Map<ExecutorInfo, ExecutorBeat> executorBeats(String stormId, Map<List<Long>, NodeInfo> executorNodePort) {
        Map<ExecutorInfo, ExecutorBeat> executorWhbs = new HashMap<>();

        Map<NodeInfo, List<List<Long>>> nodePortExecutors = Utils.reverseMap(executorNodePort);

        for (Map.Entry<NodeInfo, List<List<Long>>> entry : nodePortExecutors.entrySet()) {

            String node = entry.getKey().get_node();
            Long port = entry.getKey().get_port_iterator().next();
            ClusterWorkerHeartbeat whb = getWorkerHeartbeat(stormId, node, port);
            List<ExecutorInfo> executorInfoList = new ArrayList<>();
            for (List<Long> list : entry.getValue()) {
                executorInfoList.add(new ExecutorInfo(list.get(0).intValue(), list.get(list.size() - 1).intValue()));
            }
            if (whb != null) {
                executorWhbs.putAll(ClusterUtils.convertExecutorBeats(executorInfoList, whb));
            }
        }
        return executorWhbs;
    }

    @Override
    public List<String> supervisors(Runnable callback) {
        if (callback != null) {
            supervisorsCallback.set(callback);
        }
        return stateStorage.get_children(ClusterUtils.SUPERVISORS_SUBTREE, callback != null);
    }

    @Override
    public SupervisorInfo supervisorInfo(String supervisorId) {
        String path = ClusterUtils.supervisorPath(supervisorId);
        return ClusterUtils.maybeDeserialize(stateStorage.get_data(path, false), SupervisorInfo.class);
    }

    @Override
    public void setupHeatbeats(String stormId, Map<String, Object> topoConf) {
        stateStorage.mkdirs(ClusterUtils.WORKERBEATS_SUBTREE, defaultAcls);
        stateStorage.mkdirs(ClusterUtils.workerbeatStormRoot(stormId), ClusterUtils.mkTopoReadWriteAcls(topoConf));
    }

    @Override
    public void teardownHeartbeats(String stormId) {
        try {
            stateStorage.delete_worker_hb(ClusterUtils.workerbeatStormRoot(stormId));
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.class, e)) {
                // do nothing
                LOG.warn("Could not teardown heartbeats for {}.", stormId);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void teardownTopologyErrors(String stormId) {
        try {
            stateStorage.delete_node(ClusterUtils.errorStormRoot(stormId));
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.class, e)) {
                // do nothing
                LOG.warn("Could not teardown errors for {}.", stormId);
            } else {
                throw e;
            }
        }
    }

    @Override
    public NimbusInfo getLeader(Runnable callback) {
        if (null != callback) {
            this.leaderInfoCallback.set(callback);
        }
        return Utils.javaDeserialize(this.stateStorage.get_data(ClusterUtils.LEADERINFO_SUBTREE, callback != null), NimbusInfo.class);
    }

    @Override
    public List<String> backpressureTopologies() {
        return stateStorage.get_children(ClusterUtils.BACKPRESSURE_SUBTREE, false);
    }

    @Override
    public List<String> heartbeatStorms() {
        return stateStorage.get_worker_hb_children(ClusterUtils.WORKERBEATS_SUBTREE, false);
    }

    @Override
    public List<String> errorTopologies() {
        return stateStorage.get_children(ClusterUtils.ERRORS_SUBTREE, false);
    }

    @Override
    public void setTopologyLogConfig(String stormId, LogConfig logConfig, Map<String, Object> topoConf) {
        stateStorage.mkdirs(ClusterUtils.LOGCONFIG_SUBTREE, defaultAcls);
        stateStorage.set_data(ClusterUtils.logConfigPath(stormId), Utils.serialize(logConfig), ClusterUtils.mkTopoReadOnlyAcls(topoConf));
    }

    @Override
    public LogConfig topologyLogConfig(String stormId, Runnable cb) {
        if (cb != null) {
            logConfigCallback.put(stormId, cb);
        }
        String path = ClusterUtils.logConfigPath(stormId);
        return ClusterUtils.maybeDeserialize(stateStorage.get_data(path, cb != null), LogConfig.class);
    }

    @Override
    public void workerHeartbeat(String stormId, String node, Long port, ClusterWorkerHeartbeat info) {
        if (info != null) {
            String path = ClusterUtils.workerbeatPath(stormId, node, port);
            stateStorage.set_worker_hb(path, Utils.serialize(info), defaultAcls);
        }
    }

    @Override
    public void removeWorkerHeartbeat(String stormId, String node, Long port) {
        String path = ClusterUtils.workerbeatPath(stormId, node, port);
        stateStorage.delete_worker_hb(path);
    }

    @Override
    public void supervisorHeartbeat(String supervisorId, SupervisorInfo info) {
        String path = ClusterUtils.supervisorPath(supervisorId);
        stateStorage.set_ephemeral_node(path, Utils.serialize(info), defaultAcls);
    }

    /**
     * Check whether a topology is in throttle-on status or not: if the backpresure/storm-id dir is not empty, this topology has
     * throttle-on, otherwise throttle-off. But if the backpresure/storm-id dir is not empty and has not been updated for more than
     * timeoutMs, we treat it as throttle-off. This will prevent the spouts from getting stuck indefinitely if something wrong happens.
     *
     * @param stormId   The topology Id
     * @param timeoutMs How long until the backpressure znode is invalid.
     * @param callback  The callback function
     * @return True is backpresure/storm-id dir is not empty and at least one of the backpressure znodes has not timed out; false otherwise.
     */
    @Override
    public boolean topologyBackpressure(String stormId, long timeoutMs, Runnable callback) {
        if (callback != null) {
            backPressureCallback.put(stormId, callback);
        }
        String path = ClusterUtils.backpressureStormRoot(stormId);
        long mostRecentTimestamp = 0;
        if (stateStorage.node_exists(path, false)) {
            List<String> children = stateStorage.get_children(path, callback != null);
            mostRecentTimestamp = children.stream()
                                          .map(childPath -> stateStorage.get_data(ClusterUtils.backpressurePath(stormId, childPath), false))
                                          .filter(data -> data != null)
                                          .mapToLong(data -> ByteBuffer.wrap(data).getLong())
                                          .max()
                                          .orElse(0);
        }
        boolean ret = ((System.currentTimeMillis() - mostRecentTimestamp) < timeoutMs);
        LOG.debug("topology backpressure is {}", ret ? "on" : "off");
        return ret;
    }

    @Override
    public void setupBackpressure(String stormId, Map<String, Object> topoConf) {
        stateStorage.mkdirs(ClusterUtils.BACKPRESSURE_SUBTREE, defaultAcls);
        stateStorage.mkdirs(ClusterUtils.backpressureStormRoot(stormId), ClusterUtils.mkTopoReadWriteAcls(topoConf));
    }

    @Override
    public void removeBackpressure(String stormId) {
        try {
            stateStorage.delete_node(ClusterUtils.backpressureStormRoot(stormId));
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.class, e)) {
                // do nothing
                LOG.warn("Could not teardown backpressure node for {}.", stormId);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void removeWorkerBackpressure(String stormId, String node, Long port) {
        String path = ClusterUtils.backpressurePath(stormId, node, port);
        boolean existed = stateStorage.node_exists(path, false);
        if (existed) {
            stateStorage.delete_node(path);
        }
    }

    @Override
    public void activateStorm(String stormId, StormBase stormBase, Map<String, Object> topoConf) {
        String path = ClusterUtils.stormPath(stormId);
        stateStorage.mkdirs(ClusterUtils.STORMS_SUBTREE, defaultAcls);
        stateStorage.set_data(path, Utils.serialize(stormBase), ClusterUtils.mkTopoReadOnlyAcls(topoConf));
        this.assignmentsBackend.keepStormId(stormBase.get_name(), stormId);
    }

    /**
     * To update this function due to APersistentMap/APersistentSet is clojure's structure.
     */
    @Override
    public void updateStorm(String stormId, StormBase newElems) {
        StormBase stormBase = stormBase(stormId, null);
        if (stormBase.get_component_executors() != null) {

            Map<String, Integer> newComponentExecutors = new HashMap<>();
            Map<String, Integer> componentExecutors = newElems.get_component_executors();
            // componentExecutors maybe be APersistentMap, which don't support "put"
            for (Map.Entry<String, Integer> entry : componentExecutors.entrySet()) {
                newComponentExecutors.put(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<String, Integer> entry : stormBase.get_component_executors().entrySet()) {
                if (!componentExecutors.containsKey(entry.getKey())) {
                    newComponentExecutors.put(entry.getKey(), entry.getValue());
                }
            }
            if (newComponentExecutors.size() > 0) {
                newElems.set_component_executors(newComponentExecutors);
            }
        }

        Map<String, DebugOptions> componentDebug = new HashMap<>();
        Map<String, DebugOptions> oldComponentDebug = stormBase.get_component_debug();

        Map<String, DebugOptions> newComponentDebug = newElems.get_component_debug();
        /// oldComponentDebug.keySet()/ newComponentDebug.keySet() maybe be APersistentSet, which don't support addAll
        Set<String> debugOptionsKeys = new HashSet<>();
        debugOptionsKeys.addAll(oldComponentDebug.keySet());
        debugOptionsKeys.addAll(newComponentDebug.keySet());
        for (String key : debugOptionsKeys) {
            boolean enable = false;
            double samplingpct = 0;
            if (oldComponentDebug.containsKey(key)) {
                enable = oldComponentDebug.get(key).is_enable();
                samplingpct = oldComponentDebug.get(key).get_samplingpct();
            }
            if (newComponentDebug.containsKey(key)) {
                enable = newComponentDebug.get(key).is_enable();
                samplingpct += newComponentDebug.get(key).get_samplingpct();
            }
            DebugOptions debugOptions = new DebugOptions();
            debugOptions.set_enable(enable);
            debugOptions.set_samplingpct(samplingpct);
            componentDebug.put(key, debugOptions);
        }
        if (componentDebug.size() > 0) {
            newElems.set_component_debug(componentDebug);
        }

        if (StringUtils.isBlank(newElems.get_name())) {
            newElems.set_name(stormBase.get_name());
        }

        if (StringUtils.isBlank(newElems.get_topology_version()) && stormBase.is_set_topology_version()) {
            newElems.set_topology_version(stormBase.get_topology_version());
        }

        if (newElems.get_status() == null) {
            newElems.set_status(stormBase.get_status());
        }
        if (newElems.get_num_workers() == 0) {
            newElems.set_num_workers(stormBase.get_num_workers());
        }
        if (newElems.get_launch_time_secs() == 0) {
            newElems.set_launch_time_secs(stormBase.get_launch_time_secs());
        }
        if (StringUtils.isBlank(newElems.get_owner())) {
            newElems.set_owner(stormBase.get_owner());
        }
        if (StringUtils.isBlank(newElems.get_principal()) && stormBase.is_set_principal()) {
            newElems.set_principal(stormBase.get_principal());
        }

        if (newElems.get_topology_action_options() == null) {
            newElems.set_topology_action_options(stormBase.get_topology_action_options());
        }
        if (newElems.get_status() == null) {
            newElems.set_status(stormBase.get_status());
        }
        stateStorage.set_data(ClusterUtils.stormPath(stormId), Utils.serialize(newElems), defaultAcls);
    }

    @Override
    public void removeStormBase(String stormId) {
        stateStorage.delete_node(ClusterUtils.stormPath(stormId));
    }

    @Override
    public void setAssignment(String stormId, Assignment info, Map<String, Object> topoConf) {
        byte[] serAssignment = Utils.serialize(info);
        stateStorage.mkdirs(ClusterUtils.ASSIGNMENTS_SUBTREE, defaultAcls);
        stateStorage.set_data(ClusterUtils.assignmentPath(stormId), Utils.serialize(info), ClusterUtils.mkTopoReadOnlyAcls(topoConf));
        this.assignmentsBackend.keepOrUpdateAssignment(stormId, info);
    }

    @Override
    public void setupBlob(String key, NimbusInfo nimbusInfo, Integer versionInfo) {
        String path = ClusterUtils.blobstorePath(key) + ClusterUtils.ZK_SEPERATOR + nimbusInfo.toHostPortString() + "-" + versionInfo;
        LOG.info("set-path: {}", path);
        stateStorage.mkdirs(ClusterUtils.blobstorePath(key), defaultAcls);
        stateStorage.delete_node_blobstore(ClusterUtils.blobstorePath(key), nimbusInfo.toHostPortString());
        stateStorage.set_ephemeral_node(path, null, defaultAcls);
    }

    @Override
    public List<String> activeKeys() {
        return stateStorage.get_children(ClusterUtils.BLOBSTORE_SUBTREE, false);
    }

    // blobstore state
    @Override
    public List<String> blobstore(Runnable callback) {
        if (callback != null) {
            blobstoreCallback.set(callback);
        }
        stateStorage.sync_path(ClusterUtils.BLOBSTORE_SUBTREE);
        return stateStorage.get_children(ClusterUtils.BLOBSTORE_SUBTREE, callback != null);

    }

    @Override
    public void removeStorm(String stormId) {
        stateStorage.delete_node(ClusterUtils.assignmentPath(stormId));
        this.assignmentsBackend.clearStateForStorm(stormId);
        stateStorage.delete_node(ClusterUtils.credentialsPath(stormId));
        stateStorage.delete_node(ClusterUtils.logConfigPath(stormId));
        stateStorage.delete_node(ClusterUtils.profilerConfigPath(stormId));
        removeStormBase(stormId);
    }

    @Override
    public void removeBlobstoreKey(String blobKey) {
        LOG.debug("remove key {}", blobKey);
        stateStorage.delete_node(ClusterUtils.blobstorePath(blobKey));
    }

    @Override
    public void removeKeyVersion(String blobKey) {
        stateStorage.delete_node(ClusterUtils.blobstoreMaxKeySequenceNumberPath(blobKey));
    }

    @Override
    public void setupErrors(String stormId, Map<String, Object> topoConf) {
        stateStorage.mkdirs(ClusterUtils.ERRORS_SUBTREE, defaultAcls);
        stateStorage.mkdirs(ClusterUtils.errorStormRoot(stormId), ClusterUtils.mkTopoReadWriteAcls(topoConf));
    }

    @Override
    public void reportError(String stormId, String componentId, String node, Long port, Throwable error) {
        String path = ClusterUtils.errorPath(stormId, componentId);
        ErrorInfo errorInfo = new ErrorInfo(ClusterUtils.stringifyError(error), Time.currentTimeSecs());
        errorInfo.set_host(node);
        errorInfo.set_port(port.intValue());
        byte[] serData = Utils.serialize(errorInfo);
        stateStorage.mkdirs(path, defaultAcls);
        stateStorage.create_sequential(path + ClusterUtils.ZK_SEPERATOR + "e", serData, defaultAcls);
        String lastErrorPath = ClusterUtils.lastErrorPath(stormId, componentId);
        stateStorage.set_data(lastErrorPath, serData, defaultAcls);
        List<String> childrens = stateStorage.get_children(path, false);

        Collections.sort(childrens, new Comparator<String>() {
            public int compare(String arg0, String arg1) {
                return Long.compare(Long.parseLong(arg0.substring(1)), Long.parseLong(arg1.substring(1)));
            }
        });

        while (childrens.size() > 10) {
            String znodePath = path + ClusterUtils.ZK_SEPERATOR + childrens.remove(0);
            try {
                stateStorage.delete_node(znodePath);
            } catch (Exception e) {
                if (Utils.exceptionCauseIsInstanceOf(KeeperException.NoNodeException.class, e)) {
                    // if the node is already deleted, do nothing
                    LOG.warn("Could not find the znode: {}", znodePath);
                } else {
                    throw e;
                }
            }
        }
    }

    @Override
    public List<ErrorInfo> errors(String stormId, String componentId) {
        List<ErrorInfo> errorInfos = new ArrayList<>();
        String path = ClusterUtils.errorPath(stormId, componentId);
        if (stateStorage.node_exists(path, false)) {
            List<String> childrens = stateStorage.get_children(path, false);
            for (String child : childrens) {
                String childPath = path + ClusterUtils.ZK_SEPERATOR + child;
                ErrorInfo errorInfo = ClusterUtils.maybeDeserialize(stateStorage.get_data(childPath, false), ErrorInfo.class);
                if (errorInfo != null) {
                    errorInfos.add(errorInfo);
                }
            }
        }
        Collections.sort(errorInfos, new Comparator<ErrorInfo>() {
            public int compare(ErrorInfo arg0, ErrorInfo arg1) {
                return Integer.compare(arg1.get_error_time_secs(), arg0.get_error_time_secs());
            }
        });

        return errorInfos;
    }

    @Override
    public ErrorInfo lastError(String stormId, String componentId) {
        String path = ClusterUtils.lastErrorPath(stormId, componentId);
        if (stateStorage.node_exists(path, false)) {
            ErrorInfo errorInfo = ClusterUtils.maybeDeserialize(stateStorage.get_data(path, false), ErrorInfo.class);
            return errorInfo;
        }

        return null;
    }

    @Override
    public void setCredentials(String stormId, Credentials creds, Map<String, Object> topoConf) {
        List<ACL> aclList = ClusterUtils.mkTopoReadOnlyAcls(topoConf);
        String path = ClusterUtils.credentialsPath(stormId);
        stateStorage.set_data(path, Utils.serialize(creds), aclList);
    }

    @Override
    public Credentials credentials(String stormId, Runnable callback) {
        if (callback != null) {
            credentialsCallback.put(stormId, callback);
        }
        String path = ClusterUtils.credentialsPath(stormId);
        return ClusterUtils.maybeDeserialize(stateStorage.get_data(path, callback != null), Credentials.class);

    }

    @Override
    public void disconnect() {
        stateStorage.unregister(stateId);
        if (shouldCloseStateStorageOnDisconnect) {
            stateStorage.close();
            this.assignmentsBackend.close();
        }
    }

    @Override
    public PrivateWorkerKey getPrivateWorkerKey(WorkerTokenServiceType type, String topologyId, long keyVersion) {
        String path = ClusterUtils.secretKeysPath(type, topologyId, keyVersion);
        byte[] data = stateStorage.get_data(path, false);
        if (data == null) {
            LOG.debug("Could not find entry at {} will sync to see if that fixes it", path);
            //We didn't find it, but there are races, so we want to check again after a sync
            stateStorage.sync_path(path);
            data = stateStorage.get_data(path, false);
        }
        return ClusterUtils.maybeDeserialize(data, PrivateWorkerKey.class);
    }

    @Override
    public void addPrivateWorkerKey(WorkerTokenServiceType type, String topologyId, long keyVersion, PrivateWorkerKey key) {
        assert context.getDaemonType() == DaemonType.NIMBUS;
        stateStorage.mkdirs(ClusterUtils.SECRET_KEYS_SUBTREE, defaultAcls);
        List<ACL> secretAcls = context.getZkSecretAcls(type);
        String path = ClusterUtils.secretKeysPath(type, topologyId, keyVersion);
        LOG.info("Storing private key for {} connecting to a {} at {} with ACL {}", topologyId, type, path, secretAcls);
        stateStorage.set_data(path, Utils.serialize(key), secretAcls);
    }

    @Override
    public long getNextPrivateWorkerKeyVersion(WorkerTokenServiceType type, String topologyId) {
        String path = ClusterUtils.secretKeysPath(type, topologyId);
        try {
            List<String> versions = stateStorage.get_children(path, false);
            return versions.stream().mapToLong(Long::valueOf).max().orElse(0);
        } catch (RuntimeException e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.NoNodeException.class, e)) {
                //If the node does not exist, then the version must be 0
                return 0;
            }
            throw e;
        }
    }

    @Override
    public void removeExpiredPrivateWorkerKeys(String topologyId) {
        for (WorkerTokenServiceType type : WorkerTokenServiceType.values()) {
            String basePath = ClusterUtils.secretKeysPath(type, topologyId);
            try {
                for (String version : stateStorage.get_children(basePath, false)) {
                    String fullPath = basePath + ClusterUtils.ZK_SEPERATOR + version;
                    try {
                        PrivateWorkerKey key =
                            ClusterUtils.maybeDeserialize(stateStorage.get_data(fullPath, false), PrivateWorkerKey.class);
                        if (Time.currentTimeMillis() > key.get_expirationTimeMillis()) {
                            LOG.info("Removing expired worker key {}", fullPath);
                            stateStorage.delete_node(fullPath);
                        }
                    } catch (RuntimeException e) {
                        //This should never happen because only the primary nimbus is active, but just in case
                        // declare the race safe, even if we lose it.
                        if (!Utils.exceptionCauseIsInstanceOf(KeeperException.NoNodeException.class, e)) {
                            throw e;
                        }
                    }
                }
            } catch (RuntimeException e) {
                //No node for basePath is OK, nothing to remove
                if (!Utils.exceptionCauseIsInstanceOf(KeeperException.NoNodeException.class, e)) {
                    throw e;
                }
            }
        }
    }

    @Override
    public void removeAllPrivateWorkerKeys(String topologyId) {
        for (WorkerTokenServiceType type : WorkerTokenServiceType.values()) {
            String path = ClusterUtils.secretKeysPath(type, topologyId);
            try {
                LOG.info("Removing worker keys under {}", path);
                stateStorage.delete_node(path);
            } catch (RuntimeException e) {
                //This should never happen because only the primary nimbus is active, but just in case
                // declare the race safe, even if we lose it.
                if (!Utils.exceptionCauseIsInstanceOf(KeeperException.NoNodeException.class, e)) {
                    throw e;
                }
            }
        }
    }

    @Override
    public Set<String> idsOfTopologiesWithPrivateWorkerKeys() {
        HashSet<String> ret = new HashSet<>();
        for (WorkerTokenServiceType type : WorkerTokenServiceType.values()) {
            String path = ClusterUtils.secretKeysPath(type);
            try {
                ret.addAll(stateStorage.get_children(path, false));
            } catch (RuntimeException e) {
                //If the node does not exist it is fine/expected...
                if (!Utils.exceptionCauseIsInstanceOf(KeeperException.NoNodeException.class, e)) {
                    throw e;
                }
            }
        }
        return ret;
    }
}
