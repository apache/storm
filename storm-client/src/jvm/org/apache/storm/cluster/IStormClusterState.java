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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.ClusterWorkerHeartbeat;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.ErrorInfo;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.LogConfig;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.PrivateWorkerKey;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.generated.WorkerTokenServiceType;
import org.apache.storm.nimbus.NimbusInfo;

public interface IStormClusterState {
    List<String> assignments(Runnable callback);

    /**
     * Get the assignment based on storm id from local backend.
     *
     * @param stormId  topology id
     * @param callback callback function
     * @return {@link Assignment}
     */
    Assignment assignmentInfo(String stormId, Runnable callback);

    /**
     * Get the assignment based on storm id from remote state store, eg: ZK.
     *
     * @param stormId  topology id
     * @param callback callback function
     * @return {@link Assignment}
     */
    Assignment remoteAssignmentInfo(String stormId, Runnable callback);

    /**
     * Get all the topologies assignments mapping stormId -> Assignment from local backend.
     *
     * @return stormId -> Assignment mapping
     */
    Map<String, Assignment> assignmentsInfo();

    /**
     * Sync the remote state store assignments to local backend, used when master gains leadership, see {@link LeaderListenerCallback}.
     *
     * @param remote assigned assignments for a specific {@link IStormClusterState} instance, usually a supervisor/node.
     */
    void syncRemoteAssignments(Map<String, byte[]> remote);

    /**
     * Flag to indicate if the assignments synced successfully, see {@link #syncRemoteAssignments(Map)}.
     *
     * @return true if is synced successfully
     */
    boolean isAssignmentsBackendSynchronized();

    /**
     * Flag to indicate if the Pacameker is backend store.
     *
     * @return true if Pacemaker is being used as StateStore
     */
    boolean isPacemakerStateStore();

    /**
     * Mark the assignments as synced successfully, see {@link #isAssignmentsBackendSynchronized()}.
     */
    void setAssignmentsBackendSynchronized();

    VersionedData<Assignment> assignmentInfoWithVersion(String stormId, Runnable callback);

    Integer assignmentVersion(String stormId, Runnable callback) throws Exception;

    List<String> blobstoreInfo(String blobKey);

    List<NimbusSummary> nimbuses();

    void addNimbusHost(String nimbusId, NimbusSummary nimbusSummary);

    List<String> activeStorms();

    /**
     * Get a storm base for a topology.
     *
     * @param stormId  the id of the topology
     * @param callback something to call if the data changes (best effort)
     * @return the StormBase or null if it is not alive.
     */
    StormBase stormBase(String stormId, Runnable callback);

    /**
     * Get storm id from passed name, null if the name doesn't exist on cluster.
     *
     * @param stormName storm name
     * @return storm id
     */
    String stormId(String stormName);

    /**
     * Sync all the active storm ids of the cluster, used now when master gains leadership.
     *
     * @param ids stormName -> stormId mapping
     */
    void syncRemoteIds(Map<String, String> ids);

    ClusterWorkerHeartbeat getWorkerHeartbeat(String stormId, String node, Long port);

    List<ProfileRequest> getWorkerProfileRequests(String stormId, NodeInfo nodeInfo);

    List<ProfileRequest> getTopologyProfileRequests(String stormId);

    void setWorkerProfileRequest(String stormId, ProfileRequest profileRequest);

    void deleteTopologyProfileRequests(String stormId, ProfileRequest profileRequest);

    Map<ExecutorInfo, ExecutorBeat> executorBeats(String stormId, Map<List<Long>, NodeInfo> executorNodePort);

    List<String> supervisors(Runnable callback);

    SupervisorInfo supervisorInfo(String supervisorId); // returns nil if doesn't exist

    void setupHeatbeats(String stormId, Map<String, Object> topoConf);

    void teardownHeartbeats(String stormId);

    void teardownTopologyErrors(String stormId);

    List<String> heartbeatStorms();

    List<String> errorTopologies();

    /**
     * Get backpressure topologies.
     * @deprecated: In Storm 2.0. Retained for enabling transition from 1.x. Will be removed soon.
     */
    @Deprecated
    List<String> backpressureTopologies();

    /**
     * Get leader info from state store, which was written when a master gains leadership.
     *
     * <p>Caution: it can not be used for fencing and is only for informational purposes because we use ZK as our
     * backend now, which could have a overdue info of nodes.
     *
     * @param callback callback func
     * @return {@link NimbusInfo}
     */
    NimbusInfo getLeader(Runnable callback);

    void setTopologyLogConfig(String stormId, LogConfig logConfig, Map<String, Object> topoConf);

    LogConfig topologyLogConfig(String stormId, Runnable cb);

    void workerHeartbeat(String stormId, String node, Long port, ClusterWorkerHeartbeat info);

    void removeWorkerHeartbeat(String stormId, String node, Long port);

    void supervisorHeartbeat(String supervisorId, SupervisorInfo info);

    /**
     * Get topoloy backpressure.
     * @deprecated: In Storm 2.0. Retained for enabling transition from 1.x. Will be removed soon.
     */
    @Deprecated
    boolean topologyBackpressure(String stormId, long timeoutMs, Runnable callback);

    /**
     * Setup backpressure.
     * @deprecated: In Storm 2.0. Retained for enabling transition from 1.x. Will be removed soon.
     */
    @Deprecated
    void setupBackpressure(String stormId, Map<String, Object> topoConf);

    /**
     * Remove backpressure.
     * @deprecated: In Storm 2.0. Retained for enabling transition from 1.x. Will be removed soon.
     */
    @Deprecated
    void removeBackpressure(String stormId);

    /**
     * Remove worker backpressure.
     * @deprecated: In Storm 2.0. Retained for enabling transition from 1.x. Will be removed soon.
     */
    @Deprecated
    void removeWorkerBackpressure(String stormId, String node, Long port);

    void activateStorm(String stormId, StormBase stormBase, Map<String, Object> topoConf);

    void updateStorm(String stormId, StormBase newElems);

    void removeStormBase(String stormId);

    void setAssignment(String stormId, Assignment info, Map<String, Object> topoConf);

    void setupBlob(String key, NimbusInfo nimbusInfo, Integer versionInfo);

    List<String> activeKeys();

    List<String> blobstore(Runnable callback);

    void removeStorm(String stormId);

    void removeBlobstoreKey(String blobKey);

    void removeKeyVersion(String blobKey);

    void reportError(String stormId, String componentId, String node, Long port, Throwable error);

    void setupErrors(String stormId, Map<String, Object> topoConf);

    List<ErrorInfo> errors(String stormId, String componentId);

    ErrorInfo lastError(String stormId, String componentId);

    void setCredentials(String stormId, Credentials creds, Map<String, Object> topoConf);

    Credentials credentials(String stormId, Runnable callback);

    void disconnect();

    /**
     * Get a private key used to validate a token is correct. This is expected to be called from a privileged daemon, and the ACLs should be
     * set up to only allow nimbus and these privileged daemons access to these private keys.
     *
     * @param type       the type of service the key is for.
     * @param topologyId the topology id the key is for.
     * @param keyVersion the version of the key this is for.
     * @return the private key or null if it could not be found.
     */
    PrivateWorkerKey getPrivateWorkerKey(WorkerTokenServiceType type, String topologyId, long keyVersion);

    /**
     * Store a new version of a private key. This is expected to only ever be called from nimbus.  All ACLs however need to be setup to
     * allow the given services access to the stored information.
     *
     * @param type       the type of service this key is for.
     * @param topologyId the topology this key is for
     * @param keyVersion the version of the key this is for.
     * @param key        the key to store.
     */
    void addPrivateWorkerKey(WorkerTokenServiceType type, String topologyId, long keyVersion, PrivateWorkerKey key);

    /**
     * Get the next key version number that should be used for this topology id. This is expected to only ever be called from nimbus, but it
     * is acceptable if the ACLs are setup so that it can work from a privileged daemon for the given service.
     *
     * @param type       the type of service this is for.
     * @param topologyId the topology id this is for.
     * @return the next version number.  It should be 0 for a new topology id/service combination.
     */
    long getNextPrivateWorkerKeyVersion(WorkerTokenServiceType type, String topologyId);

    /**
     * Remove all keys for the given topology that have expired. The number of keys should be small enough that doing an exhaustive scan of
     * them all is acceptable as there is no guarantee that expiration time and version number are related.  This should be for all service
     * types. This is expected to only ever be called from nimbus and some ACLs may be setup so being called from other daemons will cause
     * it to fail.
     *
     * @param topologyId the id of the topology to scan.
     */
    void removeExpiredPrivateWorkerKeys(String topologyId);

    /**
     * Remove all of the worker keys for a given topology.  Used to clean up after a topology finishes. This is expected to only ever be
     * called from nimbus and ideally should only ever work from nimbus.
     *
     * @param topologyId the topology to clean up after.
     */
    void removeAllPrivateWorkerKeys(String topologyId);

    /**
     * Get a list of all topologyIds that currently have private worker keys stored, of any kind. This is expected to only ever be called
     * from nimbus.
     *
     * @return the list of topology ids with any kind of private worker key stored.
     */
    Set<String> idsOfTopologiesWithPrivateWorkerKeys();

    /**
     * Get all of the supervisors with the ID as the key.
     */
    default Map<String, SupervisorInfo> allSupervisorInfo() {
        return allSupervisorInfo(null);
    }

    /**
     * Get all supervisor info.
     * @param callback be alerted if the list of supervisors change
     * @return All of the supervisors with the ID as the key
     */
    default Map<String, SupervisorInfo> allSupervisorInfo(Runnable callback) {
        Map<String, SupervisorInfo> ret = new HashMap<>();
        for (String id : supervisors(callback)) {
            SupervisorInfo supervisorInfo = supervisorInfo(id);
            if (supervisorInfo != null) {
                ret.put(id, supervisorInfo);
            }
        }
        return ret;
    }

    /**
     * Get a topology ID from the name of a topology.
     *
     * @param topologyName the name of the topology to look for
     * @return the id of the topology or null if it is not alive.
     */
    default Optional<String> getTopoId(final String topologyName) {
        return Optional.ofNullable(stormId(topologyName));
    }

    default Map<String, StormBase> topologyBases() {
        Map<String, StormBase> stormBases = new HashMap<>();
        for (String topologyId : activeStorms()) {
            StormBase base = stormBase(topologyId, null);
            if (base != null) { //race condition with delete
                stormBases.put(topologyId, base);
            }
        }
        return stormBases;
    }
}
