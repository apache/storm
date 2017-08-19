/**
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

package org.apache.storm.daemon.supervisor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.storm.DaemonConfig;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.cluster.VersionedData;
import org.apache.storm.daemon.supervisor.Slot.MachineState;
import org.apache.storm.daemon.supervisor.Slot.TopoProfileAction;
import org.apache.storm.event.EventManager;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.localizer.ILocalizer;
import org.apache.storm.scheduler.ISupervisor;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadClusterState implements Runnable, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ReadClusterState.class);
    
    private final Map<String, Object> superConf;
    private final IStormClusterState stormClusterState;
    private final EventManager syncSupEventManager;
    private final AtomicReference<Map<String, VersionedData<Assignment>>> assignmentVersions;
    private final Map<Integer, Slot> slots = new HashMap<>();
    private final AtomicInteger readRetry = new AtomicInteger(0);
    private final String assignmentId;
    private final ISupervisor iSuper;
    private final ILocalizer localizer;
    private final ContainerLauncher launcher;
    private final String host;
    private final LocalState localState;
    private final AtomicReference<Map<Long, LocalAssignment>> cachedAssignments;
    
    public ReadClusterState(Supervisor supervisor) throws Exception {
        this.superConf = supervisor.getConf();
        this.stormClusterState = supervisor.getStormClusterState();
        this.syncSupEventManager = supervisor.getEventManger();
        this.assignmentVersions = new AtomicReference<>(new HashMap<>());
        this.assignmentId = supervisor.getAssignmentId();
        this.iSuper = supervisor.getiSupervisor();
        this.localizer = supervisor.getAsyncLocalizer();
        this.host = supervisor.getHostName();
        this.localState = supervisor.getLocalState();
        this.cachedAssignments = supervisor.getCurrAssignment();
        
        this.launcher = ContainerLauncher.make(superConf, assignmentId, supervisor.getSharedContext());
        
        @SuppressWarnings("unchecked")
        List<Number> ports = (List<Number>)superConf.get(DaemonConfig.SUPERVISOR_SLOTS_PORTS);
        for (Number port: ports) {
            slots.put(port.intValue(), mkSlot(port.intValue()));
        }
        
        try {
            Collection<String> workers = SupervisorUtils.supervisorWorkerIds(superConf);
            for (Slot slot: slots.values()) {
                String workerId = slot.getWorkerId();
                if (workerId != null) {
                    workers.remove(workerId);
                }
            }
            if (!workers.isEmpty()) {
                supervisor.killWorkers(workers, launcher);
            }
        } catch (Exception e) {
            LOG.warn("Error trying to clean up old workers", e);
        }

        //All the slots/assignments should be recovered now, so we can clean up anything that we don't expect to be here
        try {
            localizer.cleanupUnusedTopologies();
        } catch (Exception e) {
            LOG.warn("Error trying to clean up old topologies", e);
        }
        
        for (Slot slot: slots.values()) {
            slot.start();
        }
    }

    private Slot mkSlot(int port) throws Exception {
        return new Slot(localizer, superConf, launcher, host, port,
                localState, stormClusterState, iSuper, cachedAssignments);
    }
    
    @Override
    public synchronized void run() {
        try {
            Runnable syncCallback = new EventManagerPushCallback(this, syncSupEventManager);
            List<String> stormIds = stormClusterState.assignments(syncCallback);
            Map<String, VersionedData<Assignment>> assignmentsSnapshot =
                    getAssignmentsSnapshot(stormClusterState, stormIds, assignmentVersions.get(), syncCallback);
            
            Map<Integer, LocalAssignment> allAssignments =
                    readAssignments(assignmentsSnapshot);
            if (allAssignments == null) {
                //Something odd happened try again later
                return;
            }
            Map<String, List<ProfileRequest>> topoIdToProfilerActions = getProfileActions(stormClusterState, stormIds);
            
            HashSet<Integer> assignedPorts = new HashSet<>();
            LOG.debug("Synchronizing supervisor");
            LOG.debug("All assignment: {}", allAssignments);
            LOG.debug("Topology Ids -> Profiler Actions {}", topoIdToProfilerActions);
            for (Integer port: allAssignments.keySet()) {
                if (iSuper.confirmAssigned(port)) {
                    assignedPorts.add(port);
                }
            }
            HashSet<Integer> allPorts = new HashSet<>(assignedPorts);
            allPorts.addAll(slots.keySet());
            
            Map<Integer, Set<TopoProfileAction>> filtered = new HashMap<>();
            for (Entry<String, List<ProfileRequest>> entry: topoIdToProfilerActions.entrySet()) {
                String topoId = entry.getKey();
                if (entry.getValue() != null) {
                    for (ProfileRequest req: entry.getValue()) {
                        NodeInfo ni = req.get_nodeInfo();
                        if (host.equals(ni.get_node())) {
                            Long port = ni.get_port().iterator().next();
                            Set<TopoProfileAction> actions = filtered.get(port.intValue());
                            if (actions == null) {
                                actions = new HashSet<>();
                                filtered.put(port.intValue(), actions);
                            }
                            actions.add(new TopoProfileAction(topoId, req));
                        }
                    }
                }
            }
            
            for (Integer port: allPorts) {
                Slot slot = slots.get(port);
                if (slot == null) {
                    slot = mkSlot(port);
                    slots.put(port, slot);
                    slot.start();
                }
                slot.setNewAssignment(allAssignments.get(port));
                slot.addProfilerActions(filtered.get(port));
            }
            
        } catch (Exception e) {
            LOG.error("Failed to Sync Supervisor", e);
            throw new RuntimeException(e);
        }
    }
    
    protected Map<String, VersionedData<Assignment>> getAssignmentsSnapshot(IStormClusterState stormClusterState, List<String> topoIds,
            Map<String, VersionedData<Assignment>> localAssignmentVersion, Runnable callback) throws Exception {
        Map<String, VersionedData<Assignment>> updateAssignmentVersion = new HashMap<>();
        for (String topoId : topoIds) {
            Integer recordedVersion = -1;
            Integer version = stormClusterState.assignmentVersion(topoId, callback);
            VersionedData<Assignment> locAssignment = localAssignmentVersion.get(topoId);
            if (locAssignment != null) {
                recordedVersion = locAssignment.getVersion();
            }
            if (version == null) {
                // ignore
            } else if (version.equals(recordedVersion)) {
                updateAssignmentVersion.put(topoId, locAssignment);
            } else {
                VersionedData<Assignment> assignmentVersion = stormClusterState.assignmentInfoWithVersion(topoId, callback);
                updateAssignmentVersion.put(topoId, assignmentVersion);
            }
        }
        return updateAssignmentVersion;
    }
    
    protected Map<String, List<ProfileRequest>> getProfileActions(IStormClusterState stormClusterState, List<String> stormIds) throws Exception {
        Map<String, List<ProfileRequest>> ret = new HashMap<String, List<ProfileRequest>>();
        for (String stormId : stormIds) {
            List<ProfileRequest> profileRequests = stormClusterState.getTopologyProfileRequests(stormId);
            ret.put(stormId, profileRequests);
        }
        return ret;
    }
    
    protected Map<Integer, LocalAssignment> readAssignments(Map<String, VersionedData<Assignment>> assignmentsSnapshot) {
        try {
            Map<Integer, LocalAssignment> portLA = new HashMap<>();
            for (Map.Entry<String, VersionedData<Assignment>> assignEntry : assignmentsSnapshot.entrySet()) {
                String topoId = assignEntry.getKey();
                Assignment assignment = assignEntry.getValue().getData();

                Map<Integer, LocalAssignment> portTasks = readMyExecutors(topoId, assignmentId, assignment);

                for (Map.Entry<Integer, LocalAssignment> entry : portTasks.entrySet()) {

                    Integer port = entry.getKey();

                    LocalAssignment la = entry.getValue();

                    if (!portLA.containsKey(port)) {
                        portLA.put(port, la);
                    } else {
                        throw new RuntimeException("Should not have multiple topologies assigned to one port "
                          + port + " " + la + " " + portLA);
                    }
                }
            }
            readRetry.set(0);
            return portLA;
        } catch (RuntimeException e) {
            if (readRetry.get() > 2) {
                throw e;
            } else {
                readRetry.addAndGet(1);
            }
            LOG.warn("{} : retrying {} of 3", e.getMessage(), readRetry.get());
            return null;
        }
    }
    
    protected Map<Integer, LocalAssignment> readMyExecutors(String topoId, String assignmentId, Assignment assignment) {
        Map<Integer, LocalAssignment> portTasks = new HashMap<>();
        Map<Long, WorkerResources> slotsResources = new HashMap<>();
        Map<NodeInfo, WorkerResources> nodeInfoWorkerResourcesMap = assignment.get_worker_resources();
        if (nodeInfoWorkerResourcesMap != null) {
            for (Map.Entry<NodeInfo, WorkerResources> entry : nodeInfoWorkerResourcesMap.entrySet()) {
                if (entry.getKey().get_node().equals(assignmentId)) {
                    Set<Long> ports = entry.getKey().get_port();
                    for (Long port : ports) {
                        slotsResources.put(port, entry.getValue());
                    }
                }
            }
        }
        boolean hasShared = false;
        double amountShared = 0.0;
        if (assignment.is_set_total_shared_off_heap()) {
            Double d = assignment.get_total_shared_off_heap().get(assignmentId);
            if (d != null) {
                amountShared = d;
                hasShared = true;
            }
        }
        Map<List<Long>, NodeInfo> executorNodePort = assignment.get_executor_node_port();
        if (executorNodePort != null) {
            for (Map.Entry<List<Long>, NodeInfo> entry : executorNodePort.entrySet()) {
                if (entry.getValue().get_node().equals(assignmentId)) {
                    for (Long port : entry.getValue().get_port()) {
                        LocalAssignment localAssignment = portTasks.get(port.intValue());
                        if (localAssignment == null) {
                            List<ExecutorInfo> executors = new ArrayList<>();
                            localAssignment = new LocalAssignment(topoId, executors);
                            if (slotsResources.containsKey(port)) {
                                localAssignment.set_resources(slotsResources.get(port));
                            }
                            if (hasShared) {
                                localAssignment.set_total_node_shared(amountShared);
			    }
                            if (assignment.is_set_owner()) {
                                localAssignment.set_owner(assignment.get_owner());
                            }
                            portTasks.put(port.intValue(), localAssignment);
                        }
                        List<ExecutorInfo> executorInfoList = localAssignment.get_executors();
                        executorInfoList.add(new ExecutorInfo(entry.getKey().get(0).intValue(), entry.getKey().get(entry.getKey().size() - 1).intValue()));
                    }
                }
            }
        }
        return portTasks;
    }

    private static final long WARN_MILLIS = 1_000; //Initial timeout 1 second.  Workers commit suicide after this
    private static final long ERROR_MILLIS = 60_000; //1 min.  This really means something is wrong.  Even on a very slow node
    public static final UniFunc<Slot> DEFAULT_ON_ERROR_TIMEOUT = (slot) -> {
        throw new IllegalStateException("It took over " + ERROR_MILLIS + "ms to shut down slot " + slot);
    };
    public static final UniFunc<Slot> DEFAULT_ON_WARN_TIMEOUT = (slot) -> LOG.warn("It has taken {}ms so far and {} is still not shut down.", WARN_MILLIS, slot);
    public static final UniFunc<Slot> THREAD_DUMP_ON_ERROR = (slot) -> {
        LOG.warn("Shutdown of slot {} appears to be stuck\n{}", slot, Utils.threadDump());
        DEFAULT_ON_ERROR_TIMEOUT.call(slot);
    };
    
    public synchronized void shutdownAllWorkers(UniFunc<Slot> onWarnTimeout, UniFunc<Slot> onErrorTimeout) {
        for (Slot slot: slots.values()) {
            LOG.info("Setting {} assignment to null", slot);
            slot.setNewAssignment(null);
        }
        
        if (onWarnTimeout == null) {
            onWarnTimeout = DEFAULT_ON_WARN_TIMEOUT;
        }
        
        if (onErrorTimeout == null) {
            onErrorTimeout = DEFAULT_ON_ERROR_TIMEOUT;
        }
        
        long startTime = Time.currentTimeMillis();
        Exception exp = null;
        for (Slot slot: slots.values()) {
            LOG.info("Waiting for {} to be EMPTY, currently {}", slot, slot.getMachineState());
            try {
                while (slot.getMachineState() != MachineState.EMPTY) {
                    long timeSpentMillis = Time.currentTimeMillis() - startTime;
                    if (timeSpentMillis > ERROR_MILLIS) {
                        onErrorTimeout.call(slot);
                    }
                    
                    if (timeSpentMillis > WARN_MILLIS) {
                        onWarnTimeout.call(slot);
                    }
                    if (Time.isSimulating()) {
                        Time.advanceTime(100);
                    }
                    Thread.sleep(100);
                }
            } catch (Exception e) {
                LOG.error("Error trying to shutdown workers in {}", slot, e);
                exp = e;
            }
        }
        if (exp != null) {
            if (exp instanceof RuntimeException) {
                throw (RuntimeException)exp;
            }
            throw new RuntimeException(exp);
        }
    }
    
    @Override
    public void close() {
        for (Slot slot: slots.values()) {
            try {
                slot.close();
            } catch (Exception e) {
                LOG.error("Error trying to shutdown {}", slot, e);
            }
        }
    }

}
