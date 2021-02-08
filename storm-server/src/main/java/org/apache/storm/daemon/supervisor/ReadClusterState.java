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
import java.util.function.BiConsumer;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.supervisor.Slot.MachineState;
import org.apache.storm.daemon.supervisor.Slot.TopoProfileAction;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.localizer.AsyncLocalizer;
import org.apache.storm.metricstore.MetricStoreConfig;
import org.apache.storm.metricstore.WorkerMetricsProcessor;
import org.apache.storm.scheduler.ISupervisor;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadClusterState implements Runnable, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ReadClusterState.class);
    private static final long ERROR_MILLIS = 60_000; //1 min.  This really means something is wrong.  Even on a very slow node
    public static final UniFunc<Slot> DEFAULT_ON_ERROR_TIMEOUT = (slot) -> {
        throw new IllegalStateException("It took over " + ERROR_MILLIS + "ms to shut down slot " + slot);
    };
    public static final UniFunc<Slot> THREAD_DUMP_ON_ERROR = (slot) -> {
        LOG.warn("Shutdown of slot {} appears to be stuck\n{}", slot, Utils.threadDump());
        DEFAULT_ON_ERROR_TIMEOUT.call(slot);
    };
    private static final long WARN_MILLIS = 1_000; //Initial timeout 1 second.  Workers commit suicide after this
    public static final BiConsumer<Slot, Long> DEFAULT_ON_WARN_TIMEOUT =
        (slot, elapsedTimeMs) -> LOG.warn("It has taken {}ms so far and {} is still not shut down.", elapsedTimeMs, slot);
    private final Map<String, Object> superConf;
    private final IStormClusterState stormClusterState;
    private final Map<Integer, Slot> slots = new HashMap<>();
    private final AtomicInteger readRetry = new AtomicInteger(0);
    private final String assignmentId;
    private final int supervisorPort;
    private final ISupervisor supervisor;
    private final AsyncLocalizer localizer;
    private final ContainerLauncher launcher;
    private final String host;
    private final LocalState localState;
    private final AtomicReference<Map<Long, LocalAssignment>> cachedAssignments;
    private final OnlyLatestExecutor<Integer> metricsExec;
    private final SlotMetrics slotMetrics;
    private WorkerMetricsProcessor metricsProcessor;

    public ReadClusterState(Supervisor supervisor) throws Exception {
        this.superConf = supervisor.getConf();
        this.stormClusterState = supervisor.getStormClusterState();
        this.assignmentId = supervisor.getAssignmentId();
        this.supervisorPort = supervisor.getThriftServerPort();
        this.supervisor = supervisor.getiSupervisor();
        this.localizer = supervisor.getAsyncLocalizer();
        this.host = supervisor.getHostName();
        this.localState = supervisor.getLocalState();
        this.cachedAssignments = supervisor.getCurrAssignment();
        this.metricsExec = new OnlyLatestExecutor<>(supervisor.getHeartbeatExecutor());
        this.slotMetrics = supervisor.getSlotMetrics();

        this.launcher = ContainerLauncher.make(superConf, assignmentId, supervisorPort,
            supervisor.getSharedContext(), supervisor.getMetricsRegistry(), supervisor.getContainerMemoryTracker(),
            supervisor.getSupervisorThriftInterface());

        this.metricsProcessor = null;
        try {
            this.metricsProcessor = MetricStoreConfig.configureMetricProcessor(superConf);
        } catch (Exception e) {
            // the metrics processor is not critical to the operation of the cluster, allow Supervisor to come up
            LOG.error("Failed to initialize metric processor", e);
        }

        @SuppressWarnings("unchecked")
        List<Integer> ports = SupervisorUtils.getSlotsPorts(superConf);
        for (Integer port : ports) {
            slots.put(port, mkSlot(port));
        }

        try {
            Collection<String> detachedRunningWorkers = SupervisorUtils.supervisorWorkerIds(superConf);
            for (Slot slot : slots.values()) {
                String workerId = slot.getWorkerId();
                // We ignore workers that are still bound to a slot, which is monitored by a supervisor
                if (workerId != null) {
                    detachedRunningWorkers.remove(workerId);
                }
            }
            if (!detachedRunningWorkers.isEmpty()) {
                LOG.info("Killing detached workers {}", detachedRunningWorkers);
                supervisor.killWorkers(detachedRunningWorkers, launcher);
            }
        } catch (Exception e) {
            LOG.warn("Error trying to clean up old workers", e);
        }

        for (Slot slot : slots.values()) {
            slot.start();
        }
    }

    private Slot mkSlot(int port) throws Exception {
        return new Slot(localizer, superConf, launcher, host, port,
                        localState, stormClusterState, supervisor, cachedAssignments, metricsExec, metricsProcessor, slotMetrics);
    }

    @Override
    public synchronized void run() {
        try {
            List<String> stormIds = stormClusterState.assignments(null);
            Map<String, Assignment> assignmentsSnapshot = getAssignmentsSnapshot(stormClusterState);

            Map<Integer, LocalAssignment> allAssignments = readAssignments(assignmentsSnapshot);
            if (allAssignments == null) {
                //Something odd happened try again later
                return;
            }
            Map<String, List<ProfileRequest>> topoIdToProfilerActions = getProfileActions(stormClusterState, stormIds);

            HashSet<Integer> assignedPorts = new HashSet<>();
            LOG.debug("Synchronizing supervisor");
            LOG.debug("All assignment: {}", allAssignments);
            LOG.debug("Topology Ids -> Profiler Actions {}", topoIdToProfilerActions);
            for (Integer port : allAssignments.keySet()) {
                if (supervisor.confirmAssigned(port)) {
                    assignedPorts.add(port);
                }
            }
            HashSet<Integer> allPorts = new HashSet<>(assignedPorts);
            supervisor.assigned(allPorts);
            allPorts.addAll(slots.keySet());

            Map<Integer, Set<TopoProfileAction>> filtered = new HashMap<>();
            for (Entry<String, List<ProfileRequest>> entry : topoIdToProfilerActions.entrySet()) {
                String topoId = entry.getKey();
                if (entry.getValue() != null) {
                    for (ProfileRequest req : entry.getValue()) {
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

            for (Integer port : allPorts) {
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

    protected Map<String, Assignment> getAssignmentsSnapshot(IStormClusterState stormClusterState) throws Exception {
        return stormClusterState.assignmentsInfo();
    }

    protected Map<String, List<ProfileRequest>> getProfileActions(IStormClusterState stormClusterState, List<String> stormIds) throws
        Exception {
        Map<String, List<ProfileRequest>> ret = new HashMap<String, List<ProfileRequest>>();
        for (String stormId : stormIds) {
            List<ProfileRequest> profileRequests = stormClusterState.getTopologyProfileRequests(stormId);
            ret.put(stormId, profileRequests);
        }
        return ret;
    }

    protected Map<Integer, LocalAssignment> readAssignments(Map<String, Assignment> assignmentsSnapshot) {
        try {
            Map<Integer, LocalAssignment> portLocalAssignment = new HashMap<>();
            for (Map.Entry<String, Assignment> assignEntry : assignmentsSnapshot.entrySet()) {
                String topoId = assignEntry.getKey();
                Assignment assignment = assignEntry.getValue();

                Map<Integer, LocalAssignment> portTasks = readMyExecutors(topoId, assignmentId, assignment);

                for (Map.Entry<Integer, LocalAssignment> entry : portTasks.entrySet()) {

                    Integer port = entry.getKey();

                    LocalAssignment la = entry.getValue();

                    if (!portLocalAssignment.containsKey(port)) {
                        portLocalAssignment.put(port, la);
                    } else {
                        throw new RuntimeException("Should not have multiple topologies assigned to one port "
                                                   + port + " " + la + " " + portLocalAssignment);
                    }
                }
            }
            readRetry.set(0);
            return portLocalAssignment;
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
                if (entry.getKey().get_node().startsWith(assignmentId)) {
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
                if (entry.getValue().get_node().startsWith(assignmentId)) {
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
                        executorInfoList.add(new ExecutorInfo(entry.getKey().get(0).intValue(),
                                                              entry.getKey().get(entry.getKey().size() - 1).intValue()));
                    }
                }
            }
        }
        return portTasks;
    }

    public synchronized void shutdownAllWorkers(BiConsumer<Slot, Long> onWarnTimeout, UniFunc<Slot> onErrorTimeout) {
        for (Slot slot : slots.values()) {
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
        for (Slot slot : slots.values()) {
            LOG.info("Waiting for {} to be EMPTY, currently {}", slot, slot.getMachineState());
            try {
                while (slot.getMachineState() != MachineState.EMPTY) {
                    long timeSpentMillis = Time.currentTimeMillis() - startTime;
                    if (timeSpentMillis > ERROR_MILLIS) {
                        onErrorTimeout.call(slot);
                    }

                    if (timeSpentMillis > WARN_MILLIS) {
                        onWarnTimeout.accept(slot, timeSpentMillis);
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
                throw (RuntimeException) exp;
            }
            throw new RuntimeException(exp);
        }
    }

    @Override
    public void close() {
        for (Slot slot : slots.values()) {
            try {
                slot.close();
            } catch (Exception e) {
                LOG.error("Error trying to shutdown {}", slot, e);
            }
        }
    }

}
