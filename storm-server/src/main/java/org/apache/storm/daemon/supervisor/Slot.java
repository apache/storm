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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.ProfileAction;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.localizer.AsyncLocalizer;
import org.apache.storm.localizer.BlobChangingCallback;
import org.apache.storm.localizer.GoodToGo;
import org.apache.storm.localizer.LocallyCachedBlob;
import org.apache.storm.metricstore.WorkerMetricsProcessor;
import org.apache.storm.scheduler.ISupervisor;
import org.apache.storm.utils.EnumUtil;
import org.apache.storm.utils.EquivalenceUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Slot extends Thread implements AutoCloseable, BlobChangingCallback {
    private static final Logger LOG = LoggerFactory.getLogger(Slot.class);

    enum KillReason {
        ASSIGNMENT_CHANGED, BLOB_CHANGED, PROCESS_EXIT, MEMORY_VIOLATION, HB_TIMEOUT, HB_NULL;

        @Override
        public String toString() {
            return EnumUtil.toMetricName(this);
        }

    }

    private static final long ONE_SEC_IN_NANO = TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS);
    private final AtomicReference<LocalAssignment> newAssignment = new AtomicReference<>();
    private final AtomicReference<Set<TopoProfileAction>> profiling = new AtomicReference<>(new HashSet<>());

    private final BlockingQueue<BlobChanging> changingBlobs = new LinkedBlockingQueue<>();
    private final StaticState staticState;
    private final IStormClusterState clusterState;
    private final AtomicReference<Map<Long, LocalAssignment>> cachedCurrentAssignments;
    private final OnlyLatestExecutor<Integer> metricsExec;
    private volatile boolean done = false;
    private volatile DynamicState dynamicState;

    public Slot(AsyncLocalizer localizer, Map<String, Object> conf,
                ContainerLauncher containerLauncher, String host,
                int port, LocalState localState,
                IStormClusterState clusterState,
                ISupervisor supervisor,
                AtomicReference<Map<Long, LocalAssignment>> cachedCurrentAssignments,
                OnlyLatestExecutor<Integer> metricsExec,
                WorkerMetricsProcessor metricsProcessor,
                SlotMetrics slotMetrics) throws Exception {
        super("SLOT_" + port);
        this.metricsExec = metricsExec;
        this.cachedCurrentAssignments = cachedCurrentAssignments;
        this.clusterState = clusterState;
        this.staticState = new StaticState(localizer,
                ObjectReader.getInt(conf.get(Config.SUPERVISOR_WORKER_TIMEOUT_SECS)) * 1000,
                ObjectReader.getInt(conf.get(DaemonConfig.SUPERVISOR_WORKER_START_TIMEOUT_SECS)) * 1000,
                ObjectReader.getInt(conf.get(Config.SUPERVISOR_WORKER_SHUTDOWN_SLEEP_SECS)) * 1000,
                ObjectReader.getInt(conf.get(DaemonConfig.SUPERVISOR_MONITOR_FREQUENCY_SECS)) * 1000,
                containerLauncher,
                host,
                port,
                supervisor,
                localState,
                this,
                metricsExec, metricsProcessor, slotMetrics);

        LocalAssignment currentAssignment = null;
        Container container = null;
        LocalAssignment newAssignment = null;

        Map<Integer, LocalAssignment> assignments = localState.getLocalAssignmentsMap();
        if (assignments != null) {
            currentAssignment = assignments.get(port);
            if (currentAssignment != null) {
                try {
                    // For now we do not make a transaction when removing a topology assignment from local, an overdue
                    // assignment may be left on local disk.
                    // So we should check if the local disk assignment is valid when initializing:
                    // if topology files does not exist, the worker[possibly alive] will be reassigned if it is timed-out;
                    // if topology files exist but the topology id is invalid, just let Supervisor make a sync;
                    // if topology files exist and topology files is valid, recover the container.
                    if (ClientSupervisorUtils.doRequiredTopoFilesExist(conf, currentAssignment.get_topology_id())) {
                        container = containerLauncher.recoverContainer(port, currentAssignment, localState);
                    } else {
                        // Make the assignment null to let slot clean up the disk assignment.
                        currentAssignment = null;
                    }
                } catch (ContainerRecoveryException e) {
                    //We could not recover container will be null.
                    currentAssignment = null;
                }

                newAssignment = currentAssignment;
            }
        }

        setNewAssignment(newAssignment);
        //if the current assignment is already running, new assignment will never be promoted to currAssignment,
        // because Timer is not being compared in #equals or #equivalent, meaning newAssignment always equals to currAssignment.
        // Therefore the timer in newAssignment won't be invoked
        this.dynamicState = new DynamicState(currentAssignment, container, this.newAssignment.get(), slotMetrics);
        if (MachineState.RUNNING == dynamicState.state) {
            //We are running so we should recover the blobs.
            staticState.localizer.recoverRunningTopology(currentAssignment, port, this);
            saveNewAssignment(currentAssignment);
        }
        LOG.info("SLOT {}:{} Starting in state {} - assignment {}", staticState.host, staticState.port, dynamicState.state,
                 dynamicState.currentAssignment);
    }

    //In some cases the new LocalAssignment may be equivalent to the old, but
    // It is not equal.  In those cases we want to update the current assignment to
    // be the same as the new assignment
    //PRECONDITION: The new and current assignments must be equivalent
    private static DynamicState updateAssignmentIfNeeded(DynamicState dynamicState) {
        assert EquivalenceUtils.areLocalAssignmentsEquivalent(dynamicState.newAssignment, dynamicState.currentAssignment);
        if (dynamicState.newAssignment != null
            && !dynamicState.newAssignment.equals(dynamicState.currentAssignment)) {
            dynamicState =
                dynamicState.withCurrentAssignment(dynamicState.container, dynamicState.newAssignment);
        }
        return dynamicState;
    }

    @VisibleForTesting
    static boolean forSameTopology(LocalAssignment a, LocalAssignment b) {
        if (a == null && b == null) {
            return true;
        }
        if (a != null && b != null) {
            if (a.get_topology_id().equals(b.get_topology_id())) {
                return true;
            }
        }
        return false;
    }

    static DynamicState stateMachineStep(DynamicState dynamicState, StaticState staticState) throws Exception {
        LOG.debug("STATE {}", dynamicState.state);
        switch (dynamicState.state) {
            case EMPTY:
                return handleEmpty(dynamicState, staticState);
            case RUNNING:
                return handleRunning(dynamicState, staticState);
            case WAITING_FOR_WORKER_START:
                return handleWaitingForWorkerStart(dynamicState, staticState);
            case KILL_BLOB_UPDATE:
                return handleKillBlobUpdate(dynamicState, staticState);
            case KILL_AND_RELAUNCH:
                return handleKillAndRelaunch(dynamicState, staticState);
            case KILL:
                return handleKill(dynamicState, staticState);
            case WAITING_FOR_BLOB_LOCALIZATION:
                return handleWaitingForBlobLocalization(dynamicState, staticState);
            case WAITING_FOR_BLOB_UPDATE:
                return handleWaitingForBlobUpdate(dynamicState, staticState);
            default:
                throw new IllegalStateException("Code not ready to handle a state of " + dynamicState.state);
        }
    }

    /**
     * Prepare for a new assignment by downloading new required blobs, or going to empty if there is nothing to download.
     * PRECONDITION: The slot should be empty
     * @param dynamicState current state
     * @param staticState static data
     * @return the next state
     * @throws IOException on any error
     */
    private static DynamicState prepareForNewAssignmentNoWorkersRunning(DynamicState dynamicState,
            StaticState staticState) throws IOException {
        assert (dynamicState.container == null);
        assert dynamicState.currentAssignment == null;

        //We're either going to empty, or starting fresh blob download. Either way, the changing blob notifications are outdated.
        dynamicState = drainAllChangingBlobs(dynamicState);
        if (dynamicState.newAssignment == null) {
            return dynamicState.withState(MachineState.EMPTY);
        }
        Future<Void> pendingDownload = staticState.localizer.requestDownloadTopologyBlobs(dynamicState.newAssignment,
                                                                                          staticState.port, staticState.changingCallback);
        return dynamicState.withPendingLocalization(dynamicState.newAssignment, pendingDownload)
                           .withState(MachineState.WAITING_FOR_BLOB_LOCALIZATION);
    }

    private static DynamicState killContainerFor(KillReason reason, DynamicState dynamicState, StaticState staticState)
            throws Exception {
        assert dynamicState.container != null;

        //Skip special case if `storm kill_workers` is already invoked
        Boolean isDead = dynamicState.container.areAllProcessesDead();
        if (!isDead) {
            if (reason == KillReason.ASSIGNMENT_CHANGED || reason == KillReason.BLOB_CHANGED) {
                staticState.supervisor.killedWorker(staticState.port);
            }
            dynamicState.container.kill();
        }
        staticState.slotMetrics.numWorkersKilledFor.get(reason).mark();

        DynamicState next;
        switch (reason) {
            case ASSIGNMENT_CHANGED:
                Future<Void> pendingDownload = null;
                if (dynamicState.newAssignment != null) {
                    pendingDownload = staticState.localizer.requestDownloadTopologyBlobs(
                            dynamicState.newAssignment, staticState.port, staticState.changingCallback);
                }
                next = dynamicState.withState(MachineState.KILL)
                        .withPendingLocalization(dynamicState.newAssignment, pendingDownload);
                break;

            case BLOB_CHANGED:
                next = dynamicState.withState(MachineState.KILL_BLOB_UPDATE);
                break;

            case PROCESS_EXIT:
            case MEMORY_VIOLATION:
            case HB_TIMEOUT:
            case HB_NULL:
                //any stop profile actions that hadn't timed out yet, we should restart after the worker is running again.
                HashSet<TopoProfileAction> mod = new HashSet<>(dynamicState.profileActions);
                mod.addAll(dynamicState.pendingStopProfileActions);
                next = dynamicState.withState(MachineState.KILL_AND_RELAUNCH).withProfileActions(mod, Collections.emptySet());
                break;

            default:
                throw new IllegalArgumentException("Unknown reason for killing a container");
        }

        if (!isDead) {
            Time.sleep(staticState.killSleepMs);
        }
        return next;
    }

    /**
     * Clean up a container.
     * PRECONDITION: All of the processes have died.
     * @param dynamicState current state
     * @param staticState static data
     * @param nextState the next MachineState to go to.
     * @return the next state.
     */
    private static DynamicState cleanupCurrentContainer(DynamicState dynamicState, StaticState staticState, MachineState nextState) throws
        Exception {
        assert (dynamicState.container != null);
        assert (dynamicState.currentAssignment != null);
        assert (dynamicState.container.areAllProcessesDead());

        dynamicState.container.cleanUp();
        dynamicState.cancelPendingBlobs();
        staticState.localizer.releaseSlotFor(dynamicState.currentAssignment, staticState.port);
        DynamicState ret = dynamicState.withCurrentAssignment(null, null);
        if (nextState != null) {
            ret = ret.withState(nextState);
        }
        return ret;
    }

    /**
     * Drop all of the changingBlobs and pendingChangingBlobs.
     * 
     * <p>PRECONDITION: container is null
     *
     * @param dynamicState current state.
     * @return the next state.
     */
    private static DynamicState drainAllChangingBlobs(DynamicState dynamicState) {
        assert dynamicState.container == null;
        if (!dynamicState.changingBlobs.isEmpty()) {
            for (BlobChanging rc : dynamicState.changingBlobs) {
                rc.latch.countDown();
            }
            dynamicState = dynamicState.withChangingBlobs(Collections.emptySet());
        }

        dynamicState.cancelPendingBlobs();
        if (!dynamicState.pendingChangingBlobs.isEmpty()) {
            dynamicState = dynamicState.withPendingChangingBlobs(Collections.emptySet(), null);
        }

        return dynamicState;
    }

    /**
     * Informs the async localizer for all of blobs that the worker acknowledged the change of blobs.
     * Worker has stop as of now.
     *
     * <p>PRECONDITION: container is null
     * PRECONDITION: changingBlobs should only be for the given assignment.
     *
     * @param dynamicState the current state
     * @return the futures for the current assignment.
     */
    private static DynamicState informChangedBlobs(DynamicState dynamicState, LocalAssignment assignment) {
        assert dynamicState.container == null;
        assert dynamicState.changingBlobs.stream().allMatch((cr) -> forSameTopology(cr.assignment, assignment));

        Set<Future<Void>> futures = new HashSet<>(dynamicState.changingBlobs.size());

        // We need to add the new futures to the existing ones
        if (forSameTopology(dynamicState.pendingChangingBlobsAssignment, assignment)) {
            futures.addAll(dynamicState.pendingChangingBlobs);
        }

        // Acknowledge all changing blobs as futures
        for (BlobChanging rc : dynamicState.changingBlobs) {
            futures.add(rc.latch.countDown());
        }

        LOG.debug("found changing blobs {} moving them to pending...", dynamicState.changingBlobs);
        return dynamicState.withChangingBlobs(Collections.emptySet())
                           .withPendingChangingBlobs(futures, assignment);
    }

    /**
     * Filter all of the changing blobs to just be for those compatible with the given assignment.
     * All others will be released appropriately.
     *
     * @param dynamicState the current state
     * @param assignment the assignment to look for
     * @return the updated dynamicState
     */
    private static DynamicState filterChangingBlobsFor(DynamicState dynamicState, final LocalAssignment assignment) {
        if (dynamicState.changingBlobs.isEmpty()) {
            return dynamicState;
        }

        HashSet<BlobChanging> savedBlobs = new HashSet<>(dynamicState.changingBlobs.size());
        for (BlobChanging rc : dynamicState.changingBlobs) {
            if (forSameTopology(assignment, rc.assignment)) {
                savedBlobs.add(rc);
            } else {
                rc.latch.countDown();
            }
        }
        return dynamicState.withChangingBlobs(savedBlobs);
    }

    /**
     * State Transitions for WAITING_FOR_BLOB_LOCALIZATION state, when the slot is waiting for
     * blobs of the pending assignment to be completely downloaded, before the container is launched/relaunched.
     * PRECONDITION: neither pendingLocalization nor pendingDownload is null.
     * PRECONDITION: The slot should be empty
     * @param dynamicState current state
     * @param staticState static data
     * @return the next state
     * @throws Exception on any error
     */
    private static DynamicState handleWaitingForBlobLocalization(DynamicState dynamicState, StaticState staticState) throws Exception {
        assert (dynamicState.pendingLocalization != null);
        assert (dynamicState.pendingDownload != null);
        assert (dynamicState.container == null);
        assert dynamicState.currentAssignment == null;

        //Ignore changes to scheduling while downloading the topology blobs
        // We don't support canceling the download through the future yet,
        // because pending blobs may be shared by multiple workers and cancel it
        // may lead to race condition
        // To keep everything in sync, just wait for all workers
        try {
            //Release things that don't need to wait for us to finish downloading.
            dynamicState = filterChangingBlobsFor(dynamicState, dynamicState.pendingLocalization);
            if (!dynamicState.changingBlobs.isEmpty()) {
                //Unblock downloading by accepting the futures.
                dynamicState = informChangedBlobs(dynamicState, dynamicState.pendingLocalization);
            }

            if (!EquivalenceUtils.areLocalAssignmentsEquivalent(dynamicState.newAssignment, dynamicState.pendingLocalization)) {
                //Scheduling changed
                dynamicState.cancelPendingBlobs();
                staticState.localizer.releaseSlotFor(dynamicState.pendingLocalization, staticState.port);
                // Switch to the new assignment even if localization hasn't completed, or go to empty state
                // if no new assignment.
                return prepareForNewAssignmentNoWorkersRunning(dynamicState
                    .withPendingLocalization(null, null),
                    staticState);
            }

            // Wait until time out
            dynamicState.pendingDownload.get(1000, TimeUnit.MILLISECONDS);
            //Downloading of all blobs finished. This is the precondition for all codes below.
            if (!dynamicState.pendingChangingBlobs.isEmpty()) {
                LOG.info("There are pending changes, waiting for them to finish before launching container...");
                //We cannot launch the container yet the resources may still be updating
                return dynamicState.withState(MachineState.WAITING_FOR_BLOB_UPDATE)
                                   .withPendingLocalization(null, null);
            }

            staticState.slotMetrics.numWorkersLaunched.mark();
            Container c =
                staticState.containerLauncher.launchContainer(staticState.port, dynamicState.pendingLocalization, staticState.localState);
            return dynamicState
                .withCurrentAssignment(c, dynamicState.pendingLocalization).withState(MachineState.WAITING_FOR_WORKER_START)
                .withPendingLocalization(null, null);
        } catch (TimeoutException e) {
            //We waited for 1 second loop around and try again....
            return dynamicState;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AuthorizationException) {
                LOG.error("{}", ((AuthorizationException) e.getCause()).get_msg());
            } else if (e.getCause() instanceof KeyNotFoundException) {
                LOG.error("{}", ((KeyNotFoundException) e.getCause()).get_msg());
            } else {
                LOG.error("{}", e.getCause().getMessage());
            }

            // release the reference on all blobs associated with this worker.
            dynamicState.cancelPendingBlobs();
            staticState.localizer.releaseSlotFor(dynamicState.pendingLocalization, staticState.port);
            // we wait for 3 seconds
            Time.sleepSecs(3);

            //Try again, or go to empty if assignment has been nulled
            return prepareForNewAssignmentNoWorkersRunning(dynamicState
                .withPendingLocalization(null, null),
                staticState);
        }
    }

    /**
     * State Transitions for WAITING_FOR_BLOB_UPDATE state.
     *
     * <p>PRECONDITION: container is null
     * PRECONDITION: pendingChangingBlobs is not empty (otherwise why did we go to this state)
     * PRECONDITION: pendingChangingBlobsAssignment is not null.
     *
     * @param dynamicState current state
     * @param staticState static data
     * @return the next state
     * @throws Exception on any error
     */
    private static DynamicState handleWaitingForBlobUpdate(DynamicState dynamicState, StaticState staticState)
        throws Exception {
        assert dynamicState.container == null;
        assert dynamicState.pendingChangingBlobsAssignment != null;
        assert !dynamicState.pendingChangingBlobs.isEmpty();
        assert dynamicState.pendingDownload == null;
        assert dynamicState.pendingLocalization == null;

        if (!EquivalenceUtils.areLocalAssignmentsEquivalent(dynamicState.newAssignment, dynamicState.currentAssignment)) {
            //We were rescheduled while waiting for the resources to be updated,
            // but the container is already not running.
            LOG.info("SLOT {}: Assignment Changed from {} to {}", staticState.port,
                     dynamicState.currentAssignment, dynamicState.newAssignment);
            dynamicState.cancelPendingBlobs();
            if (dynamicState.currentAssignment != null) {
                staticState.localizer.releaseSlotFor(dynamicState.currentAssignment, staticState.port);
            }
            staticState.localizer.releaseSlotFor(dynamicState.pendingChangingBlobsAssignment,
                    staticState.port);
            return prepareForNewAssignmentNoWorkersRunning(dynamicState.withCurrentAssignment(null, null),
                                                           staticState);
        }

        dynamicState = filterChangingBlobsFor(dynamicState, dynamicState.pendingChangingBlobsAssignment);
        if (!dynamicState.changingBlobs.isEmpty()) {
            dynamicState = informChangedBlobs(dynamicState, dynamicState.pendingChangingBlobsAssignment);
        }

        //We only have a set amount of time we can wait for before looping around again
        long start = Time.nanoTime();
        try {
            for (Future<Void> pending : dynamicState.pendingChangingBlobs) {
                long now = Time.nanoTime();
                long timeLeft = ONE_SEC_IN_NANO - (now - start);
                if (timeLeft <= 0) {
                    throw new TimeoutException();
                }
                pending.get(timeLeft, TimeUnit.NANOSECONDS);
            }
            //All done we can launch the worker now
            Container c = staticState.containerLauncher.launchContainer(staticState.port, dynamicState.pendingChangingBlobsAssignment,
                                                                        staticState.localState);
            return dynamicState
                .withCurrentAssignment(c, dynamicState.pendingChangingBlobsAssignment).withState(MachineState.WAITING_FOR_WORKER_START)
                .withPendingChangingBlobs(Collections.emptySet(), null);
        } catch (TimeoutException ex) {
            return dynamicState;
        }
    }

    /**
     * State Transitions for KILL state.
     * PRECONDITION: container.kill() was called
     * PRECONDITION: container != null && currentAssignment != null
     * @param dynamicState current state
     * @param staticState static data
     * @return the next state
     * @throws Exception on any error
     */
    private static DynamicState handleKill(DynamicState dynamicState, StaticState staticState) throws Exception {
        assert (dynamicState.container != null);
        assert (dynamicState.currentAssignment != null);
        assert dynamicState.pendingChangingBlobs.isEmpty();
        assert dynamicState.pendingChangingBlobsAssignment == null;

        if (dynamicState.container.areAllProcessesDead()) {
            LOG.info("SLOT {} all processes are dead...", staticState.port);
            return cleanupCurrentContainer(dynamicState,
                    staticState,
                    dynamicState.pendingLocalization == null
                            ? MachineState.EMPTY
                            : MachineState.WAITING_FOR_BLOB_LOCALIZATION);
        }

        LOG.info("SLOT {} force kill and wait...", staticState.port);
        dynamicState.container.forceKill();
        Time.sleep(staticState.killSleepMs);
        return dynamicState;
    }

    /**
     * State Transitions for KILL_AND_RELAUNCH state.
     * PRECONDITION: container.kill() was called
     * PRECONDITION: container != null && currentAssignment != null
     * @param dynamicState current state
     * @param staticState static data
     * @return the next state
     * @throws Exception on any error
     */
    private static DynamicState handleKillAndRelaunch(DynamicState dynamicState, StaticState staticState) throws Exception {
        assert (dynamicState.container != null);
        assert (dynamicState.currentAssignment != null);
        assert dynamicState.pendingChangingBlobs.isEmpty();
        assert dynamicState.pendingChangingBlobsAssignment == null;
        assert dynamicState.pendingLocalization == null;
        assert dynamicState.pendingDownload == null;

        if (dynamicState.container.areAllProcessesDead()) {
            if (EquivalenceUtils.areLocalAssignmentsEquivalent(dynamicState.newAssignment, dynamicState.currentAssignment)) {
                dynamicState.container.cleanUpForRestart();
                dynamicState.container.relaunch();
                return dynamicState.withState(MachineState.WAITING_FOR_WORKER_START);
            }
            //Scheduling changed after we killed all of the processes
            return prepareForNewAssignmentNoWorkersRunning(cleanupCurrentContainer(dynamicState, staticState, null), staticState);
        }
        //The child processes typically exit in < 1 sec.  If 2 mins later they are still around something is wrong
        if ((Time.currentTimeMillis() - dynamicState.startTime) > 120_000) {
            throw new RuntimeException("Not all processes in " + dynamicState.container + " exited after 120 seconds");
        }
        dynamicState.container.forceKill();
        Time.sleep(staticState.killSleepMs);
        return dynamicState;
    }

    /**
     * State Transitions for KILL_BLOB_UPDATE state.
     * PRECONDITION: container.kill() was called
     * PRECONDITION: container != null && currentAssignment != null
     *
     * @param dynamicState current state
     * @param staticState static data
     * @return the next state
     * @throws Exception on any error
     */
    private static DynamicState handleKillBlobUpdate(DynamicState dynamicState, StaticState staticState) throws Exception {
        assert (dynamicState.container != null);
        assert (dynamicState.currentAssignment != null);
        assert dynamicState.pendingChangingBlobs.isEmpty();
        assert dynamicState.pendingChangingBlobsAssignment == null;
        assert dynamicState.pendingDownload == null;
        assert dynamicState.pendingLocalization == null;

        //Release things that don't need to wait for us
        dynamicState = filterChangingBlobsFor(dynamicState, dynamicState.currentAssignment);

        if (dynamicState.container.areAllProcessesDead()) {
            if (EquivalenceUtils.areLocalAssignmentsEquivalent(dynamicState.newAssignment, dynamicState.currentAssignment)) {
                dynamicState.container.cleanUp();
                dynamicState = dynamicState.withCurrentAssignment(null, dynamicState.currentAssignment);
                return informChangedBlobs(dynamicState, dynamicState.currentAssignment)
                    .withState(MachineState.WAITING_FOR_BLOB_UPDATE);
            }
            //Scheduling changed after we killed all of the processes
            return prepareForNewAssignmentNoWorkersRunning(cleanupCurrentContainer(dynamicState, staticState, null), staticState);
        }
        //The child processes typically exit in < 1 sec.  If 2 mins later they are still around something is wrong
        if ((Time.currentTimeMillis() - dynamicState.startTime) > 120_000) {
            throw new RuntimeException("Not all processes in " + dynamicState.container + " exited after 120 seconds");
        }
        dynamicState.container.forceKill();
        Time.sleep(staticState.killSleepMs);
        return dynamicState;
    }

    /**
     * State Transitions for WAITING_FOR_WORKER_START state.
     * PRECONDITION: container != null && currentAssignment != null
     * @param dynamicState current state
     * @param staticState static data
     * @return the next state
     * @throws Exception on any error
     */
    private static DynamicState handleWaitingForWorkerStart(DynamicState dynamicState, StaticState staticState) throws Exception {
        assert (dynamicState.container != null);
        assert (dynamicState.currentAssignment != null);
        assert dynamicState.pendingChangingBlobs.isEmpty();
        assert dynamicState.pendingChangingBlobsAssignment == null;
        assert dynamicState.pendingDownload == null;
        assert dynamicState.pendingLocalization == null;

        LSWorkerHeartbeat hb = dynamicState.container.readHeartbeat();
        if (hb != null) {
            long hbAgeMs = (Time.currentTimeSecs() - hb.get_time_secs()) * 1000;
            long hbTimeoutMs = getHbTimeoutMs(staticState, dynamicState);
            if (hbAgeMs <= hbTimeoutMs) {
                return dynamicState.withState(MachineState.RUNNING);
            }
        }

        if (!EquivalenceUtils.areLocalAssignmentsEquivalent(dynamicState.newAssignment, dynamicState.currentAssignment)) {
            //We were rescheduled while waiting for the worker to come up
            LOG.info("SLOT {}: Assignment Changed from {} to {}", staticState.port, dynamicState.currentAssignment,
                     dynamicState.newAssignment);
            return killContainerFor(KillReason.ASSIGNMENT_CHANGED, dynamicState, staticState);
        }
        dynamicState = updateAssignmentIfNeeded(dynamicState);

        long timeDiffms = (Time.currentTimeMillis() - dynamicState.startTime);
        long hbFirstTimeoutMs = getFirstHbTimeoutMs(staticState, dynamicState);
        if (timeDiffms > hbFirstTimeoutMs) {
            staticState.slotMetrics.numWorkerStartTimedOut.mark();
            LOG.warn("SLOT {}: Container {} failed to launch in {} ms.", staticState.port, dynamicState.container,
                    hbFirstTimeoutMs);
            return killContainerFor(KillReason.HB_TIMEOUT, dynamicState, staticState);
        }

        dynamicState = filterChangingBlobsFor(dynamicState, dynamicState.currentAssignment);
        if (!dynamicState.changingBlobs.isEmpty()) {
            //Kill the container and restart it
            return killContainerFor(KillReason.BLOB_CHANGED, dynamicState, staticState);
        }
        Time.sleep(1000);
        return dynamicState;
    }

    /**
     * State Transitions for RUNNING state.
     * PRECONDITION: container != null && currentAssignment != null
     * @param dynamicState current state
     * @param staticState static data
     * @return the next state
     * @throws Exception on any error
     */
    private static DynamicState handleRunning(DynamicState dynamicState, StaticState staticState) throws Exception {
        assert (dynamicState.container != null);
        assert (dynamicState.currentAssignment != null);
        assert dynamicState.pendingChangingBlobs.isEmpty();
        assert dynamicState.pendingChangingBlobsAssignment == null;
        assert dynamicState.pendingDownload == null;
        assert dynamicState.pendingLocalization == null;

        if (!EquivalenceUtils.areLocalAssignmentsEquivalent(dynamicState.newAssignment, dynamicState.currentAssignment)) {
            LOG.info("SLOT {}: Assignment Changed from {} to {}", staticState.port, dynamicState.currentAssignment,
                     dynamicState.newAssignment);
            //Scheduling changed while running...
            return killContainerFor(KillReason.ASSIGNMENT_CHANGED, dynamicState, staticState);
        }
        dynamicState = updateAssignmentIfNeeded(dynamicState);

        dynamicState = filterChangingBlobsFor(dynamicState, dynamicState.currentAssignment);
        if (!dynamicState.changingBlobs.isEmpty()) {
            //Kill the container and restart it
            return killContainerFor(KillReason.BLOB_CHANGED, dynamicState, staticState);
        }

        if (dynamicState.container.didMainProcessExit()) {
            LOG.warn("SLOT {}: main process has exited for topology: {}",
                    staticState.port, dynamicState.currentAssignment.get_topology_id());
            return killContainerFor(KillReason.PROCESS_EXIT, dynamicState, staticState);
        }

        if (dynamicState.container.isMemoryLimitViolated(dynamicState.currentAssignment)) {
            LOG.warn("SLOT {}: violated memory limits for topology: {}",
                    staticState.port, dynamicState.currentAssignment.get_topology_id());
            return killContainerFor(KillReason.MEMORY_VIOLATION, dynamicState, staticState);
        }

        LSWorkerHeartbeat hb = dynamicState.container.readHeartbeat();
        if (hb == null) {
            LOG.warn("SLOT {}: HB returned as null for topology: {}",
                    staticState.port, dynamicState.currentAssignment.get_topology_id());
            //This can happen if the supervisor crashed after launching a
            // worker that never came up.
            return killContainerFor(KillReason.HB_NULL, dynamicState, staticState);
        }

        long timeDiffMs = (Time.currentTimeSecs() - hb.get_time_secs()) * 1000;
        long hbTimeoutMs = getHbTimeoutMs(staticState, dynamicState);
        if (timeDiffMs > hbTimeoutMs) {
            LOG.warn("SLOT {}: HB is too old {} > {} for topology: {}",
                    staticState.port, timeDiffMs, hbTimeoutMs, dynamicState.currentAssignment.get_topology_id());
            return killContainerFor(KillReason.HB_TIMEOUT, dynamicState, staticState);
        }

        //The worker is up and running check for profiling requests
        if (!dynamicState.profileActions.isEmpty()) {
            HashSet<TopoProfileAction> mod = new HashSet<>(dynamicState.profileActions);
            HashSet<TopoProfileAction> modPending = new HashSet<>(dynamicState.pendingStopProfileActions);
            Iterator<TopoProfileAction> iter = mod.iterator();
            while (iter.hasNext()) {
                TopoProfileAction action = iter.next();
                if (!action.topoId.equals(dynamicState.currentAssignment.get_topology_id())) {
                    iter.remove();
                    LOG.warn("Dropping {} wrong topology is running", action);
                    //Not for this topology so skip it
                } else {
                    if (modPending.contains(action)) {
                        boolean isTimeForStop = Time.currentTimeMillis() > action.request.get_time_stamp();
                        if (isTimeForStop) {
                            if (dynamicState.container.runProfiling(action.request, true)) {
                                LOG.debug("Stopped {} action finished", action);
                                iter.remove();
                                modPending.remove(action);
                            } else {
                                LOG.warn("Stopping {} failed, will be retried", action);
                            }
                        } else {
                            LOG.debug("Still pending {} now: {}", action, Time.currentTimeMillis());
                        }
                    } else {
                        //J_PROFILE_START is not used.  When you see a J_PROFILE_STOP
                        // start profiling and save it away to stop when timeout happens
                        if (action.request.get_action() == ProfileAction.JPROFILE_STOP) {
                            if (dynamicState.container.runProfiling(action.request, false)) {
                                modPending.add(action);
                                LOG.debug("Started {} now: {}", action, Time.currentTimeMillis());
                            } else {
                                LOG.warn("Starting {} failed, will be retried", action);
                            }
                        } else {
                            if (dynamicState.container.runProfiling(action.request, false)) {
                                LOG.debug("Started {} action finished", action);
                                iter.remove();
                            } else {
                                LOG.warn("Starting {} failed, will be retried", action);
                            }
                        }
                    }
                }
            }
            dynamicState = dynamicState.withProfileActions(mod, modPending);
        }

        dynamicState.container.processMetrics(staticState.metricsExec, staticState.metricsProcessor);

        Time.sleep(staticState.monitorFreqMs);
        return dynamicState;
    }

    static DynamicState handleEmpty(DynamicState dynamicState, StaticState staticState) throws InterruptedException, IOException {
        assert dynamicState.currentAssignment == null;
        assert dynamicState.container == null;
        assert dynamicState.pendingChangingBlobs.isEmpty();
        assert dynamicState.pendingChangingBlobsAssignment == null;
        assert dynamicState.pendingDownload == null;
        assert dynamicState.pendingLocalization == null;
        if (!EquivalenceUtils.areLocalAssignmentsEquivalent(dynamicState.newAssignment, dynamicState.currentAssignment)) {
            return prepareForNewAssignmentNoWorkersRunning(dynamicState, staticState);
        }
        dynamicState = updateAssignmentIfNeeded(dynamicState);

        //Both assignments are null, just wait
        if (dynamicState.profileActions != null && !dynamicState.profileActions.isEmpty()) {
            //Nothing is scheduled here so throw away all of the profileActions
            LOG.warn("Dropping {} no topology is running", dynamicState.profileActions);
            dynamicState = dynamicState.withProfileActions(Collections.emptySet(), Collections.emptySet());
        }
        //Drop the change notifications we are not running anything right now
        dynamicState = drainAllChangingBlobs(dynamicState);
        Time.sleep(1000);
        return dynamicState;
    }

    MachineState getMachineState() {
        return dynamicState.state;
    }

    /*
     * Get worker heartbeat timeout time in ms. Use topology specified timeout if provided.
     */
    private static long getHbTimeoutMs(StaticState staticState, DynamicState dynamicState) {
        long hbTimeoutMs = staticState.hbTimeoutMs;
        Map<String, Object> topoConf = dynamicState.container.topoConf;

        if (topoConf != null && topoConf.containsKey(Config.TOPOLOGY_WORKER_TIMEOUT_SECS)) {
            long topoHbTimeoutMs = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_WORKER_TIMEOUT_SECS)) * 1000;
            topoHbTimeoutMs = Math.max(topoHbTimeoutMs, hbTimeoutMs);
            hbTimeoutMs = topoHbTimeoutMs;
        }

        return hbTimeoutMs;
    }

    /*
     * Get worker heartbeat timeout when waiting for worker to start.
     * If topology specific timeout if set, ensure first heartbeat timeout >= topology specific timeout.
     */
    private static long getFirstHbTimeoutMs(StaticState staticState, DynamicState dynamicState) {
        return Math.max(getHbTimeoutMs(staticState, dynamicState), staticState.firstHbTimeoutMs);
    }

    /**
     * Set a new assignment asynchronously.
     * @param newAssignment the new assignment for this slot to run, null to run nothing
     */
    public final void setNewAssignment(LocalAssignment newAssignment) {
        this.newAssignment.set(newAssignment == null
                ? null
                : new TimerDecoratedAssignment(newAssignment, staticState.slotMetrics.workerLaunchDuration));
    }

    @Override
    public void blobChanging(LocalAssignment assignment, int port, LocallyCachedBlob blob, GoodToGo go) {
        assert port == staticState.port : "got a callback that is not for us " + port + " != " + staticState.port;
        //This is called async so lets assume that it is something we care about
        try {
            changingBlobs.put(new BlobChanging(assignment, blob, go.getLatch()));
        } catch (InterruptedException e) {
            throw new RuntimeException("This should not have happened, but it did (the queue is unbounded)", e);
        }
    }

    public void addProfilerActions(Set<TopoProfileAction> actions) {
        if (actions != null) {
            while (true) {
                Set<TopoProfileAction> orig = profiling.get();
                Set<TopoProfileAction> newActions = new HashSet<>(orig);
                newActions.addAll(actions);
                if (profiling.compareAndSet(orig, newActions)) {
                    return;
                }
            }
        }
    }

    /**
     * get the workerID (nullable) from CURRENT container, if existed, or return null.
     * @return workerID
     */
    public String getWorkerId() {
        String workerId = null;
        Container c = dynamicState.container;
        if (c != null) {
            workerId = c.getWorkerId();
        }
        return workerId;
    }

    private void saveNewAssignment(LocalAssignment assignment) {
        synchronized (staticState.localState) {
            Map<Integer, LocalAssignment> assignments = staticState.localState.getLocalAssignmentsMap();
            if (assignments == null) {
                assignments = new HashMap<>();
            }
            if (assignment == null) {
                assignments.remove(staticState.port);
            } else {
                assignments.put(staticState.port, assignment);
            }
            staticState.localState.setLocalAssignmentsMap(assignments);
        }
        Map<Long, LocalAssignment> update = null;
        Map<Long, LocalAssignment> orig = null;
        do {
            Long lport = new Long(staticState.port);
            orig = cachedCurrentAssignments.get();
            update = new HashMap<>(orig);
            if (assignment == null) {
                update.remove(lport);
            } else {
                update.put(lport, assignment);
            }
        } while (!cachedCurrentAssignments.compareAndSet(orig, update));
    }

    @Override
    public void run() {
        try {
            while (!done) {
                Set<TopoProfileAction> origProfileActions = new HashSet<>(profiling.get());

                Set<BlobChanging> changingResourcesToHandle = dynamicState.changingBlobs;
                if (!changingBlobs.isEmpty()) {
                    changingResourcesToHandle = new HashSet<>(changingResourcesToHandle);
                    changingBlobs.drainTo(changingResourcesToHandle);
                    Iterator<BlobChanging> it = changingResourcesToHandle.iterator();

                    //Remove/Clean up changed requests that are not for us
                    while (it.hasNext()) {
                        BlobChanging rc = it.next();
                        if (!forSameTopology(rc.assignment, dynamicState.currentAssignment)
                                && !forSameTopology(rc.assignment, dynamicState.newAssignment)) {
                            rc.latch.countDown(); //Ignore the future
                            it.remove();
                        }
                    }
                }

                DynamicState nextState =
                    stateMachineStep(dynamicState.withNewAssignment(newAssignment.get())
                                                 .withProfileActions(origProfileActions, dynamicState.pendingStopProfileActions)
                                                 .withChangingBlobs(changingResourcesToHandle), staticState);

                if (LOG.isDebugEnabled() || dynamicState.state != nextState.state) {
                    LOG.info("STATE {} -> {}", dynamicState, nextState);
                }
                //Save the current state for recovery
                if ((nextState.currentAssignment != null
                                && !nextState.currentAssignment.equals(dynamicState.currentAssignment))
                        || (dynamicState.currentAssignment != null
                                && !dynamicState.currentAssignment.equals(nextState.currentAssignment))) {
                    LOG.info("SLOT {}: Changing current assignment from {} to {}", staticState.port, dynamicState.currentAssignment,
                             nextState.currentAssignment);
                    saveNewAssignment(nextState.currentAssignment);
                }

                if (EquivalenceUtils.areLocalAssignmentsEquivalent(nextState.newAssignment, nextState.currentAssignment)
                        && nextState.currentAssignment != null
                        && nextState.currentAssignment.get_owner() == null
                        && nextState.newAssignment != null
                        && nextState.newAssignment.get_owner() != null) {
                    //This is an odd case for a rolling upgrade where the user on the old assignment may be null,
                    // but not on the new one.  Although in all other ways they are the same.
                    // If this happens we want to use the assignment with the owner.
                    LOG.info("Updating assignment to save owner {}", nextState.newAssignment.get_owner());
                    saveNewAssignment(nextState.newAssignment);
                    nextState = nextState.withCurrentAssignment(nextState.container, nextState.newAssignment);
                }

                // clean up the profiler actions that are not being processed
                Set<TopoProfileAction> removed = new HashSet<>(origProfileActions);
                removed.removeAll(dynamicState.profileActions);
                removed.removeAll(dynamicState.pendingStopProfileActions);
                for (TopoProfileAction action : removed) {
                    try {
                        clusterState.deleteTopologyProfileRequests(action.topoId, action.request);
                    } catch (Exception e) {
                        LOG.error("Error trying to remove profiling request, it will be retried", e);
                    }
                }
                Set<TopoProfileAction> orig;
                Set<TopoProfileAction> copy;
                do {
                    orig = profiling.get();
                    copy = new HashSet<>(orig);
                    copy.removeAll(removed);
                } while (!profiling.compareAndSet(orig, copy));
                dynamicState = nextState;
            }
        } catch (Throwable e) {
            if (!Utils.exceptionCauseIsInstanceOf(InterruptedException.class, e)) {
                LOG.error("Error when processing event", e);
                Utils.exitProcess(20, "Error when processing an event");
            }
        }
    }

    @Override
    public void close() throws Exception {
        done = true;
        this.interrupt();
        this.join();
    }

    enum MachineState {
        /**
         * Slot is empty, and no worker is running.
         */
        EMPTY,
        /**
         * Slot is running a worker.
         */
        RUNNING,
        /**
         * Slot has launched a worker, and is waiting for it to heartbeat.
         */
        WAITING_FOR_WORKER_START,
        /**
         * Slot has just killed its worker, and is now waiting for it to die so it can be relaunched in the same container.
         */
        KILL_AND_RELAUNCH,
        /**
         * Slot has just killed its worker, and is now waiting for it to die so the container can be deleted. 
         */
        KILL,
        /**
         * Slot has just killed its worker, and is now waiting for it to die so the localizer can update a blob.
         */
        KILL_BLOB_UPDATE,
        /**
         * The slot is empty, and is waiting for blobs to download before the worker can be launched.
         */
        WAITING_FOR_BLOB_LOCALIZATION,
        /**
         * The slot is empty, and is waiting for blobs to be updated before the worker can be (re)launched.
         */
        WAITING_FOR_BLOB_UPDATE;

        @Override
        public String toString() {
            return EnumUtil.toMetricName(this);
        }
    }

    static class StaticState {
        public final AsyncLocalizer localizer;
        public final long hbTimeoutMs;
        public final long firstHbTimeoutMs;
        public final long killSleepMs;
        public final long monitorFreqMs;
        public final ContainerLauncher containerLauncher;
        public final int port;
        public final String host;
        public final ISupervisor supervisor;
        public final LocalState localState;
        public final BlobChangingCallback changingCallback;
        public final OnlyLatestExecutor<Integer> metricsExec;
        public final WorkerMetricsProcessor metricsProcessor;
        public final SlotMetrics slotMetrics;

        StaticState(AsyncLocalizer localizer, long hbTimeoutMs, long firstHbTimeoutMs,
                    long killSleepMs, long monitorFreqMs,
                    ContainerLauncher containerLauncher, String host, int port,
                    ISupervisor supervisor, LocalState localState,
                    BlobChangingCallback changingCallback,
                    OnlyLatestExecutor<Integer> metricsExec,
                    WorkerMetricsProcessor metricsProcessor,
                    SlotMetrics slotMetrics) {
            this.localizer = localizer;
            this.hbTimeoutMs = hbTimeoutMs;
            this.firstHbTimeoutMs = firstHbTimeoutMs;
            this.containerLauncher = containerLauncher;
            this.killSleepMs = killSleepMs;
            this.monitorFreqMs = monitorFreqMs;
            this.host = host;
            this.port = port;
            this.supervisor = supervisor;
            this.localState = localState;
            this.changingCallback = changingCallback;
            this.metricsExec = metricsExec;
            this.metricsProcessor = metricsProcessor;
            this.slotMetrics = slotMetrics;
        }
    }

    static class DynamicState {
        public final MachineState state;

        /**
         * Latest assignment assigned to this worker, updated asynchronously.
         */
        public final LocalAssignment newAssignment;

        /**
         * Assignment the worker is running on, when the state machine enters this state.
         */
        public final LocalAssignment currentAssignment;

        /**
         * Assignment assigned to the worker and waiting for blob download.
         * This is the same as newAssignment if no newer assignment is arrived before
         * localization completes.
         */
        public final LocalAssignment pendingLocalization;

        public final Container container;

        /**
         * Signals that blobs have been downloaded for the first time.
         */
        public final Future<Void> pendingDownload;
        public final Set<TopoProfileAction> profileActions;
        public final Set<TopoProfileAction> pendingStopProfileActions;

        /**
         * Blobs that are changed and need to be synced. The localizer notifies the Slot about changing blobs on every state step.
         * Blob updates are blocked until the Slot unblocks them, at which point they go in {@link #pendingChangingBlobs}.
         * Updates are blocked until the Slot worker is dead, since blobs may otherwise be actively used.
         */
        public final Set<BlobChanging> changingBlobs;
        
        /**
         * The assignment {@link #pendingChangingBlobs} belongs to.
         */
        public final LocalAssignment pendingChangingBlobsAssignment;

        public final Set<Future<Void>> pendingChangingBlobs;

        /**
         * The entering time when the machine transitions to current state.
         */
        public final long startTime;
        private final SlotMetrics slotMetrics;

        DynamicState(final LocalAssignment currentAssignment, Container container, final LocalAssignment newAssignment,
            SlotMetrics slotMetrics) {
            this.currentAssignment = currentAssignment;
            this.container = container;
            if ((currentAssignment == null) ^ (container == null)) {
                throw new IllegalArgumentException("Container and current assignment must both be null, or neither can be null");
            }

            if (currentAssignment == null) {
                state = MachineState.EMPTY;
            } else {
                state = MachineState.RUNNING;
            }
            slotMetrics.transitionIntoState.get(state).mark();

            this.startTime = Time.currentTimeMillis();
            this.newAssignment = newAssignment;
            this.pendingLocalization = null;
            this.pendingDownload = null;
            this.profileActions = Collections.emptySet();
            this.pendingStopProfileActions = Collections.emptySet();
            this.changingBlobs = Collections.emptySet();
            this.pendingChangingBlobsAssignment = null;
            this.pendingChangingBlobs = Collections.emptySet();
            this.slotMetrics = slotMetrics;
        }

        DynamicState(final MachineState state, final LocalAssignment newAssignment,
                            final Container container, final LocalAssignment currentAssignment,
                            final LocalAssignment pendingLocalization, final long startTime,
                            final Future<Void> pendingDownload, final Set<TopoProfileAction> profileActions,
                            final Set<TopoProfileAction> pendingStopProfileActions,
                            final Set<BlobChanging> changingBlobs,
                            final Set<Future<Void>> pendingChangingBlobs, final LocalAssignment pendingChaningBlobsAssignment,
                            final SlotMetrics slotMetrics) {
            assert pendingChangingBlobs != null;
            assert pendingChangingBlobs.isEmpty() == (pendingChaningBlobsAssignment == null);
            this.state = state;
            this.newAssignment = newAssignment;
            this.currentAssignment = currentAssignment;
            this.container = container;
            this.pendingLocalization = pendingLocalization;
            this.startTime = startTime;
            this.pendingDownload = pendingDownload;
            this.profileActions = profileActions;
            this.pendingStopProfileActions = pendingStopProfileActions;
            this.changingBlobs = changingBlobs;
            this.pendingChangingBlobs = pendingChangingBlobs;
            this.pendingChangingBlobsAssignment = pendingChaningBlobsAssignment;
            this.slotMetrics = slotMetrics;
        }

        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append(state);
            sb.append(" msInState: ");
            sb.append(Time.currentTimeMillis() - startTime);
            if (container != null) {
                sb.append(" ");
                sb.append(container);
            }
            return sb.toString();
        }

        /**
         * Set the new assignment for the state.  This should never be called from within the state machine.
         * It is an input from outside.
         * @param newAssignment the new assignment to set
         * @return the updated DynamicState.
         */
        public DynamicState withNewAssignment(LocalAssignment newAssignment) {
            return new DynamicState(this.state, newAssignment,
                                    this.container, this.currentAssignment,
                                    this.pendingLocalization, this.startTime,
                                    this.pendingDownload, this.profileActions,
                                    this.pendingStopProfileActions, this.changingBlobs,
                                    this.pendingChangingBlobs, this.pendingChangingBlobsAssignment, this.slotMetrics);
        }

        public DynamicState withPendingLocalization(LocalAssignment pendingLocalization, Future<Void> pendingDownload) {
            return new DynamicState(this.state, this.newAssignment,
                                    this.container, this.currentAssignment,
                                    pendingLocalization, this.startTime,
                                    pendingDownload, this.profileActions,
                                    this.pendingStopProfileActions, this.changingBlobs,
                                    this.pendingChangingBlobs, this.pendingChangingBlobsAssignment, this.slotMetrics);
        }

        public DynamicState withPendingLocalization(Future<Void> pendingDownload) {
            return withPendingLocalization(this.pendingLocalization, pendingDownload);
        }

        /**
         * Transition to the given state. Notice that it's possible to transition to
         * the same state.
         * @param state The state to transition into
         * @return New dynamicState
         */
        public DynamicState withState(final MachineState state) {
            long newStartTime = Time.currentTimeMillis();
            //We may (though unlikely) lose metering here if state transition is too frequent (less than a millisecond)
            slotMetrics.timeSpentInState.get(this.state).update(newStartTime - startTime, TimeUnit.MILLISECONDS);
            slotMetrics.transitionIntoState.get(state).mark();

            LocalAssignment assignment = this.currentAssignment;
            if (MachineState.RUNNING != this.state && MachineState.RUNNING == state
                && this.currentAssignment instanceof TimerDecoratedAssignment) {
                ((TimerDecoratedAssignment) assignment).stopTiming();
                //Timer is discarded after the initial launch of an assignment
                assignment = new LocalAssignment(this.currentAssignment);
            }

            return new DynamicState(state, this.newAssignment,
                                    this.container, assignment,
                                    this.pendingLocalization, newStartTime,
                                    this.pendingDownload, this.profileActions,
                                    this.pendingStopProfileActions, this.changingBlobs,
                                    this.pendingChangingBlobs, this.pendingChangingBlobsAssignment, this.slotMetrics);
        }

        public DynamicState withCurrentAssignment(final Container container, final LocalAssignment currentAssignment) {
            return new DynamicState(this.state, this.newAssignment,
                                    container, currentAssignment,
                                    this.pendingLocalization, this.startTime,
                                    this.pendingDownload, this.profileActions,
                                    this.pendingStopProfileActions, this.changingBlobs,
                                    this.pendingChangingBlobs, this.pendingChangingBlobsAssignment, this.slotMetrics);
        }

        public DynamicState withProfileActions(Set<TopoProfileAction> profileActions, Set<TopoProfileAction> pendingStopProfileActions) {
            return new DynamicState(this.state, this.newAssignment,
                                    this.container, this.currentAssignment,
                                    this.pendingLocalization, this.startTime,
                                    this.pendingDownload, profileActions,
                                    pendingStopProfileActions, this.changingBlobs,
                                    this.pendingChangingBlobs, this.pendingChangingBlobsAssignment, this.slotMetrics);
        }

        /**
         * Set the blocked changing blobs. This is an input from the outside, and should never be called by the state machine steps.
         */
        public DynamicState withChangingBlobs(Set<BlobChanging> changingBlobs) {
            if (changingBlobs == this.changingBlobs) {
                return this;
            }
            return new DynamicState(this.state, this.newAssignment,
                                    this.container, this.currentAssignment,
                                    this.pendingLocalization, this.startTime,
                                    this.pendingDownload, profileActions,
                                    this.pendingStopProfileActions, changingBlobs,
                                    this.pendingChangingBlobs, this.pendingChangingBlobsAssignment, this.slotMetrics);
        }

        public DynamicState withPendingChangingBlobs(Set<Future<Void>> pendingChangingBlobs,
                                                     LocalAssignment pendingChangingBlobsAssignment) {
            return new DynamicState(this.state, this.newAssignment,
                                    this.container, this.currentAssignment,
                                    this.pendingLocalization, this.startTime,
                                    this.pendingDownload, profileActions,
                                    this.pendingStopProfileActions, this.changingBlobs,
                                    pendingChangingBlobs,
                                    pendingChangingBlobsAssignment, this.slotMetrics);
        }

        private void cancelPendingBlobs() {
            // Make sure any downloading blobs in the background are stopped.
            // This prevents a race condition where we could be adding references to a
            // delayed downloading blob after the slot gets released, causing orphaned blobs.
            for (Future future : pendingChangingBlobs) {
                if (!future.isDone()) {
                    LOG.info("Canceling download of {}", future);
                    future.cancel(true);
                }
            }
        }
    }

    static class TopoProfileAction {
        public final String topoId;
        public final ProfileRequest request;

        TopoProfileAction(String topoId, ProfileRequest request) {
            this.topoId = topoId;
            this.request = request;
        }

        @Override
        public int hashCode() {
            return (37 * topoId.hashCode()) + request.hashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof TopoProfileAction)) {
                return false;
            }
            TopoProfileAction o = (TopoProfileAction) other;
            return topoId.equals(o.topoId) && request.equals(o.request);
        }

        @Override
        public String toString() {
            return "{ " + topoId + ": " + request + " }";
        }
    }

    /**
     * Holds the information about a blob that is changing.
     */
    static class BlobChanging {
        private final LocalAssignment assignment;
        private final LocallyCachedBlob blob;
        private final GoodToGo.GoodToGoLatch latch;

        BlobChanging(LocalAssignment assignment, LocallyCachedBlob blob, GoodToGo.GoodToGoLatch latch) {
            this.assignment = assignment;
            this.blob = blob;
            this.latch = latch;
        }

        @Override
        public String toString() {
            return "BLOB CHANGING " + blob + " " + assignment;
        }
    }
}
