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

import com.codahale.metrics.Meter;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.ProfileAction;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.localizer.ILocalizer;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.ISupervisor;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Slot extends Thread implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(Slot.class);
    private static final Meter numWorkersLaunched =
        StormMetricsRegistry.registerMeter("supervisor:num-workers-launched");
    private static final Meter numWorkersKilledProcessExit =
        StormMetricsRegistry.registerMeter("supervisor:num-workers-killed-process-exit");
    private static final Meter numWorkersKilledMemoryViolation =
        StormMetricsRegistry.registerMeter("supervisor:num-workers-killed-memory-violation");
    private static final Meter numWorkersKilledHBTimeout =
        StormMetricsRegistry.registerMeter("supervisor:num-workers-killed-hb-timeout");
    private static final Meter numWorkersKilledHBNull =
        StormMetricsRegistry.registerMeter("supervisor:num-workers-killed-hb-null");
    private static final Meter numForceKill =
        StormMetricsRegistry.registerMeter("supervisor:num-workers-force-kill");

    static enum MachineState {
        EMPTY,
        RUNNING,
        WAITING_FOR_WORKER_START,
        KILL_AND_RELAUNCH,
        KILL,
        WAITING_FOR_BASIC_LOCALIZATION,
        WAITING_FOR_BLOB_LOCALIZATION;
    }
    
    static class StaticState {
        public final ILocalizer localizer;
        public final long hbTimeoutMs;
        public final long firstHbTimeoutMs;
        public final long killSleepMs;
        public final long monitorFreqMs;
        public final ContainerLauncher containerLauncher;
        public final int port;
        public final String host;
        public final ISupervisor iSupervisor;
        public final LocalState localState;
        
        StaticState(ILocalizer localizer, long hbTimeoutMs, long firstHbTimeoutMs,
                long killSleepMs, long monitorFreqMs,
                ContainerLauncher containerLauncher, String host, int port,
                ISupervisor iSupervisor, LocalState localState) {
            this.localizer = localizer;
            this.hbTimeoutMs = hbTimeoutMs;
            this.firstHbTimeoutMs = firstHbTimeoutMs;
            this.containerLauncher = containerLauncher;
            this.killSleepMs = killSleepMs;
            this.monitorFreqMs = monitorFreqMs;
            this.host = host;
            this.port = port;
            this.iSupervisor = iSupervisor;
            this.localState = localState;
        }
    }
    
    static class DynamicState {
        public final MachineState state;
        public final LocalAssignment newAssignment;
        public final LocalAssignment currentAssignment;
        public final Container container;
        public final LocalAssignment pendingLocalization;
        public final Future<Void> pendingDownload;
        public final Set<TopoProfileAction> profileActions;
        public final Set<TopoProfileAction> pendingStopProfileActions;
        
        /**
         * The last time that WAITING_FOR_WORKER_START, KILL, or KILL_AND_RELAUNCH were entered into.
         */
        public final long startTime;
        
        public DynamicState(final LocalAssignment currentAssignment, Container container, final LocalAssignment newAssignment) {
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
            
            this.startTime = System.currentTimeMillis();
            this.newAssignment = newAssignment;
            this.pendingLocalization = null;
            this.pendingDownload = null;
            this.profileActions = new HashSet<>();
            this.pendingStopProfileActions = new HashSet<>();
        }
        
        public DynamicState(final MachineState state, final LocalAssignment newAssignment,
                final Container container, final LocalAssignment currentAssignment,
                final LocalAssignment pendingLocalization, final long startTime,
                final Future<Void> pendingDownload, final Set<TopoProfileAction> profileActions, 
                final Set<TopoProfileAction> pendingStopProfileActions) {
            this.state = state;
            this.newAssignment = newAssignment;
            this.currentAssignment = currentAssignment;
            this.container = container;
            this.pendingLocalization = pendingLocalization;
            this.startTime = startTime;
            this.pendingDownload = pendingDownload;
            this.profileActions = profileActions;
            this.pendingStopProfileActions = pendingStopProfileActions;
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
                    this.pendingStopProfileActions);
        }
        
        public DynamicState withPendingLocalization(LocalAssignment pendingLocalization, Future<Void> pendingDownload) {
            return new DynamicState(this.state, this.newAssignment,
                    this.container, this.currentAssignment,
                    pendingLocalization, this.startTime,
                    pendingDownload, this.profileActions,
                    this.pendingStopProfileActions);
        }
        
        public DynamicState withPendingLocalization(Future<Void> pendingDownload) {
            return withPendingLocalization(this.pendingLocalization, pendingDownload);
        }
        
        public DynamicState withState(final MachineState state) {
            long newStartTime = Time.currentTimeMillis();
            return new DynamicState(state, this.newAssignment,
                    this.container, this.currentAssignment,
                    this.pendingLocalization, newStartTime,
                    this.pendingDownload, this.profileActions,
                    this.pendingStopProfileActions);
        }

        public DynamicState withCurrentAssignment(final Container container, final LocalAssignment currentAssignment) {
            return new DynamicState(this.state, this.newAssignment,
                    container, currentAssignment,
                    this.pendingLocalization, this.startTime,
                    this.pendingDownload, this.profileActions,
                    this.pendingStopProfileActions);
        }

        public DynamicState withProfileActions(Set<TopoProfileAction> profileActions, Set<TopoProfileAction> pendingStopProfileActions) {
            return new DynamicState(this.state, this.newAssignment,
                    this.container, this.currentAssignment,
                    this.pendingLocalization, this.startTime,
                    this.pendingDownload, profileActions,
                    pendingStopProfileActions);
        }
    };

    //In some cases the new LocalAssignment may be equivalent to the old, but
    // It is not equal.  In those cases we want to update the current assignment to
    // be the same as the new assignment
    private static DynamicState updateAssignmentIfNeeded(DynamicState dynamicState) {
        if (dynamicState.newAssignment != null
            && !dynamicState.newAssignment.equals(dynamicState.currentAssignment)) {
            dynamicState =
                dynamicState.withCurrentAssignment(dynamicState.container, dynamicState.newAssignment);
        }
        return dynamicState;
    }

    static class TopoProfileAction {
        public final String topoId;
        public final ProfileRequest request;

        public TopoProfileAction(String topoId, ProfileRequest request) {
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
    
    static boolean equivalent(LocalAssignment a, LocalAssignment b) {
        if (a == null && b == null) {
            return true;
        }
        if (a != null && b != null) {
            if (a.get_topology_id().equals(b.get_topology_id())) {
                Set<ExecutorInfo> aexec = new HashSet<>(a.get_executors());
                Set<ExecutorInfo> bexec = new HashSet<>(b.get_executors());
                if (aexec.equals(bexec)) {
                    boolean aHasResources = a.is_set_resources();
                    boolean bHasResources = b.is_set_resources();
                    if (!aHasResources && !bHasResources) {
                        return true;
                    }
                    if (aHasResources && bHasResources) {
                        if (a.get_resources().equals(b.get_resources())) {
                            return true;
                        }
                    }
                }
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
            case KILL_AND_RELAUNCH:
                return handleKillAndRelaunch(dynamicState, staticState);
            case KILL:
                return handleKill(dynamicState, staticState);
            case WAITING_FOR_BASIC_LOCALIZATION:
                return handleWaitingForBasicLocalization(dynamicState, staticState);
            case WAITING_FOR_BLOB_LOCALIZATION:
                return handleWaitingForBlobLocalization(dynamicState, staticState);
            default:
                throw new IllegalStateException("Code not ready to handle a state of "+dynamicState.state);
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
    static DynamicState prepareForNewAssignmentNoWorkersRunning(DynamicState dynamicState, StaticState staticState) throws IOException {
        assert(dynamicState.container == null);
        
        if (dynamicState.newAssignment == null) {
            return dynamicState.withState(MachineState.EMPTY);
        }
        Future<Void> pendingDownload = staticState.localizer.requestDownloadBaseTopologyBlobs(dynamicState.newAssignment, staticState.port);
        return dynamicState.withPendingLocalization(dynamicState.newAssignment, pendingDownload).withState(MachineState.WAITING_FOR_BASIC_LOCALIZATION);
    }
    
    /**
     * Kill the current container and start downloading what the new assignment needs, if there is a new assignment
     * PRECONDITION: container != null
     * @param dynamicState current state
     * @param staticState static data
     * @return the next state
     * @throws Exception 
     */
    static DynamicState killContainerForChangedAssignment(DynamicState dynamicState, StaticState staticState) throws Exception {
        assert(dynamicState.container != null);
        
        staticState.iSupervisor.killedWorker(staticState.port);
        dynamicState.container.kill();
        Future<Void> pendingDownload = null;
        if (dynamicState.newAssignment != null) {
            pendingDownload = staticState.localizer.requestDownloadBaseTopologyBlobs(dynamicState.newAssignment, staticState.port);
        }
        Time.sleep(staticState.killSleepMs);
        return dynamicState.withPendingLocalization(dynamicState.newAssignment, pendingDownload).withState(MachineState.KILL);
    }
    
    /**
     * Kill the current container and relaunch it.  (Something odd happened)
     * PRECONDITION: container != null
     * @param dynamicState current state
     * @param staticState static data
     * @return the next state
     * @throws Exception 
     */
    static DynamicState killAndRelaunchContainer(DynamicState dynamicState, StaticState staticState) throws Exception {
        assert(dynamicState.container != null);
        
        dynamicState.container.kill();
        Time.sleep(staticState.killSleepMs);
        
        //any stop profile actions that hadn't timed out yet, we should restart after the worker is running again.
        HashSet<TopoProfileAction> mod = new HashSet<>(dynamicState.profileActions);
        mod.addAll(dynamicState.pendingStopProfileActions);
        return dynamicState.withState(MachineState.KILL_AND_RELAUNCH).withProfileActions(mod, Collections.<TopoProfileAction> emptySet());
    }
    
    /**
     * Clean up a container
     * PRECONDITION: All of the processes have died.
     * @param dynamicState current state
     * @param staticState static data
     * @param nextState the next MachineState to go to.
     * @return the next state.
     */
    static DynamicState cleanupCurrentContainer(DynamicState dynamicState, StaticState staticState, MachineState nextState) throws Exception {
        assert(dynamicState.container != null);
        assert(dynamicState.currentAssignment != null);
        assert(dynamicState.container.areAllProcessesDead());
        
        dynamicState.container.cleanUp();
        staticState.localizer.releaseSlotFor(dynamicState.currentAssignment, staticState.port);
        DynamicState ret = dynamicState.withCurrentAssignment(null, null);
        if (nextState != null) {
            ret = ret.withState(nextState);
        }
        return ret;
    }
    
    /**
     * State Transitions for WAITING_FOR_BLOB_LOCALIZATION state.
     * PRECONDITION: neither pendingLocalization nor pendingDownload is null.
     * PRECONDITION: The slot should be empty
     * @param dynamicState current state
     * @param staticState static data
     * @return the next state
     * @throws Exception on any error
     */
    static DynamicState handleWaitingForBlobLocalization(DynamicState dynamicState, StaticState staticState) throws Exception {
        assert(dynamicState.pendingLocalization != null);
        assert(dynamicState.pendingDownload != null);
        assert(dynamicState.container == null);
        
        //Ignore changes to scheduling while downloading the topology blobs
        // We don't support canceling the download through the future yet,
        // so to keep everything in sync, just wait
        try {
            dynamicState.pendingDownload.get(1000, TimeUnit.MILLISECONDS);
            //Downloading of all blobs finished.
            if (!equivalent(dynamicState.newAssignment, dynamicState.pendingLocalization)) {
                //Scheduling changed
                staticState.localizer.releaseSlotFor(dynamicState.pendingLocalization, staticState.port);
                return prepareForNewAssignmentNoWorkersRunning(dynamicState, staticState);
            }
            dynamicState = updateAssignmentIfNeeded(dynamicState);
            numWorkersLaunched.mark();
            Container c = staticState.containerLauncher.launchContainer(staticState.port, dynamicState.pendingLocalization, staticState.localState);
            return dynamicState.withCurrentAssignment(c, dynamicState.pendingLocalization).withState(MachineState.WAITING_FOR_WORKER_START).withPendingLocalization(null, null);
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
            // release the reference on all blobs associated with this topology.
            staticState.localizer.releaseSlotFor(dynamicState.pendingLocalization, staticState.port);
            // we wait for 3 seconds
            Time.sleepSecs(3);
            return dynamicState.withState(MachineState.EMPTY);
        }
    }
    
    /**
     * State Transitions for WAITING_FOR_BASIC_LOCALIZATION state.
     * PRECONDITION: neither pendingLocalization nor pendingDownload is null.
     * PRECONDITION: The slot should be empty
     * @param dynamicState current state
     * @param staticState static data
     * @return the next state
     * @throws Exception on any error
     */
    static DynamicState handleWaitingForBasicLocalization(DynamicState dynamicState, StaticState staticState) throws Exception {
        assert(dynamicState.pendingLocalization != null);
        assert(dynamicState.pendingDownload != null);
        assert(dynamicState.container == null);
        
        //Ignore changes to scheduling while downloading the topology code
        // We don't support canceling the download through the future yet,
        // so to keep everything in sync, just wait
        try {
            dynamicState.pendingDownload.get(1000, TimeUnit.MILLISECONDS);
            Future<Void> pendingDownload = staticState.localizer.requestDownloadTopologyBlobs(dynamicState.pendingLocalization, staticState.port);
            return dynamicState.withPendingLocalization(pendingDownload).withState(MachineState.WAITING_FOR_BLOB_LOCALIZATION);
        } catch (TimeoutException e) {
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
    static DynamicState handleKill(DynamicState dynamicState, StaticState staticState) throws Exception {
        assert(dynamicState.container != null);
        assert(dynamicState.currentAssignment != null);
        
        if (dynamicState.container.areAllProcessesDead()) {
            LOG.warn("SLOT {} all processes are dead...", staticState.port);
            return cleanupCurrentContainer(dynamicState, staticState, 
                    dynamicState.pendingLocalization == null ? MachineState.EMPTY : MachineState.WAITING_FOR_BASIC_LOCALIZATION);
        }

        LOG.warn("SLOT {} force kill and wait...", staticState.port);
        numForceKill.mark();
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
    static DynamicState handleKillAndRelaunch(DynamicState dynamicState, StaticState staticState) throws Exception {
        assert(dynamicState.container != null);
        assert(dynamicState.currentAssignment != null);
        
        if (dynamicState.container.areAllProcessesDead()) {
            if (equivalent(dynamicState.newAssignment, dynamicState.currentAssignment)) {
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
        numForceKill.mark();
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
    static DynamicState handleWaitingForWorkerStart(DynamicState dynamicState, StaticState staticState) throws Exception {
        assert(dynamicState.container != null);
        assert(dynamicState.currentAssignment != null);
        
        LSWorkerHeartbeat hb = dynamicState.container.readHeartbeat();
        if (hb != null) {
            long hbAgeMs = (Time.currentTimeSecs() - hb.get_time_secs()) * 1000;
            if (hbAgeMs <= staticState.hbTimeoutMs) {
                return dynamicState.withState(MachineState.RUNNING);
            }
        }
        
        if (!equivalent(dynamicState.newAssignment, dynamicState.currentAssignment)) {
            //We were rescheduled while waiting for the worker to come up
            LOG.warn("SLOT {}: Assignment Changed from {} to {}", staticState.port, dynamicState.currentAssignment, dynamicState.newAssignment);
            return killContainerForChangedAssignment(dynamicState, staticState);
        }
        dynamicState = updateAssignmentIfNeeded(dynamicState);

        long timeDiffms = (Time.currentTimeMillis() - dynamicState.startTime);
        if (timeDiffms > staticState.firstHbTimeoutMs) {
            LOG.warn("SLOT {}: Container {} failed to launch in {} ms.", staticState.port, dynamicState.container, staticState.firstHbTimeoutMs);
            return killAndRelaunchContainer(dynamicState, staticState);
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
    static DynamicState handleRunning(DynamicState dynamicState, StaticState staticState) throws Exception {
        assert(dynamicState.container != null);
        assert(dynamicState.currentAssignment != null);
        
        if (!equivalent(dynamicState.newAssignment, dynamicState.currentAssignment)) {
            LOG.warn("SLOT {}: Assignment Changed from {} to {}", staticState.port, dynamicState.currentAssignment, dynamicState.newAssignment);
            //Scheduling changed while running...
            return killContainerForChangedAssignment(dynamicState, staticState);
        }
        dynamicState = updateAssignmentIfNeeded(dynamicState);

        if (dynamicState.container.didMainProcessExit()) {
            numWorkersKilledProcessExit.mark();
            LOG.warn("SLOT {}: main process has exited", staticState.port);
            return killAndRelaunchContainer(dynamicState, staticState);
        }

        if (dynamicState.container.isMemoryLimitViolated(dynamicState.currentAssignment)) {
            numWorkersKilledMemoryViolation.mark();
            LOG.warn("SLOT {}: violated memory limits", staticState.port);
            return killAndRelaunchContainer(dynamicState, staticState);
        }
        
        LSWorkerHeartbeat hb = dynamicState.container.readHeartbeat();
        if (hb == null) {
            numWorkersKilledHBNull.mark();
            LOG.warn("SLOT {}: HB returned as null", staticState.port);
            //This can happen if the supervisor crashed after launching a
            // worker that never came up.
            return killAndRelaunchContainer(dynamicState, staticState);
        }
        
        long timeDiffMs = (Time.currentTimeSecs() - hb.get_time_secs()) * 1000;
        if (timeDiffMs > staticState.hbTimeoutMs) {
            numWorkersKilledHBTimeout.mark();
            LOG.warn("SLOT {}: HB is too old {} > {}", staticState.port, timeDiffMs, staticState.hbTimeoutMs);
            return killAndRelaunchContainer(dynamicState, staticState);
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
        Time.sleep(staticState.monitorFreqMs);
        return dynamicState;
    }

    static DynamicState handleEmpty(DynamicState dynamicState, StaticState staticState) throws InterruptedException, IOException {
        if (!equivalent(dynamicState.newAssignment, dynamicState.currentAssignment)) {
            return prepareForNewAssignmentNoWorkersRunning(dynamicState, staticState);
        }
        dynamicState = updateAssignmentIfNeeded(dynamicState);
        
        //Both assignments are null, just wait
        if (dynamicState.profileActions != null && !dynamicState.profileActions.isEmpty()) {
            //Nothing is scheduled here so throw away all of the profileActions
            LOG.warn("Dropping {} no topology is running", dynamicState.profileActions);
            dynamicState = dynamicState.withProfileActions(Collections.<TopoProfileAction> emptySet(), Collections.<TopoProfileAction> emptySet());
        }
        Time.sleep(1000);
        return dynamicState;
    }

    private final AtomicReference<LocalAssignment> newAssignment = new AtomicReference<>();
    private final AtomicReference<Set<TopoProfileAction>> profiling =
            new AtomicReference<Set<TopoProfileAction>>(new HashSet<TopoProfileAction>());
    private final StaticState staticState;
    private final IStormClusterState clusterState;
    private volatile boolean done = false;
    private volatile DynamicState dynamicState;
    private final AtomicReference<Map<Long, LocalAssignment>> cachedCurrentAssignments;
    
    public Slot(ILocalizer localizer, Map<String, Object> conf, 
            ContainerLauncher containerLauncher, String host,
            int port, LocalState localState,
            IStormClusterState clusterState,
            ISupervisor iSupervisor,
            AtomicReference<Map<Long, LocalAssignment>> cachedCurrentAssignments) throws Exception {
        super("SLOT_"+port);

        this.cachedCurrentAssignments = cachedCurrentAssignments;
        this.clusterState = clusterState;
        Map<Integer, LocalAssignment> assignments = localState.getLocalAssignmentsMap();
        LocalAssignment currentAssignment = null;
        if (assignments != null) {
            currentAssignment = assignments.get(port);
        }
        Container container = null;
        if (currentAssignment != null) { 
            try {
                container = containerLauncher.recoverContainer(port, currentAssignment, localState);
            } catch (ContainerRecoveryException e) {
                //We could not recover container will be null.
            }
        }
        
        LocalAssignment newAssignment = currentAssignment;
        if (currentAssignment != null && container == null) {
            currentAssignment = null;
            //Assigned something but it is not running
        }
        
        dynamicState = new DynamicState(currentAssignment, container, newAssignment);
        staticState = new StaticState(localizer, 
                ObjectReader.getInt(conf.get(Config.SUPERVISOR_WORKER_TIMEOUT_SECS)) * 1000,
                ObjectReader.getInt(conf.get(DaemonConfig.SUPERVISOR_WORKER_START_TIMEOUT_SECS)) * 1000,
                ObjectReader.getInt(conf.get(DaemonConfig.SUPERVISOR_WORKER_SHUTDOWN_SLEEP_SECS)) * 1000,
                ObjectReader.getInt(conf.get(DaemonConfig.SUPERVISOR_MONITOR_FREQUENCY_SECS)) * 1000,
                containerLauncher,
                host,
                port,
                iSupervisor,
                localState);
        this.newAssignment.set(dynamicState.newAssignment);
        if (MachineState.RUNNING == dynamicState.state) {
            //We are running so we should recover the blobs.
            staticState.localizer.recoverRunningTopology(currentAssignment, port);
            saveNewAssignment(currentAssignment);
        }
        LOG.warn("SLOT {}:{} Starting in state {} - assignment {}", staticState.host, staticState.port, dynamicState.state, dynamicState.currentAssignment);
    }
    
    public MachineState getMachineState() {
        return dynamicState.state;
    }
    
    /**
     * Set a new assignment asynchronously
     * @param newAssignment the new assignment for this slot to run, null to run nothing
     */
    public void setNewAssignment(LocalAssignment newAssignment) {
        this.newAssignment.set(newAssignment);
    }
    
    public void addProfilerActions(Set<TopoProfileAction> actions) {
        if (actions != null) {
            while(true) {
                Set<TopoProfileAction> orig = profiling.get();
                Set<TopoProfileAction> newActions = new HashSet<>(orig);
                newActions.addAll(actions);
                if (profiling.compareAndSet(orig, newActions)) {
                    return;
                }
            }
        }
    }
    
    public String getWorkerId() {
        String workerId = null;
        Container c = dynamicState.container;
        if (c != null) {
            workerId = c.getWorkerId();
        }
        return workerId;
    }
    
    private void saveNewAssignment(LocalAssignment assignment) {
        synchronized(staticState.localState) {
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
    
    public void run() {
        try {
            while(!done) {
                Set<TopoProfileAction> origProfileActions = new HashSet<>(profiling.get());
                Set<TopoProfileAction> removed = new HashSet<>(origProfileActions);
                
                DynamicState nextState = 
                        stateMachineStep(dynamicState.withNewAssignment(newAssignment.get())
                                .withProfileActions(origProfileActions, dynamicState.pendingStopProfileActions), staticState);

                if (LOG.isDebugEnabled() || dynamicState.state != nextState.state) {
                    LOG.info("STATE {} -> {}", dynamicState, nextState);
                }
                //Save the current state for recovery
                if ((nextState.currentAssignment != null && !nextState.currentAssignment.equals(dynamicState.currentAssignment)) ||
                        (dynamicState.currentAssignment != null && !dynamicState.currentAssignment.equals(nextState.currentAssignment))) {
                    LOG.info("SLOT {}: Changing current assignment from {} to {}", staticState.port, dynamicState.currentAssignment, nextState.currentAssignment);
                    saveNewAssignment(nextState.currentAssignment);
                }

                if (equivalent(nextState.newAssignment, nextState.currentAssignment)
                    && nextState.currentAssignment != null && nextState.currentAssignment.get_owner() == null
                    && nextState.newAssignment != null && nextState.newAssignment.get_owner() != null) {
                    //This is an odd case for a rolling upgrade where the user on the old assignment may be null,
                    // but not on the new one.  Although in all other ways they are the same.
                    // If this happens we want to use the assignment with the owner.
                    LOG.info("Updating assignment to save owner {}", nextState.newAssignment.get_owner());
                    saveNewAssignment(nextState.newAssignment);
                    nextState = nextState.withCurrentAssignment(nextState.container, nextState.newAssignment);
                }
                
                // clean up the profiler actions that are not being processed
                removed.removeAll(dynamicState.profileActions);
                removed.removeAll(dynamicState.pendingStopProfileActions);
                for (TopoProfileAction action: removed) {
                    try {
                        clusterState.deleteTopologyProfileRequests(action.topoId, action.request);
                    } catch (Exception e) {
                        LOG.error("Error trying to remove profiling request, it will be retried", e);
                    }
                }
                Set<TopoProfileAction> orig, copy;
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
}
