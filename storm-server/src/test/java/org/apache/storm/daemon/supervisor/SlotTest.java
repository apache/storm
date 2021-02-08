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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.storm.daemon.supervisor.Slot.DynamicState;
import org.apache.storm.daemon.supervisor.Slot.MachineState;
import org.apache.storm.daemon.supervisor.Slot.StaticState;
import org.apache.storm.daemon.supervisor.Slot.TopoProfileAction;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.ProfileAction;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.localizer.AsyncLocalizer;
import org.apache.storm.localizer.BlobChangingCallback;
import org.apache.storm.localizer.GoodToGo;
import org.apache.storm.localizer.LocallyCachedBlob;
import org.apache.storm.scheduler.ISupervisor;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import java.util.concurrent.ExecutionException;
import org.apache.storm.metric.StormMetricsRegistry;

public class SlotTest {
    private static final Logger LOG = LoggerFactory.getLogger(SlotTest.class);

    static WorkerResources mkWorkerResources(Double cpu, Double mem_on_heap, Double mem_off_heap) {
        WorkerResources resources = new WorkerResources();
        if (cpu != null) {
            resources.set_cpu(cpu);
        }

        if (mem_on_heap != null) {
            resources.set_mem_on_heap(mem_on_heap);
        }

        if (mem_off_heap != null) {
            resources.set_mem_off_heap(mem_off_heap);
        }
        return resources;
    }

    static LSWorkerHeartbeat mkWorkerHB(String id, int port, List<ExecutorInfo> exec, Integer timeSecs) {
        LSWorkerHeartbeat ret = new LSWorkerHeartbeat();
        ret.set_topology_id(id);
        ret.set_port(port);
        ret.set_executors(exec);
        ret.set_time_secs(timeSecs);
        return ret;
    }

    static List<ExecutorInfo> mkExecutorInfoList(int... executors) {
        ArrayList<ExecutorInfo> ret = new ArrayList<>(executors.length);
        for (int exec : executors) {
            ExecutorInfo execInfo = new ExecutorInfo();
            execInfo.set_task_start(exec);
            execInfo.set_task_end(exec);
            ret.add(execInfo);
        }
        return ret;
    }

    static LocalAssignment mkLocalAssignment(String id, List<ExecutorInfo> exec, WorkerResources resources) {
        LocalAssignment ret = new LocalAssignment();
        ret.set_topology_id(id);
        ret.set_executors(exec);
        if (resources != null) {
            ret.set_resources(resources);
        }
        return ret;
    }

    @Test
    public void testForSameTopology() {
        LocalAssignment a = mkLocalAssignment("A", mkExecutorInfoList(1, 2, 3, 4, 5), mkWorkerResources(100.0, 100.0, 100.0));
        LocalAssignment aResized = mkLocalAssignment("A", mkExecutorInfoList(1, 2, 3, 4, 5), mkWorkerResources(100.0, 200.0, 100.0));
        LocalAssignment b = mkLocalAssignment("B", mkExecutorInfoList(1, 2, 3, 4, 5, 6), mkWorkerResources(100.0, 100.0, 100.0));
        LocalAssignment bReordered = mkLocalAssignment("B", mkExecutorInfoList(6, 5, 4, 3, 2, 1), mkWorkerResources(100.0, 100.0, 100.0));

        assertTrue(Slot.forSameTopology(null, null));
        assertTrue(Slot.forSameTopology(a, a));
        assertTrue(Slot.forSameTopology(a, aResized));
        assertTrue(Slot.forSameTopology(aResized, a));
        assertTrue(Slot.forSameTopology(b, bReordered));
        assertTrue(Slot.forSameTopology(bReordered, b));

        assertFalse(Slot.forSameTopology(a, null));
        assertFalse(Slot.forSameTopology(null, b));
        assertFalse(Slot.forSameTopology(a, b));
    }

    @Test
    public void testEmptyToEmpty() throws Exception {
        try (SimulatedTime t = new SimulatedTime(1010)) {
            AsyncLocalizer localizer = mock(AsyncLocalizer.class);
            LocalState state = mock(LocalState.class);
            BlobChangingCallback cb = mock(BlobChangingCallback.class);
            ContainerLauncher containerLauncher = mock(ContainerLauncher.class);
            ISupervisor iSuper = mock(ISupervisor.class);
            SlotMetrics slotMetrics = new SlotMetrics(new StormMetricsRegistry());
            StaticState staticState = new StaticState(localizer, 1000, 1000, 1000, 1000,
                containerLauncher, "localhost", 8080, iSuper, state, cb, null, null,
                slotMetrics);
            DynamicState dynamicState = new DynamicState(null, null, null, slotMetrics);
            DynamicState nextState = Slot.handleEmpty(dynamicState, staticState);
            assertEquals(MachineState.EMPTY, nextState.state);
            assertTrue(Time.currentTimeMillis() > 1000);
        }
    }

    @Test
    public void testLaunchContainerFromEmpty() throws Exception {
        try (SimulatedTime t = new SimulatedTime(1010)) {
            int port = 8080;
            String topoId = "NEW";
            List<ExecutorInfo> execList = mkExecutorInfoList(1, 2, 3, 4, 5);
            LocalAssignment newAssignment =
                mkLocalAssignment(topoId, execList, mkWorkerResources(100.0, 100.0, 100.0));

            AsyncLocalizer localizer = mock(AsyncLocalizer.class);
            BlobChangingCallback cb = mock(BlobChangingCallback.class);
            Container container = mock(Container.class);
            LocalState state = mock(LocalState.class);
            ContainerLauncher containerLauncher = mock(ContainerLauncher.class);
            when(containerLauncher.launchContainer(port, newAssignment, state)).thenReturn(container);
            LSWorkerHeartbeat hb = mkWorkerHB(topoId, port, execList, Time.currentTimeSecs());
            when(container.readHeartbeat()).thenReturn(hb, hb);

            @SuppressWarnings("unchecked")
            CompletableFuture<Void> blobFuture = mock(CompletableFuture.class);
            when(localizer.requestDownloadTopologyBlobs(newAssignment, port, cb)).thenReturn(blobFuture);

            ISupervisor iSuper = mock(ISupervisor.class);
            SlotMetrics slotMetrics = new SlotMetrics(new StormMetricsRegistry());
            StaticState staticState = new StaticState(localizer, 5000, 120000, 1000, 1000,
                                                      containerLauncher, "localhost", port, iSuper, state, cb, null, null, slotMetrics);
            DynamicState dynamicState = new DynamicState(null, null, null, slotMetrics)
                .withNewAssignment(newAssignment);

            DynamicState nextState = Slot.stateMachineStep(dynamicState, staticState);
            verify(localizer).requestDownloadTopologyBlobs(newAssignment, port, cb);
            assertEquals(MachineState.WAITING_FOR_BLOB_LOCALIZATION, nextState.state);
            assertSame("pendingDownload not set properly", blobFuture, nextState.pendingDownload);
            assertEquals(newAssignment, nextState.pendingLocalization);
            assertEquals(0, Time.currentTimeMillis());

            nextState = Slot.stateMachineStep(nextState, staticState);
            verify(blobFuture).get(1000, TimeUnit.MILLISECONDS);
            verify(containerLauncher).launchContainer(port, newAssignment, state);
            assertEquals(MachineState.WAITING_FOR_WORKER_START, nextState.state);
            assertSame("pendingDownload is not null", null, nextState.pendingDownload);
            assertSame(null, nextState.pendingLocalization);
            assertSame(newAssignment, nextState.currentAssignment);
            assertSame(container, nextState.container);
            assertEquals(0, Time.currentTimeMillis());

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.RUNNING, nextState.state);
            assertSame("pendingDownload is not null", null, nextState.pendingDownload);
            assertSame(null, nextState.pendingLocalization);
            assertSame(newAssignment, nextState.currentAssignment);
            assertSame(container, nextState.container);
            assertEquals(0, Time.currentTimeMillis());

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.RUNNING, nextState.state);
            assertSame("pendingDownload is not null", null, nextState.pendingDownload);
            assertSame(null, nextState.pendingLocalization);
            assertSame(newAssignment, nextState.currentAssignment);
            assertSame(container, nextState.container);
            assertTrue(Time.currentTimeMillis() > 1000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.RUNNING, nextState.state);
            assertSame("pendingDownload is not null", null, nextState.pendingDownload);
            assertSame(null, nextState.pendingLocalization);
            assertSame(newAssignment, nextState.currentAssignment);
            assertSame(container, nextState.container);
            assertTrue(Time.currentTimeMillis() > 2000);
        }
    }
    
    @Test
    public void testErrorHandlingWhenLocalizationFails() throws Exception {
        try (SimulatedTime t = new SimulatedTime(1010)) {
            int port = 8080;
            String topoId = "NEW";
            List<ExecutorInfo> execList = mkExecutorInfoList(1, 2, 3, 4, 5);
            LocalAssignment newAssignment =
                mkLocalAssignment(topoId, execList, mkWorkerResources(100.0, 100.0, 100.0));

            AsyncLocalizer localizer = mock(AsyncLocalizer.class);
            BlobChangingCallback cb = mock(BlobChangingCallback.class);
            Container container = mock(Container.class);
            LocalState state = mock(LocalState.class);
            ContainerLauncher containerLauncher = mock(ContainerLauncher.class);
            when(containerLauncher.launchContainer(port, newAssignment, state)).thenReturn(container);
            LSWorkerHeartbeat hb = mkWorkerHB(topoId, port, execList, Time.currentTimeSecs());
            when(container.readHeartbeat()).thenReturn(hb, hb);

            @SuppressWarnings("unchecked")
            CompletableFuture<Void> blobFuture = mock(CompletableFuture.class);
            CompletableFuture<Void> secondBlobFuture = mock(CompletableFuture.class);
            when(secondBlobFuture.get(anyLong(), any())).thenThrow(new ExecutionException(new RuntimeException("Localization failure")));
            CompletableFuture<Void> thirdBlobFuture = mock(CompletableFuture.class);
            when(localizer.requestDownloadTopologyBlobs(newAssignment, port, cb))
                .thenReturn(blobFuture)
                .thenReturn(secondBlobFuture)
                .thenReturn(thirdBlobFuture);

            ISupervisor iSuper = mock(ISupervisor.class);
            SlotMetrics slotMetrics = new SlotMetrics(new StormMetricsRegistry());
            StaticState staticState = new StaticState(localizer, 5000, 120000, 1000, 1000,
                                                      containerLauncher, "localhost", port, iSuper, state, cb, null, null, slotMetrics);
            DynamicState dynamicState = new DynamicState(null, null, null, slotMetrics)
                .withNewAssignment(newAssignment);

            DynamicState nextState = Slot.stateMachineStep(dynamicState, staticState);
            verify(localizer).requestDownloadTopologyBlobs(newAssignment, port, cb);
            assertEquals(MachineState.WAITING_FOR_BLOB_LOCALIZATION, nextState.state);
            assertSame("pendingDownload not set properly", blobFuture, nextState.pendingDownload);
            assertEquals(newAssignment, nextState.pendingLocalization);
            assertEquals(0, Time.currentTimeMillis());

            //Assignment has changed
            nextState = Slot.stateMachineStep(nextState.withNewAssignment(null), staticState);
            assertThat(nextState.state, is(MachineState.EMPTY));
            assertThat(nextState.pendingChangingBlobs, is(Collections.emptySet()));
            assertThat(nextState.pendingChangingBlobsAssignment, nullValue());
            assertThat(nextState.pendingLocalization, nullValue());
            assertThat(nextState.pendingDownload, nullValue());
            
            clearInvocations(localizer);
            nextState = Slot.stateMachineStep(dynamicState.withNewAssignment(newAssignment), staticState);
            verify(localizer).requestDownloadTopologyBlobs(newAssignment, port, cb);
            assertEquals(MachineState.WAITING_FOR_BLOB_LOCALIZATION, nextState.state);
            assertSame("pendingDownload not set properly", secondBlobFuture, nextState.pendingDownload);
            assertEquals(newAssignment, nextState.pendingLocalization);
            
            //Error occurs, but assignment has not changed
            clearInvocations(localizer);
            nextState = Slot.stateMachineStep(nextState, staticState);
            verify(localizer).requestDownloadTopologyBlobs(newAssignment, port, cb);
            assertEquals(MachineState.WAITING_FOR_BLOB_LOCALIZATION, nextState.state);
            assertSame("pendingDownload not set properly", thirdBlobFuture, nextState.pendingDownload);
            assertEquals(newAssignment, nextState.pendingLocalization);
            assertThat(Time.currentTimeMillis(), greaterThan(3L));

            nextState = Slot.stateMachineStep(nextState, staticState);
            verify(thirdBlobFuture).get(1000, TimeUnit.MILLISECONDS);
            verify(containerLauncher).launchContainer(port, newAssignment, state);
            assertEquals(MachineState.WAITING_FOR_WORKER_START, nextState.state);
            assertSame("pendingDownload is not null", null, nextState.pendingDownload);
            assertSame(null, nextState.pendingLocalization);
            assertSame(newAssignment, nextState.currentAssignment);
            assertSame(container, nextState.container);
        }
    }

    @Test
    public void testRelaunch() throws Exception {
        try (SimulatedTime t = new SimulatedTime(1010)) {
            int port = 8080;
            String topoId = "CURRENT";
            List<ExecutorInfo> execList = mkExecutorInfoList(1, 2, 3, 4, 5);
            LocalAssignment assignment =
                mkLocalAssignment(topoId, execList, mkWorkerResources(100.0, 100.0, 100.0));

            AsyncLocalizer localizer = mock(AsyncLocalizer.class);
            BlobChangingCallback cb = mock(BlobChangingCallback.class);
            Container container = mock(Container.class);
            ContainerLauncher containerLauncher = mock(ContainerLauncher.class);
            LSWorkerHeartbeat oldhb = mkWorkerHB(topoId, port, execList, Time.currentTimeSecs() - 10);
            LSWorkerHeartbeat goodhb = mkWorkerHB(topoId, port, execList, Time.currentTimeSecs());
            when(container.readHeartbeat()).thenReturn(oldhb, oldhb, goodhb, goodhb);
            when(container.areAllProcessesDead()).thenReturn(false, false, true);

            ISupervisor iSuper = mock(ISupervisor.class);
            LocalState state = mock(LocalState.class);
            SlotMetrics slotMetrics = new SlotMetrics(new StormMetricsRegistry());
            StaticState staticState = new StaticState(localizer, 5000, 120000, 1000, 1000,
                                                      containerLauncher, "localhost", port, iSuper, state, cb, null, null, slotMetrics);
            DynamicState dynamicState = new DynamicState(assignment, container, assignment, slotMetrics);

            DynamicState nextState = Slot.stateMachineStep(dynamicState, staticState);
            assertEquals(MachineState.KILL_AND_RELAUNCH, nextState.state);
            verify(container).kill();
            assertTrue(Time.currentTimeMillis() > 1000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.KILL_AND_RELAUNCH, nextState.state);
            verify(container).forceKill();
            assertTrue(Time.currentTimeMillis() > 2000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.WAITING_FOR_WORKER_START, nextState.state);
            verify(container).relaunch();

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.WAITING_FOR_WORKER_START, nextState.state);
            assertTrue(Time.currentTimeMillis() > 3000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.RUNNING, nextState.state);
        }
    }

    @Test
    public void testReschedule() throws Exception {
        try (SimulatedTime t = new SimulatedTime(1010)) {
            int port = 8080;
            String cTopoId = "CURRENT";
            List<ExecutorInfo> cExecList = mkExecutorInfoList(1, 2, 3, 4, 5);
            LocalAssignment cAssignment =
                mkLocalAssignment(cTopoId, cExecList, mkWorkerResources(100.0, 100.0, 100.0));

            BlobChangingCallback cb = mock(BlobChangingCallback.class);

            Container cContainer = mock(Container.class);
            LSWorkerHeartbeat chb = mkWorkerHB(cTopoId, port, cExecList, Time.currentTimeSecs());
            when(cContainer.readHeartbeat()).thenReturn(chb);
            when(cContainer.areAllProcessesDead()).thenReturn(false, false, true);

            String nTopoId = "NEW";
            List<ExecutorInfo> nExecList = mkExecutorInfoList(1, 2, 3, 4, 5);
            LocalAssignment nAssignment =
                mkLocalAssignment(nTopoId, nExecList, mkWorkerResources(100.0, 100.0, 100.0));

            AsyncLocalizer localizer = mock(AsyncLocalizer.class);
            Container nContainer = mock(Container.class);
            LocalState state = mock(LocalState.class);
            ContainerLauncher containerLauncher = mock(ContainerLauncher.class);
            when(containerLauncher.launchContainer(port, nAssignment, state)).thenReturn(nContainer);
            LSWorkerHeartbeat nhb = mkWorkerHB(nTopoId, 100, nExecList, Time.currentTimeSecs());
            when(nContainer.readHeartbeat()).thenReturn(nhb, nhb);

            @SuppressWarnings("unchecked")
            CompletableFuture<Void> blobFuture = mock(CompletableFuture.class);
            when(localizer.requestDownloadTopologyBlobs(nAssignment, port, cb)).thenReturn(blobFuture);

            ISupervisor iSuper = mock(ISupervisor.class);
            SlotMetrics slotMetrics = new SlotMetrics(new StormMetricsRegistry());
            StaticState staticState = new StaticState(localizer, 5000, 120000, 1000, 1000,
                                                      containerLauncher, "localhost", port, iSuper, state, cb, null, null, slotMetrics);
            DynamicState dynamicState = new DynamicState(cAssignment, cContainer, nAssignment, slotMetrics);

            DynamicState nextState = Slot.stateMachineStep(dynamicState, staticState);
            assertEquals(MachineState.KILL, nextState.state);
            verify(cContainer).kill();
            verify(localizer).requestDownloadTopologyBlobs(nAssignment, port, cb);
            assertSame("pendingDownload not set properly", blobFuture, nextState.pendingDownload);
            assertEquals(nAssignment, nextState.pendingLocalization);
            assertTrue(Time.currentTimeMillis() > 1000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.KILL, nextState.state);
            verify(cContainer).forceKill();
            assertSame("pendingDownload not set properly", blobFuture, nextState.pendingDownload);
            assertEquals(nAssignment, nextState.pendingLocalization);
            assertTrue(Time.currentTimeMillis() > 2000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.WAITING_FOR_BLOB_LOCALIZATION, nextState.state);
            verify(cContainer).cleanUp();
            verify(localizer).releaseSlotFor(cAssignment, port);
            assertTrue(Time.currentTimeMillis() > 2000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            verify(blobFuture).get(1000, TimeUnit.MILLISECONDS);
            verify(containerLauncher).launchContainer(port, nAssignment, state);
            assertEquals(MachineState.WAITING_FOR_WORKER_START, nextState.state);
            assertSame("pendingDownload is not null", null, nextState.pendingDownload);
            assertSame(null, nextState.pendingLocalization);
            assertSame(nAssignment, nextState.currentAssignment);
            assertSame(nContainer, nextState.container);
            assertTrue(Time.currentTimeMillis() > 2000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.RUNNING, nextState.state);
            assertSame("pendingDownload is not null", null, nextState.pendingDownload);
            assertSame(null, nextState.pendingLocalization);
            assertSame(nAssignment, nextState.currentAssignment);
            assertSame(nContainer, nextState.container);
            assertTrue(Time.currentTimeMillis() > 2000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.RUNNING, nextState.state);
            assertSame("pendingDownload is not null", null, nextState.pendingDownload);
            assertSame(null, nextState.pendingLocalization);
            assertSame(nAssignment, nextState.currentAssignment);
            assertSame(nContainer, nextState.container);
            assertTrue(Time.currentTimeMillis() > 3000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.RUNNING, nextState.state);
            assertSame("pendingDownload is not null", null, nextState.pendingDownload);
            assertSame(null, nextState.pendingLocalization);
            assertSame(nAssignment, nextState.currentAssignment);
            assertSame(nContainer, nextState.container);
            assertTrue(Time.currentTimeMillis() > 4000);
        }
    }

    @Test
    public void testRunningToEmpty() throws Exception {
        try (SimulatedTime t = new SimulatedTime(1010)) {
            int port = 8080;
            String cTopoId = "CURRENT";
            List<ExecutorInfo> cExecList = mkExecutorInfoList(1, 2, 3, 4, 5);
            LocalAssignment cAssignment =
                mkLocalAssignment(cTopoId, cExecList, mkWorkerResources(100.0, 100.0, 100.0));

            Container cContainer = mock(Container.class);
            LSWorkerHeartbeat chb = mkWorkerHB(cTopoId, port, cExecList, Time.currentTimeSecs());
            when(cContainer.readHeartbeat()).thenReturn(chb);
            when(cContainer.areAllProcessesDead()).thenReturn(false, false, true);

            AsyncLocalizer localizer = mock(AsyncLocalizer.class);
            BlobChangingCallback cb = mock(BlobChangingCallback.class);
            ContainerLauncher containerLauncher = mock(ContainerLauncher.class);

            ISupervisor iSuper = mock(ISupervisor.class);
            LocalState state = mock(LocalState.class);
            SlotMetrics slotMetrics = new SlotMetrics(new StormMetricsRegistry());
            StaticState staticState = new StaticState(localizer, 5000, 120000, 1000, 1000,
                                                      containerLauncher, "localhost", port, iSuper, state, cb, null, null, slotMetrics);
            DynamicState dynamicState = new DynamicState(cAssignment, cContainer, null, slotMetrics);

            DynamicState nextState = Slot.stateMachineStep(dynamicState, staticState);
            assertEquals(MachineState.KILL, nextState.state);
            verify(cContainer).kill();
            verify(localizer, never()).requestDownloadTopologyBlobs(null, port, cb);
            assertSame("pendingDownload not set properly", null, nextState.pendingDownload);
            assertEquals(null, nextState.pendingLocalization);
            assertTrue(Time.currentTimeMillis() > 1000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.KILL, nextState.state);
            verify(cContainer).forceKill();
            assertSame("pendingDownload not set properly", null, nextState.pendingDownload);
            assertEquals(null, nextState.pendingLocalization);
            assertTrue(Time.currentTimeMillis() > 2000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.EMPTY, nextState.state);
            verify(cContainer).cleanUp();
            verify(localizer).releaseSlotFor(cAssignment, port);
            assertEquals(null, nextState.container);
            assertEquals(null, nextState.currentAssignment);
            assertTrue(Time.currentTimeMillis() > 2000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.EMPTY, nextState.state);
            assertEquals(null, nextState.container);
            assertEquals(null, nextState.currentAssignment);
            assertTrue(Time.currentTimeMillis() > 3000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.EMPTY, nextState.state);
            assertEquals(null, nextState.container);
            assertEquals(null, nextState.currentAssignment);
            assertTrue(Time.currentTimeMillis() > 3000);
        }
    }

    @Test
    public void testRunWithProfileActions() throws Exception {
        try (SimulatedTime t = new SimulatedTime(1010)) {
            int port = 8080;
            String cTopoId = "CURRENT";
            List<ExecutorInfo> cExecList = mkExecutorInfoList(1, 2, 3, 4, 5);
            LocalAssignment cAssignment =
                mkLocalAssignment(cTopoId, cExecList, mkWorkerResources(100.0, 100.0, 100.0));

            Container cContainer = mock(Container.class);
            LSWorkerHeartbeat chb = mkWorkerHB(cTopoId, port, cExecList, Time.currentTimeSecs() + 100); //NOT going to timeout for a while
            when(cContainer.readHeartbeat()).thenReturn(chb, chb, chb, chb, chb, chb);
            when(cContainer.runProfiling(any(ProfileRequest.class), anyBoolean())).thenReturn(true);

            AsyncLocalizer localizer = mock(AsyncLocalizer.class);
            BlobChangingCallback cb = mock(BlobChangingCallback.class);
            ContainerLauncher containerLauncher = mock(ContainerLauncher.class);

            ISupervisor iSuper = mock(ISupervisor.class);
            LocalState state = mock(LocalState.class);
            StaticState staticState = new StaticState(localizer, 5000, 120000, 1000, 1000,
                                                      containerLauncher, "localhost", port, iSuper, state, cb, null, null, new SlotMetrics(new StormMetricsRegistry()));
            Set<TopoProfileAction> profileActions = new HashSet<>();
            ProfileRequest request = new ProfileRequest();
            request.set_action(ProfileAction.JPROFILE_STOP);
            NodeInfo info = new NodeInfo();
            info.set_node("localhost");
            info.add_to_port(port);
            request.set_nodeInfo(info);
            request.set_time_stamp(Time.currentTimeMillis() + 3000);//3 seconds from now

            TopoProfileAction profile = new TopoProfileAction(cTopoId, request);
            profileActions.add(profile);
            Set<TopoProfileAction> expectedPending = new HashSet<>();
            expectedPending.add(profile);

            SlotMetrics slotMetrics = new SlotMetrics(new StormMetricsRegistry());
            DynamicState dynamicState = new DynamicState(cAssignment, cContainer, cAssignment, slotMetrics)
                .withProfileActions(profileActions, Collections.<TopoProfileAction>emptySet());

            DynamicState nextState = Slot.stateMachineStep(dynamicState, staticState);
            assertEquals(MachineState.RUNNING, nextState.state);
            verify(cContainer).runProfiling(request, false);
            assertEquals(expectedPending, nextState.pendingStopProfileActions);
            assertEquals(expectedPending, nextState.profileActions);
            assertTrue(Time.currentTimeMillis() > 1000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.RUNNING, nextState.state);
            assertEquals(expectedPending, nextState.pendingStopProfileActions);
            assertEquals(expectedPending, nextState.profileActions);
            assertTrue(Time.currentTimeMillis() > 2000);


            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.RUNNING, nextState.state);
            assertEquals(expectedPending, nextState.pendingStopProfileActions);
            assertEquals(expectedPending, nextState.profileActions);
            assertTrue(Time.currentTimeMillis() > 3000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.RUNNING, nextState.state);
            verify(cContainer).runProfiling(request, true);
            assertEquals(Collections.<TopoProfileAction>emptySet(), nextState.pendingStopProfileActions);
            assertEquals(Collections.<TopoProfileAction>emptySet(), nextState.profileActions);
            assertTrue(Time.currentTimeMillis() > 4000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.RUNNING, nextState.state);
            assertEquals(Collections.<TopoProfileAction>emptySet(), nextState.pendingStopProfileActions);
            assertEquals(Collections.<TopoProfileAction>emptySet(), nextState.profileActions);
            assertTrue(Time.currentTimeMillis() > 5000);
        }
    }

    @Test
    public void testResourcesChangedFiltered() throws Exception {
        try (SimulatedTime t = new SimulatedTime(1010)) {
            int port = 8080;
            String cTopoId = "CURRENT";
            List<ExecutorInfo> cExecList = mkExecutorInfoList(1, 2, 3, 4, 5);
            LocalAssignment cAssignment =
                mkLocalAssignment(cTopoId, cExecList, mkWorkerResources(100.0, 100.0, 100.0));

            String otherTopoId = "OTHER";
            LocalAssignment otherAssignment = mkLocalAssignment(otherTopoId, cExecList, mkWorkerResources(100.0, 100.0, 100.0));

            BlobChangingCallback cb = mock(BlobChangingCallback.class);

            Container cContainer = mock(Container.class);
            LSWorkerHeartbeat chb = mkWorkerHB(cTopoId, port, cExecList, Time.currentTimeSecs());
            when(cContainer.readHeartbeat()).thenReturn(chb);
            when(cContainer.areAllProcessesDead()).thenReturn(false, false, true);
            AsyncLocalizer localizer = mock(AsyncLocalizer.class);
            Container nContainer = mock(Container.class);
            LocalState state = mock(LocalState.class);
            ContainerLauncher containerLauncher = mock(ContainerLauncher.class);
            when(containerLauncher.launchContainer(port, cAssignment, state)).thenReturn(nContainer);
            when(nContainer.readHeartbeat()).thenReturn(chb, chb);

            ISupervisor iSuper = mock(ISupervisor.class);
            long heartbeatTimeoutMs = 5000;
            StaticState staticState = new StaticState(localizer, heartbeatTimeoutMs, 120_000, 1000, 1000,
                                                      containerLauncher, "localhost", port, iSuper, state, cb, null, null, new SlotMetrics(new StormMetricsRegistry()));

            Set<Slot.BlobChanging> changing = new HashSet<>();

            LocallyCachedBlob stormJar = mock(LocallyCachedBlob.class);
            GoodToGo.GoodToGoLatch stormJarLatch = mock(GoodToGo.GoodToGoLatch.class);
            CompletableFuture<Void> stormJarLatchFuture = mock(CompletableFuture.class);
            when(stormJarLatch.countDown()).thenReturn(stormJarLatchFuture);
            changing.add(new Slot.BlobChanging(cAssignment, stormJar, stormJarLatch));
            Set<Slot.BlobChanging> desired = new HashSet<>(changing);

            LocallyCachedBlob otherJar = mock(LocallyCachedBlob.class);
            GoodToGo.GoodToGoLatch otherJarLatch = mock(GoodToGo.GoodToGoLatch.class);
            changing.add(new Slot.BlobChanging(otherAssignment, otherJar, otherJarLatch));

            SlotMetrics slotMetrics = new SlotMetrics(new StormMetricsRegistry());
            DynamicState dynamicState = new DynamicState(cAssignment, cContainer, cAssignment, slotMetrics).withChangingBlobs(changing);

            DynamicState nextState = Slot.stateMachineStep(dynamicState, staticState);
            assertEquals(MachineState.KILL_BLOB_UPDATE, nextState.state);
            verify(iSuper).killedWorker(port);
            verify(cContainer).kill();
            verify(localizer, never()).requestDownloadTopologyBlobs(any(), anyInt(), any());
            verify(stormJarLatch, never()).countDown();
            verify(otherJarLatch, times(1)).countDown();
            assertNull(nextState.pendingDownload);
            assertNull(nextState.pendingLocalization);
            assertEquals(desired, nextState.changingBlobs);
            assertTrue(nextState.pendingChangingBlobs.isEmpty());
            assertNull(nextState.pendingChangingBlobsAssignment);
            assertThat(Time.currentTimeMillis(), greaterThan(1000L));

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.KILL_BLOB_UPDATE, nextState.state);
            verify(cContainer).forceKill();
            assertNull(nextState.pendingDownload);
            assertNull(nextState.pendingLocalization);
            assertEquals(desired, nextState.changingBlobs);
            assertTrue(nextState.pendingChangingBlobs.isEmpty());
            assertNull(nextState.pendingChangingBlobsAssignment);
            assertThat(Time.currentTimeMillis(), greaterThan(2000L));

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.WAITING_FOR_BLOB_UPDATE, nextState.state);
            verify(cContainer).cleanUp();
            assertThat(Time.currentTimeMillis(), greaterThan(2000L));

            nextState = Slot.stateMachineStep(nextState, staticState);
            verify(stormJarLatchFuture).get(anyLong(), any());
            verify(containerLauncher).launchContainer(port, cAssignment, state);
            assertEquals(MachineState.WAITING_FOR_WORKER_START, nextState.state);
            assertNull(nextState.pendingChangingBlobsAssignment);
            assertTrue(nextState.pendingChangingBlobs.isEmpty());
            assertSame(cAssignment, nextState.currentAssignment);
            assertSame(nContainer, nextState.container);
            assertThat(Time.currentTimeMillis(), greaterThan(2000L));
            assertThat(Time.currentTimeMillis(), lessThan(heartbeatTimeoutMs));

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.RUNNING, nextState.state);
            assertNull(nextState.pendingChangingBlobsAssignment);
            assertTrue(nextState.pendingChangingBlobs.isEmpty());
            assertSame(cAssignment, nextState.currentAssignment);
            assertSame(nContainer, nextState.container);
            assertTrue(Time.currentTimeMillis() > 2000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.RUNNING, nextState.state);
            assertNull(nextState.pendingChangingBlobsAssignment);
            assertTrue(nextState.pendingChangingBlobs.isEmpty());
            assertSame(cAssignment, nextState.currentAssignment);
            assertSame(nContainer, nextState.container);
            assertTrue(Time.currentTimeMillis() > 3000);

            nextState = Slot.stateMachineStep(nextState, staticState);
            assertEquals(MachineState.RUNNING, nextState.state);
            assertNull(nextState.pendingChangingBlobsAssignment);
            assertTrue(nextState.pendingChangingBlobs.isEmpty());
            assertSame(cAssignment, nextState.currentAssignment);
            assertSame(nContainer, nextState.container);
            assertTrue(Time.currentTimeMillis() > 4000);
        }
    }
}
