/*
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

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.scheduler.ISupervisor;
import org.apache.storm.shade.org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReadClusterStateTest {

    private static final String TOPOLOGY_ID = "topo-1";

    @TempDir
    Path localDir;

    private final IStormClusterState clusterState = mock(IStormClusterState.class);
    private final ISupervisor iSupervisor = mock(ISupervisor.class);

    private ReadClusterState newReadClusterState() throws Exception {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_CLUSTER_MODE, "local");
        conf.put(Config.STORM_LOCAL_DIR, localDir.toString());
        conf.put(DaemonConfig.SUPERVISOR_SLOTS_PORTS, Collections.emptyList());

        Supervisor supervisor = mock(Supervisor.class);
        when(supervisor.getConf()).thenReturn(conf);
        when(supervisor.getStormClusterState()).thenReturn(clusterState);
        when(supervisor.getAssignmentId()).thenReturn("supervisor-1");
        when(supervisor.getiSupervisor()).thenReturn(iSupervisor);
        when(supervisor.getHostName()).thenReturn("host-1");
        when(supervisor.getCurrAssignment()).thenReturn(new AtomicReference<>(new HashMap<>()));
        return new ReadClusterState(supervisor);
    }

    @Test
    public void profilerRequestFailureDoesNotHaltAssignmentSync() throws Exception {
        when(clusterState.assignments(null)).thenReturn(Collections.singletonList(TOPOLOGY_ID));
        when(clusterState.assignmentsInfo()).thenReturn(Collections.emptyMap());
        // What ClientZookeeper surfaces after Curator gives up, as in the reported supervisor crash.
        when(clusterState.getTopologyProfileRequests(TOPOLOGY_ID)).thenThrow(new RuntimeException(
            new KeeperException.SessionExpiredException()));

        ReadClusterState readClusterState = newReadClusterState();

        assertDoesNotThrow(readClusterState::run);
        verify(iSupervisor).assigned(any());
    }

    @Test
    public void assignmentReadFailureStillPropagates() throws Exception {
        when(clusterState.assignments(null)).thenThrow(new IllegalStateException("broken local backend"));

        ReadClusterState readClusterState = newReadClusterState();

        assertThrows(RuntimeException.class, readClusterState::run);
        verify(clusterState, never()).getTopologyProfileRequests(anyString());
    }
}
