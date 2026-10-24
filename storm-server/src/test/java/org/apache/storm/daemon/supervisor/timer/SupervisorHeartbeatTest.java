/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file
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

package org.apache.storm.daemon.supervisor.timer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.scheduler.ISupervisor;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SupervisorHeartbeatTest {

    private static final String SUPERVISOR_ID = "supervisor-1";

    private Supervisor mockSupervisor(IStormClusterState clusterState) {
        Supervisor supervisor = mock(Supervisor.class);
        ISupervisor iSupervisor = mock(ISupervisor.class);
        when(iSupervisor.getMetadata()).thenReturn(Arrays.asList(6700, 6701));

        when(supervisor.getStormClusterState()).thenReturn(clusterState);
        when(supervisor.getId()).thenReturn(SUPERVISOR_ID);
        when(supervisor.getiSupervisor()).thenReturn(iSupervisor);
        when(supervisor.getCurrAssignment()).thenReturn(new AtomicReference<>(new HashMap<>()));
        when(supervisor.getHostName()).thenReturn("host-1");
        when(supervisor.getAssignmentId()).thenReturn(SUPERVISOR_ID);
        when(supervisor.getThriftServerPort()).thenReturn(6627);
        when(supervisor.getUpTime()).thenReturn(Utils.makeUptimeComputer());
        when(supervisor.getStormVersion()).thenReturn("test-version");
        return supervisor;
    }

    private Map<String, Object> baseConf() {
        return new HashMap<>(Utils.readStormConfig());
    }

    @Test
    public void transientHeartbeatFailureIsSwallowedInsteadOfKillingTheProcess() {
        IStormClusterState clusterState = mock(IStormClusterState.class);
        doThrow(new RuntimeException("simulated transient ZK failure"))
            .when(clusterState).supervisorHeartbeat(anyString(), any(SupervisorInfo.class));

        Supervisor supervisor = mockSupervisor(clusterState);
        SupervisorHeartbeat heartbeat = new SupervisorHeartbeat(baseConf(), supervisor);

        assertDoesNotThrow(heartbeat::run);
        verify(clusterState, atLeastOnce()).supervisorHeartbeat(anyString(), any(SupervisorInfo.class));
    }

    @Test
    public void successfulCycleSendsTheHeartbeat() {
        IStormClusterState clusterState = mock(IStormClusterState.class);
        Supervisor supervisor = mockSupervisor(clusterState);
        SupervisorHeartbeat heartbeat = new SupervisorHeartbeat(baseConf(), supervisor);

        assertDoesNotThrow(heartbeat::run);
        verify(clusterState).supervisorHeartbeat(eq(SUPERVISOR_ID), any(SupervisorInfo.class));
    }
}
