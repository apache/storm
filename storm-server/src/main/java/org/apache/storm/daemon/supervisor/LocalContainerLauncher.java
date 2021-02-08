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

import java.io.IOException;
import java.util.Map;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.messaging.IContext;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.utils.LocalState;

/**
 * Launch Containers in local mode.
 */
public class LocalContainerLauncher extends ContainerLauncher {
    private final Map<String, Object> conf;
    private final String supervisorId;
    private final int supervisorPort;
    private final IContext sharedContext;
    private final StormMetricsRegistry metricsRegistry;
    private final ContainerMemoryTracker containerMemoryTracker;
    private final org.apache.storm.generated.Supervisor.Iface localSupervisor;

    public LocalContainerLauncher(Map<String, Object> conf, String supervisorId, int supervisorPort,
                                  IContext sharedContext, StormMetricsRegistry metricsRegistry, 
                                  ContainerMemoryTracker containerMemoryTracker,
                                  org.apache.storm.generated.Supervisor.Iface localSupervisor) {
        this.conf = conf;
        this.supervisorId = supervisorId;
        this.supervisorPort = supervisorPort;
        this.sharedContext = sharedContext;
        this.metricsRegistry = metricsRegistry;
        this.containerMemoryTracker = containerMemoryTracker;
        this.localSupervisor = localSupervisor;
    }

    @Override
    public Container launchContainer(int port, LocalAssignment assignment, LocalState state) throws IOException {
        LocalContainer ret = new LocalContainer(conf, supervisorId, supervisorPort,
            port, assignment, sharedContext, metricsRegistry, containerMemoryTracker, localSupervisor);
        ret.setup();
        ret.launch();
        return ret;
    }

    @Override
    public Container recoverContainer(int port, LocalAssignment assignment, LocalState state) throws IOException {
        //We are in the same process we cannot recover anything
        throw new ContainerRecoveryException("Local Mode Recovery is not supported");
    }

    @Override
    public Killable recoverContainer(String workerId, LocalState localState) throws IOException {
        //We are in the same process we cannot recover anything
        throw new ContainerRecoveryException("Local Mode Recovery is not supported");
    }
}
