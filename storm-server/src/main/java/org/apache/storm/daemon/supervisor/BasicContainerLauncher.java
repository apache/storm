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
import org.apache.storm.container.ResourceIsolationInterface;
import org.apache.storm.daemon.supervisor.Container.ContainerType;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.utils.LocalState;

/**
 * Launch containers with no security using standard java commands.
 */
public class BasicContainerLauncher extends ContainerLauncher {
    protected final ResourceIsolationInterface resourceIsolationManager;
    private final Map<String, Object> conf;
    private final String supervisorId;
    private final int supervisorPort;
    private final StormMetricsRegistry metricsRegistry;
    private final ContainerMemoryTracker containerMemoryTracker;

    public BasicContainerLauncher(Map<String, Object> conf, String supervisorId, int supervisorPort,
                                  ResourceIsolationInterface resourceIsolationManager, StormMetricsRegistry metricsRegistry,
                                  ContainerMemoryTracker containerMemoryTracker) throws IOException {
        this.conf = conf;
        this.supervisorId = supervisorId;
        this.supervisorPort = supervisorPort;
        this.resourceIsolationManager = resourceIsolationManager;
        this.metricsRegistry = metricsRegistry;
        this.containerMemoryTracker = containerMemoryTracker;
    }

    @Override
    public Container launchContainer(int port, LocalAssignment assignment, LocalState state) throws IOException {
        Container container = new BasicContainer(ContainerType.LAUNCH, conf, supervisorId, supervisorPort, port,
            assignment, resourceIsolationManager, state, null, metricsRegistry,
            containerMemoryTracker);
        container.setup();
        container.launch();
        return container;
    }

    @Override
    public Container recoverContainer(int port, LocalAssignment assignment, LocalState state) throws IOException {
        return new BasicContainer(ContainerType.RECOVER_FULL, conf, supervisorId, supervisorPort, port, assignment,
                resourceIsolationManager, state, null, metricsRegistry, containerMemoryTracker);
    }

    @Override
    public Killable recoverContainer(String workerId, LocalState localState) throws IOException {
        return new BasicContainer(ContainerType.RECOVER_PARTIAL, conf, supervisorId, supervisorPort, -1, null,
                resourceIsolationManager, localState, workerId, metricsRegistry, containerMemoryTracker);
    }
}
