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

import java.io.IOException;
import java.util.Map;
import org.apache.storm.DaemonConfig;
import org.apache.storm.container.DefaultResourceIsolationManager;
import org.apache.storm.container.ResourceIsolationInterface;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.messaging.IContext;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launches containers.
 */
public abstract class ContainerLauncher {
    private static final Logger LOG = LoggerFactory.getLogger(ContainerLauncher.class);

    protected ContainerLauncher() {
        //Empty
    }

    /**
     * Factory to create the right container launcher
     * for the config and the environment.
     * @param conf the config
     * @param supervisorId the ID of the supervisor
     * @param supervisorPort the parent supervisor thrift server port
     * @param sharedContext Used in local mode to let workers talk together without netty
     * @param metricsRegistry The metrics registry.
     * @param containerMemoryTracker The shared memory tracker for the supervisor's containers
     * @param localSupervisor The local supervisor Thrift interface. Only used for local clusters, distributed clusters use Thrift directly.
     * @return the proper container launcher
     * @throws IOException on any error
     */
    public static ContainerLauncher make(Map<String, Object> conf, String supervisorId, int supervisorPort,
                                         IContext sharedContext, StormMetricsRegistry metricsRegistry, 
                                         ContainerMemoryTracker containerMemoryTracker,
                                         org.apache.storm.generated.Supervisor.Iface localSupervisor) throws IOException {
        if (ConfigUtils.isLocalMode(conf)) {
            return new LocalContainerLauncher(conf, supervisorId, supervisorPort, sharedContext, metricsRegistry, containerMemoryTracker,
                localSupervisor);
        }

        ResourceIsolationInterface resourceIsolationManager;
        if (ObjectReader.getBoolean(conf.get(DaemonConfig.STORM_RESOURCE_ISOLATION_PLUGIN_ENABLE), false)) {
            resourceIsolationManager = ReflectionUtils.newInstance((String) conf.get(DaemonConfig.STORM_RESOURCE_ISOLATION_PLUGIN));
            LOG.info("Using resource isolation plugin {}: {}", conf.get(DaemonConfig.STORM_RESOURCE_ISOLATION_PLUGIN),
                resourceIsolationManager);
        } else {
            resourceIsolationManager = new DefaultResourceIsolationManager();
            LOG.info("{} is false. Using default resource isolation plugin: {}", DaemonConfig.STORM_RESOURCE_ISOLATION_PLUGIN_ENABLE,
                resourceIsolationManager);
        }

        resourceIsolationManager.prepare(conf);

        return new BasicContainerLauncher(conf, supervisorId, supervisorPort, resourceIsolationManager, metricsRegistry, 
            containerMemoryTracker);
    }

    /**
     * Launch a container in a given slot.
     * @param port the port to run this on
     * @param assignment what to launch
     * @param state the current state of the supervisor
     * @return The container that can be used to manager the processes.
     * @throws IOException on any error 
     */
    public abstract Container launchContainer(int port, LocalAssignment assignment, LocalState state) throws IOException;

    /**
     * Recover a container for a running process.
     * @param port the port the assignment is running on
     * @param assignment the assignment that was launched
     * @param state the current state of the supervisor
     * @return The container that can be used to manage the processes.
     * @throws IOException on any error
     * @throws ContainerRecoveryException if the Container could not be recovered
     */
    public abstract Container recoverContainer(int port, LocalAssignment assignment, LocalState state) throws IOException,
        ContainerRecoveryException;

    /**
     * Try to recover a container using just the worker ID.  
     * The result is really only useful for killing the container
     * and so is returning a Killable.  Even if a Container is returned
     * do not case the result to Container because only the Killable APIs
     * are guaranteed to work. 
     * @param workerId the id of the worker to use
     * @param localState the state of the running supervisor
     * @return a Killable that can be used to kill the underlying container.
     * @throws IOException on any error
     * @throws ContainerRecoveryException if the Container could not be recovered
     */
    public abstract Killable recoverContainer(String workerId, LocalState localState) throws IOException, ContainerRecoveryException;
}
