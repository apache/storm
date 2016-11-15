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

import java.io.IOException;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.messaging.IContext;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launches containers
 */
public abstract class ContainerLauncher {
    private static final Logger LOG = LoggerFactory.getLogger(ContainerLauncher.class);
    
    /**
     * Factory to create the right container launcher 
     * for the config and the environment.
     * @param conf the config
     * @param supervisorId the ID of the supervisor
     * @param sharedContext Used in local mode to let workers talk together without netty
     * @return the proper container launcher
     * @throws IOException on any error
     */
    public static ContainerLauncher make(Map<String, Object> conf, String supervisorId, IContext sharedContext) throws IOException {
        if (ConfigUtils.isLocalMode(conf)) {
            return new LocalContainerLauncher(conf, supervisorId, sharedContext);
        }
        
        if (Utils.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false)) {
            return new RunAsUserContainerLauncher(conf, supervisorId);
        }
        return new BasicContainerLauncher(conf, supervisorId);
    }
    
    protected ContainerLauncher() {
        //Empty
    }

    /**
     * Launch a container in a given slot
     * @param port the port to run this on
     * @param assignment what to launch
     * @param state the current state of the supervisor
     * @return The container that can be used to manager the processes.
     * @throws IOException on any error 
     */
    public abstract Container launchContainer(int port, LocalAssignment assignment, LocalState state) throws IOException;
    
    /**
     * Recover a container for a running process
     * @param port the port the assignment is running on
     * @param assignment the assignment that was launched
     * @param state the current state of the supervisor
     * @return The container that can be used to manage the processes.
     * @throws IOException on any error
     * @throws ContainerRecoveryException if the Container could not be recovered
     */
    public abstract Container recoverContainer(int port, LocalAssignment assignment, LocalState state) throws IOException, ContainerRecoveryException;
    
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
