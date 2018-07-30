/*
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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.storm.container.ResourceIsolationInterface;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunAsUserContainer extends BasicContainer {
    private static final Logger LOG = LoggerFactory.getLogger(RunAsUserContainer.class);

    public RunAsUserContainer(Container.ContainerType type, Map<String, Object> conf, String supervisorId,
                              int supervisorPort, int port, LocalAssignment assignment,
                              ResourceIsolationInterface resourceIsolationManager, LocalState localState,
                              String workerId, StormMetricsRegistry metricsRegistry, 
                              ContainerMemoryTracker containerMemoryTracker) throws IOException {
        this(type, conf, supervisorId, supervisorPort, port, assignment, resourceIsolationManager, localState, workerId, metricsRegistry,
            containerMemoryTracker, null, null, null);
    }

    RunAsUserContainer(Container.ContainerType type, Map<String, Object> conf, String supervisorId, int supervisorPort,
                       int port, LocalAssignment assignment, ResourceIsolationInterface resourceIsolationManager,
                       LocalState localState, String workerId, StormMetricsRegistry metricsRegistry,
                       ContainerMemoryTracker containerMemoryTracker, Map<String, Object> topoConf,
                       AdvancedFSOps ops, String profileCmd) throws IOException {
        super(type, conf, supervisorId, supervisorPort, port, assignment, resourceIsolationManager, localState,
              workerId, metricsRegistry, containerMemoryTracker, topoConf, ops, profileCmd);
        if (Utils.isOnWindows()) {
            throw new UnsupportedOperationException("ERROR: Windows doesn't support running workers as different users yet");
        }
    }

    private void signal(long pid, int signal) throws IOException {
        List<String> commands = Arrays.asList("signal", String.valueOf(pid), String.valueOf(signal));
        String user = getWorkerUser();
        String logPrefix = "kill -" + signal + " " + pid;
        ClientSupervisorUtils.processLauncherAndWait(_conf, user, commands, null, logPrefix);
    }

    @Override
    protected void kill(long pid) throws IOException {
        signal(pid, 15);
    }

    @Override
    protected void forceKill(long pid) throws IOException {
        signal(pid, 9);
    }

    @Override
    protected boolean runProfilingCommand(List<String> command, Map<String, String> env, String logPrefix, File targetDir) throws
        IOException, InterruptedException {
        String user = this.getWorkerUser();
        String td = targetDir.getAbsolutePath();
        LOG.info("Running as user: {} command: {}", user, command);
        String containerFile = ServerUtils.containerFilePath(td);
        if (Utils.checkFileExists(containerFile)) {
            SupervisorUtils.rmrAsUser(_conf, containerFile, containerFile);
        }
        String scriptFile = ServerUtils.scriptFilePath(td);
        if (Utils.checkFileExists(scriptFile)) {
            SupervisorUtils.rmrAsUser(_conf, scriptFile, scriptFile);
        }
        String script = ServerUtils.writeScript(td, command, env);
        List<String> args = Arrays.asList("profiler", td, script);
        int ret = ClientSupervisorUtils.processLauncherAndWait(_conf, user, args, env, logPrefix);
        return ret == 0;
    }

    @Override
    protected void launchWorkerProcess(List<String> command, Map<String, String> env,
                                       String logPrefix, ExitCodeCallback processExitCallback, File targetDir) throws IOException {
        String workerDir = targetDir.getAbsolutePath();
        String user = this.getWorkerUser();
        List<String> args = Arrays.asList("worker", workerDir, ServerUtils.writeScript(workerDir, command, env));
        List<String> commandPrefix = null;
        if (_resourceIsolationManager != null) {
            commandPrefix = _resourceIsolationManager.getLaunchCommandPrefix(_workerId);
        }
        ClientSupervisorUtils.processLauncher(_conf, user, commandPrefix, args, null, logPrefix, processExitCallback, targetDir);
    }

    /**
     * If 'supervisor.run.worker.as.user' is set, worker will be launched as the user that launched the topology.
     */
    @Override
    protected String getRunWorkerAsUser() {
        try {
            return getWorkerUser();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
