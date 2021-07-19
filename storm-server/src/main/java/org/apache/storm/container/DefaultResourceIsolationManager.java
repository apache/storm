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

package org.apache.storm.container;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.daemon.supervisor.ClientSupervisorUtils;
import org.apache.storm.daemon.supervisor.ExitCodeCallback;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the default class to manage worker processes, including launching, killing, profiling and etc.
 */
public class DefaultResourceIsolationManager implements ResourceIsolationInterface {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultResourceIsolationManager.class);
    protected Map<String, Object> conf;
    protected boolean runAsUser;

    @Override
    public void prepare(Map<String, Object> conf) throws IOException {
        this.conf = conf;
        runAsUser = ObjectReader.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false);
    }

    @Override
    public void reserveResourcesForWorker(String workerId, Integer workerMemory, Integer workerCpu, String numaId) {
        //NO OP
    }

    @Override
    public void cleanup(String user, String workerId, int port) throws IOException {
        //NO OP
    }

    @Override
    public void launchWorkerProcess(String user, String topologyId,  Map<String, Object> topoConf,
                                    int port, String workerId, List<String> command, Map<String, String> env,
                                    String logPrefix, ExitCodeCallback processExitCallback, File targetDir) throws IOException {
        if (runAsUser) {
            String workerDir = targetDir.getAbsolutePath();
            List<String> args = Arrays.asList("worker", workerDir, ServerUtils.writeScript(workerDir, command, env));
            ClientSupervisorUtils.processLauncher(
                conf, user, null, args, null, logPrefix,
                processExitCallback, targetDir
            );
        } else {
            ClientSupervisorUtils.launchProcess(command, env, logPrefix, processExitCallback, targetDir);
        }
    }

    @Override
    public long getMemoryUsage(String user, String workerId, int port) throws IOException {
        return 0;
    }

    @Override
    public long getSystemFreeMemoryMb() throws IOException {
        return 0;
    }

    @Override
    public void kill(String user, String workerId) throws IOException {
        Set<Long> pids = getAllPids(workerId);
        for (Long pid : pids) {
            kill(pid, user);
        }
    }

    /**
     * Kill a given process.
     * @param pid the id of the process to kill
     * @throws IOException on I/O exception
     */
    private void kill(long pid, String user) throws IOException {
        if (runAsUser) {
            signal(pid, 15, user);
        } else {
            ServerUtils.killProcessWithSigTerm(String.valueOf(pid));
        }
    }

    @Override
    public void forceKill(String user, String workerId) throws IOException {
        Set<Long> pids = getAllPids(workerId);
        for (Long pid : pids) {
            forceKill(pid, user);
        }
    }

    /**
     * Kill a given process forcefully.
     * @param pid the id of the process to kill
     * @throws IOException on I/O exception
     */
    private void forceKill(long pid, String user) throws IOException {
        if (runAsUser) {
            signal(pid, 9, user);
        } else {
            ServerUtils.forceKillProcess(String.valueOf(pid));
        }
    }

    /**
     * Get all the pids that are a part of the container.
     * @return all of the pids that are a part of this container
     */
    protected Set<Long> getAllPids(String workerId) throws IOException {
        Set<Long> ret = new HashSet<>();
        for (String listing : ConfigUtils.readDirContents(ConfigUtils.workerPidsRoot(conf, workerId))) {
            ret.add(Long.valueOf(listing));
        }
        return ret;
    }

    private void signal(long pid, int signal, String user) throws IOException {
        List<String> commands = Arrays.asList("signal", String.valueOf(pid), String.valueOf(signal));
        String logPrefix = "kill -" + signal + " " + pid;
        ClientSupervisorUtils.processLauncherAndWait(conf, user, commands, null, logPrefix);
    }

    @Override
    public boolean areAllProcessesDead(String user, String workerId) throws IOException {
        Set<Long> pids = getAllPids(workerId);
        return ServerUtils.areAllProcessesDead(conf, user, workerId, pids);
    }

    @Override
    public boolean runProfilingCommand(String user, String workerId, List<String> command, Map<String, String> env,
                                       String logPrefix, File targetDir) throws IOException, InterruptedException {
        if (runAsUser) {
            String td = targetDir.getAbsolutePath();
            LOG.info("Running as user: {} command: {}", user, command);
            String containerFile = ServerUtils.containerFilePath(td);
            if (Utils.checkFileExists(containerFile)) {
                SupervisorUtils.rmrAsUser(conf, containerFile, containerFile);
            }
            String scriptFile = ServerUtils.scriptFilePath(td);
            if (Utils.checkFileExists(scriptFile)) {
                SupervisorUtils.rmrAsUser(conf, scriptFile, scriptFile);
            }
            String script = ServerUtils.writeScript(td, command, env);
            List<String> args = Arrays.asList("profiler", td, script);
            int ret = ClientSupervisorUtils.processLauncherAndWait(conf, user, args, env, logPrefix);
            return ret == 0;
        } else {
            Process p = ClientSupervisorUtils.launchProcess(command, env, logPrefix, null, targetDir);
            int ret = p.waitFor();
            return ret == 0;
        }
    }

    /**
     * This class doesn't really manage resources.
     * @return false
     */
    @Override
    public boolean isResourceManaged() {
        return false;
    }
}
