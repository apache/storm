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

import com.codahale.metrics.Meter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.storm.Config;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ShellUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientSupervisorUtils {
    //Worker launched through external commands, hence we count their exceptions toward shell exceptions
    public static final Meter numWorkerLaunchExceptions = ShellUtils.numShellExceptions;

    private static final Logger LOG = LoggerFactory.getLogger(ClientSupervisorUtils.class);

    static boolean doRequiredTopoFilesExist(Map<String, Object> conf, String stormId) throws IOException {
        String stormroot = ConfigUtils.supervisorStormDistRoot(conf, stormId);
        String stormcodepath = ConfigUtils.supervisorStormCodePath(stormroot);
        String stormconfpath = ConfigUtils.supervisorStormConfPath(stormroot);
        if (!Utils.checkFileExists(stormroot)) {
            return false;
        }
        if (!Utils.checkFileExists(stormcodepath)) {
            return false;
        }
        if (!Utils.checkFileExists(stormconfpath)) {
            return false;
        }
        String stormjarpath = ConfigUtils.supervisorStormJarPath(stormroot);
        if (ConfigUtils.isLocalMode(conf) || Utils.checkFileExists(stormjarpath)) {
            return true;
        }
        return false;
    }

    public static int processLauncherAndWait(Map<String, Object> conf, String user, List<String> args,
                                             final Map<String, String> environment, final String logPreFix) throws IOException {
        return processLauncherAndWait(conf, user, args, environment, logPreFix, null);
    }

    public static int processLauncherAndWait(Map<String, Object> conf, String user, List<String> args,
                                             final Map<String, String> environment, final String logPreFix, File dir)
        throws IOException {
        int ret = 0;
        Process process = processLauncher(conf, user, null, args, environment, null, null, dir);
        if (StringUtils.isNotBlank(logPreFix)) {
            Utils.readAndLogStream(logPreFix, process.getInputStream());
        }
        try {
            process.waitFor();
        } catch (InterruptedException e) {
            LOG.warn("{} interrupted.", logPreFix);
            Thread.currentThread().interrupt();
            process.destroy();
            throw new IOException(logPreFix + " interrupted", e);
        }
        ret = process.exitValue();
        return ret;
    }

    public static Process processLauncher(Map<String, Object> conf, String user, List<String> commandPrefix, List<String> args,
                                   Map<String, String> environment, final String logPreFix,
                                   final ExitCodeCallback exitCodeCallback, File dir) throws IOException {
        if (StringUtils.isBlank(user)) {
            throw new IllegalArgumentException("User cannot be blank when calling processLauncher.");
        }
        String wlinitial = (String) (conf.get(Config.SUPERVISOR_WORKER_LAUNCHER));
        String stormHome = ConfigUtils.concatIfNotNull(System.getProperty(ConfigUtils.STORM_HOME));
        String wl;
        if (StringUtils.isNotBlank(wlinitial)) {
            wl = wlinitial;
        } else {
            wl = stormHome + "/bin/worker-launcher";
        }
        List<String> commands = new ArrayList<>();
        if (commandPrefix != null) {
            commands.addAll(commandPrefix);
        }
        commands.add(wl);
        commands.add(user);
        commands.addAll(args);
        LOG.info("Running as user: {} command: {}", user, commands);
        return launchProcess(commands, environment, logPreFix, exitCodeCallback, dir);
    }

    /**
     * Launch a new process as per {@link ProcessBuilder} with a given callback.
     *
     * @param command          the command to be executed in the new process
     * @param environment      the environment to be applied to the process. Can be null.
     * @param logPrefix        a prefix for log entries from the output of the process. Can be null.
     * @param exitCodeCallback code to be called passing the exit code value when the process completes
     * @param dir              the working directory of the new process
     * @return the new process
     */
    public static Process launchProcess(List<String> command,
                                        Map<String, String> environment,
                                        final String logPrefix,
                                        final ExitCodeCallback exitCodeCallback,
                                        File dir)
        throws IOException {
        ProcessBuilder builder = new ProcessBuilder(command);
        Map<String, String> procEnv = builder.environment();
        if (dir != null) {
            builder.directory(dir);
        }
        builder.redirectErrorStream(true);
        if (environment != null) {
            procEnv.putAll(environment);
        }
        final Process process;
        try {
            process = builder.start();
        } catch (IOException e) {
            numWorkerLaunchExceptions.mark();
            throw e;
        }
        if (logPrefix != null || exitCodeCallback != null) {
            Utils.asyncLoop(new Callable<Long>() {
                @Override
                public Long call() {
                    if (logPrefix != null) {
                        Utils.readAndLogStream(logPrefix,
                                               process.getInputStream());
                    }
                    if (exitCodeCallback != null) {
                        try {
                            process.waitFor();
                            exitCodeCallback.call(process.exitValue());
                        } catch (InterruptedException ie) {
                            LOG.info("{} interrupted", logPrefix);
                            exitCodeCallback.call(-1);
                        }
                    }
                    return null; // Run only once.
                }
            });
        }
        return process;
    }

    public static void setupStormCodeDir(Map<String, Object> conf, String user, String dir) throws IOException {
        if (ObjectReader.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false)) {
            String logPrefix = "Storm Code Dir Setup for " + dir;
            List<String> commands = new ArrayList<>();
            commands.add("code-dir");
            commands.add(dir);
            processLauncherAndWait(conf, user, commands, null, logPrefix);
        }
    }

    public static void setupWorkerArtifactsDir(Map<String, Object> conf, String user, String dir) throws IOException {
        if (ObjectReader.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false)) {
            String logPrefix = "Worker Artifacts Setup for " + dir;
            List<String> commands = new ArrayList<>();
            commands.add("artifacts-dir");
            commands.add(dir);
            processLauncherAndWait(conf, user, commands, null, logPrefix);
        }
    }
}
