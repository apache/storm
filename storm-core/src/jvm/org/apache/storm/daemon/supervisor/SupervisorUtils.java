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

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.localizer.LocalResource;
import org.apache.storm.localizer.Localizer;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Utils;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

public class SupervisorUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SupervisorUtils.class);

    private static final SupervisorUtils INSTANCE = new SupervisorUtils();
    private static SupervisorUtils _instance = INSTANCE;
    public static void setInstance(SupervisorUtils u) {
        _instance = u;
    }
    public static void resetInstance() {
        _instance = INSTANCE;
    }

    static Process processLauncher(Map<String, Object> conf, String user, List<String> commandPrefix, List<String> args, Map<String, String> environment, final String logPreFix,
                                          final ExitCodeCallback exitCodeCallback, File dir) throws IOException {
        if (StringUtils.isBlank(user)) {
            throw new IllegalArgumentException("User cannot be blank when calling processLauncher.");
        }
        String wlinitial = (String) (conf.get(Config.SUPERVISOR_WORKER_LAUNCHER));
        String stormHome = ConfigUtils.concatIfNotNull(System.getProperty("storm.home"));
        String wl;
        if (StringUtils.isNotBlank(wlinitial)) {
            wl = wlinitial;
        } else {
            wl = stormHome + "/bin/worker-launcher";
        }
        List<String> commands = new ArrayList<>();
        if (commandPrefix != null){
            commands.addAll(commandPrefix);
        }
        commands.add(wl);
        commands.add(user);
        commands.addAll(args);
        LOG.info("Running as user: {} command: {}", user, commands);
        return SupervisorUtils.launchProcess(commands, environment, logPreFix, exitCodeCallback, dir);
    }

    public static int processLauncherAndWait(Map<String, Object> conf, String user, List<String> args, final Map<String, String> environment, final String logPreFix)
            throws IOException {
        int ret = 0;
        Process process = processLauncher(conf, user, null, args, environment, logPreFix, null, null);
        if (StringUtils.isNotBlank(logPreFix))
            Utils.readAndLogStream(logPreFix, process.getInputStream());
        try {
            process.waitFor();
        } catch (InterruptedException e) {
            LOG.info("{} interrupted.", logPreFix);
        }
        ret = process.exitValue();
        return ret;
    }

    public static void setupStormCodeDir(Map<String, Object> conf, Map<String, Object> stormConf, String dir) throws IOException {
        if (Utils.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false)) {
            String logPrefix = "Storm Code Dir Setup for " + dir;
            List<String> commands = new ArrayList<>();
            commands.add("code-dir");
            commands.add(dir);
            processLauncherAndWait(conf, (String) (stormConf.get(Config.TOPOLOGY_SUBMITTER_USER)), commands, null, logPrefix);
        }
    }

    public static void setupWorkerArtifactsDir(Map<String, Object> conf, Map<String, Object> stormConf, String dir) throws IOException {
        if (Utils.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false)) {
            String logPrefix = "Worker Artifacts Setup for " + dir;
            List<String> commands = new ArrayList<>();
            commands.add("artifacts-dir");
            commands.add(dir);
            processLauncherAndWait(conf, (String) (stormConf.get(Config.TOPOLOGY_SUBMITTER_USER)), commands, null, logPrefix);
        }
    }

    public static void rmrAsUser(Map<String, Object> conf, String id, String path) throws IOException {
        String user = Utils.getFileOwner(path);
        String logPreFix = "rmr " + id;
        List<String> commands = new ArrayList<>();
        commands.add("rmr");
        commands.add(path);
        SupervisorUtils.processLauncherAndWait(conf, user, commands, null, logPreFix);
        if (Utils.checkFileExists(path)) {
            throw new RuntimeException(path + " was not deleted.");
        }
    }

    /**
     * Given the blob information returns the value of the uncompress field, handling it either being a string or a boolean value, or if it's not specified then
     * returns false
     * 
     * @param blobInfo
     * @return
     */
    public static Boolean shouldUncompressBlob(Map<String, Object> blobInfo) {
        return Utils.getBoolean(blobInfo.get("uncompress"), false);
    }

    /**
     * Returns a list of LocalResources based on the blobstore-map passed in
     * 
     * @param blobstoreMap
     * @return
     */
    public static List<LocalResource> blobstoreMapToLocalresources(Map<String, Map<String, Object>> blobstoreMap) {
        List<LocalResource> localResourceList = new ArrayList<>();
        if (blobstoreMap != null) {
            for (Map.Entry<String, Map<String, Object>> map : blobstoreMap.entrySet()) {
                LocalResource localResource = new LocalResource(map.getKey(), shouldUncompressBlob(map.getValue()));
                localResourceList.add(localResource);
            }
        }
        return localResourceList;
    }

    /**
     * For each of the downloaded topologies, adds references to the blobs that the topologies are using. This is used to reconstruct the cache on restart.
     * 
     * @param localizer
     * @param stormId
     * @param conf
     */
    static void addBlobReferences(Localizer localizer, String stormId, Map<String, Object> conf) throws IOException {
        Map<String, Object> stormConf = ConfigUtils.readSupervisorStormConf(conf, stormId);
        Map<String, Map<String, Object>> blobstoreMap = (Map<String, Map<String, Object>>) stormConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
        String user = (String) stormConf.get(Config.TOPOLOGY_SUBMITTER_USER);
        String topoName = (String) stormConf.get(Config.TOPOLOGY_NAME);
        List<LocalResource> localresources = SupervisorUtils.blobstoreMapToLocalresources(blobstoreMap);
        if (blobstoreMap != null) {
            localizer.addReferences(localresources, user, topoName);
        }
    }

    public static Set<String> readDownloadedTopologyIds(Map<String, Object> conf) throws IOException {
        Set<String> stormIds = new HashSet<>();
        String path = ConfigUtils.supervisorStormDistRoot(conf);
        Collection<String> rets = Utils.readDirContents(path);
        for (String ret : rets) {
            stormIds.add(URLDecoder.decode(ret));
        }
        return stormIds;
    }

    public static Collection<String> supervisorWorkerIds(Map<String, Object> conf) {
        String workerRoot = ConfigUtils.workerRoot(conf);
        return Utils.readDirContents(workerRoot);
    }

    static boolean doRequiredTopoFilesExist(Map<String, Object> conf, String stormId) throws IOException {
        String stormroot = ConfigUtils.supervisorStormDistRoot(conf, stormId);
        String stormjarpath = ConfigUtils.supervisorStormJarPath(stormroot);
        String stormcodepath = ConfigUtils.supervisorStormCodePath(stormroot);
        String stormconfpath = ConfigUtils.supervisorStormConfPath(stormroot);
        if (!Utils.checkFileExists(stormroot))
            return false;
        if (!Utils.checkFileExists(stormcodepath))
            return false;
        if (!Utils.checkFileExists(stormconfpath))
            return false;
        if (ConfigUtils.isLocalMode(conf) || Utils.checkFileExists(stormjarpath))
            return true;
        return false;
    }

    /**
     * map from worker id to heartbeat
     *
     * @param conf
     * @return
     * @throws Exception
     */
    public static Map<String, LSWorkerHeartbeat> readWorkerHeartbeats(Map<String, Object> conf) throws Exception {
        return _instance.readWorkerHeartbeatsImpl(conf);
    }

    public Map<String, LSWorkerHeartbeat> readWorkerHeartbeatsImpl(Map<String, Object> conf) throws Exception {
        Map<String, LSWorkerHeartbeat> workerHeartbeats = new HashMap<>();

        Collection<String> workerIds = SupervisorUtils.supervisorWorkerIds(conf);

        for (String workerId : workerIds) {
            LSWorkerHeartbeat whb = readWorkerHeartbeat(conf, workerId);
            // ATTENTION: whb can be null
            workerHeartbeats.put(workerId, whb);
        }
        return workerHeartbeats;
    }


    /**
     * get worker heartbeat by workerId
     *
     * @param conf
     * @param workerId
     * @return
     * @throws IOException
     */
    private static LSWorkerHeartbeat readWorkerHeartbeat(Map<String, Object> conf, String workerId) {
        return _instance.readWorkerHeartbeatImpl(conf, workerId);
    }

    protected LSWorkerHeartbeat readWorkerHeartbeatImpl(Map<String, Object> conf, String workerId) {
        try {
            LocalState localState = ConfigUtils.workerState(conf, workerId);
            return localState.getWorkerHeartBeat();
        } catch (Exception e) {
            LOG.warn("Failed to read local heartbeat for workerId : {},Ignoring exception.", workerId, e);
            return null;
        }
    }

    public static boolean  isWorkerHbTimedOut(int now, LSWorkerHeartbeat whb, Map<String, Object> conf) {
        return _instance.isWorkerHbTimedOutImpl(now, whb, conf);
    }

    private  boolean  isWorkerHbTimedOutImpl(int now, LSWorkerHeartbeat whb, Map<String, Object> conf) {
        return (now - whb.get_time_secs()) > Utils.getInt(conf.get(Config.SUPERVISOR_WORKER_TIMEOUT_SECS));
    }
    
    /**
     * Launch a new process as per {@link java.lang.ProcessBuilder} with a given
     * callback.
     * @param command the command to be executed in the new process
     * @param environment the environment to be applied to the process. Can be
     *                    null.
     * @param logPrefix a prefix for log entries from the output of the process.
     *                  Can be null.
     * @param exitCodeCallback code to be called passing the exit code value
     *                         when the process completes
     * @param dir the working directory of the new process
     * @return the new process
     * @throws IOException
     * @see java.lang.ProcessBuilder
     */
    public static Process launchProcess(List<String> command,
                                        Map<String,String> environment,
                                        final String logPrefix,
                                        final ExitCodeCallback exitCodeCallback,
                                        File dir)
            throws IOException {
        ProcessBuilder builder = new ProcessBuilder(command);
        Map<String,String> procEnv = builder.environment();
        if (dir != null) {
            builder.directory(dir);
        }
        builder.redirectErrorStream(true);
        if (environment != null) {
            procEnv.putAll(environment);
        }
        final Process process = builder.start();
        if (logPrefix != null || exitCodeCallback != null) {
            Utils.asyncLoop(new Callable<Object>() {
                public Object call() {
                    if (logPrefix != null ) {
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
    
    static List<ACL> supervisorZkAcls() {
        final List<ACL> acls = new ArrayList<>();
        acls.add(ZooDefs.Ids.CREATOR_ALL_ACL.get(0));
        acls.add(new ACL((ZooDefs.Perms.READ ^ ZooDefs.Perms.CREATE), ZooDefs.Ids.ANYONE_ID_UNSAFE));
        return acls;
    }
}
