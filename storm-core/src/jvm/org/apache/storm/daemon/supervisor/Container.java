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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * Represents a container that a worker will run in.
 */
public abstract class Container implements Killable {
    private static final Logger LOG = LoggerFactory.getLogger(Container.class);
    public static enum ContainerType {
        LAUNCH(false, false),
        RECOVER_FULL(true, false),
        RECOVER_PARTIAL(true, true);

        private final boolean _recovery;
        private final boolean _onlyKillable;
        
        ContainerType(boolean recovery, boolean onlyKillable) {
            _recovery = recovery;
            _onlyKillable = onlyKillable;
        }
        
        public boolean isRecovery() {
            return _recovery;
        }
        
        public void assertFull() {
            if (_onlyKillable) {
                throw new IllegalStateException("Container is only Killable.");
            }
        }
        
        public boolean isOnlyKillable() {
            return _onlyKillable;
        }
    }
    
    protected final Map<String, Object> _conf;
    protected final Map<String, Object> _topoConf; //Not set if RECOVER_PARTIAL
    protected String _workerId; 
    protected final String _topologyId; //Not set if RECOVER_PARTIAL
    protected final String _supervisorId;
    protected final int _port; //Not set if RECOVER_PARTIAL
    protected final LocalAssignment _assignment; //Not set if RECOVER_PARTIAL
    protected final AdvancedFSOps _ops;
    protected ContainerType _type;
    
    /**
     * Create a new Container.
     * @param type the type of container being made.
     * @param conf the supervisor config
     * @param supervisorId the ID of the supervisor this is a part of.
     * @param port the port the container is on.  Should be <= 0 if only a partial recovery
     * @param assignment the assignment for this container. Should be null if only a partial recovery.
     * @param workerId the id of the worker to use.  Must not be null if doing a partial recovery.
     * @param topoConf the config of the topology (mostly for testing) if null 
     * and not a partial recovery the real conf is read.
     * @param ops file system operations (mostly for testing) if null a new one is made
     * @throws IOException on any error.
     */
    protected Container(ContainerType type, Map<String, Object> conf, String supervisorId,
            int port, LocalAssignment assignment,
            String workerId, Map<String, Object> topoConf,  AdvancedFSOps ops) throws IOException {
        assert(type != null);
        assert(conf != null);
        assert(supervisorId != null);
        
        if (ops == null) {
            ops = AdvancedFSOps.make(conf);
        }
        
        _workerId = workerId;
        _type = type;
        _port = port;
        _ops = ops;
        _conf = conf;
        _supervisorId = supervisorId;
        _assignment = assignment;
        
        if (_type.isOnlyKillable()) {
            assert(_assignment == null);
            assert(_port <= 0);
            assert(_workerId != null);
            _topologyId = null;
            _topoConf = null;
        } else {
            assert(assignment != null);
            assert(port > 0);
            _topologyId = assignment.get_topology_id();
            if (!_ops.doRequiredTopoFilesExist(_conf, _topologyId)) {
                LOG.info("Missing topology storm code, so can't launch  worker with assignment {} for this supervisor {} on port {} with id {}", _assignment,
                        _supervisorId, _port, _workerId);
                throw new ContainerRecoveryException("Missing required topology files...");
            }
            if (topoConf == null) {
                _topoConf = readTopoConf();
            } else {
                //For testing...
                _topoConf = topoConf;
            }
        }
    }
    
    @Override
    public String toString() {
        return "topo:" + _topologyId + " worker:" + _workerId;
    }
    
    protected Map<String, Object> readTopoConf() throws IOException {
        assert(_topologyId != null);
        return ConfigUtils.readSupervisorStormConf(_conf, _topologyId);
    }
    
    /**
     * Kill a given process
     * @param pid the id of the process to kill
     * @throws IOException
     */
    protected void kill(long pid) throws IOException {
        Utils.killProcessWithSigTerm(String.valueOf(pid));
    }
    
    /**
     * Kill a given process
     * @param pid the id of the process to kill
     * @throws IOException
     */
    protected void forceKill(long pid) throws IOException {
        Utils.forceKillProcess(String.valueOf(pid));
    }
    
    @Override
    public void kill() throws IOException {
        LOG.info("Killing {}:{}", _supervisorId, _workerId);
        Set<Long> pids = getAllPids();

        for (Long pid : pids) {
            kill(pid);
        }
    }
    
    @Override
    public void forceKill() throws IOException {
        LOG.info("Force Killing {}:{}", _supervisorId, _workerId);
        Set<Long> pids = getAllPids();
        
        for (Long pid : pids) {
            forceKill(pid);
        }
    }
    
    /**
     * Read the Heartbeat for the current container.
     * @return the Heartbeat
     * @throws IOException on any error
     */
    public LSWorkerHeartbeat readHeartbeat() throws IOException {
        LocalState localState = ConfigUtils.workerState(_conf, _workerId);
        LSWorkerHeartbeat hb = localState.getWorkerHeartBeat();
        LOG.trace("{}: Reading heartbeat {}", _workerId, hb);
        return hb;
    }

    /**
     * Is a process alive and running?
     * @param pid the PID of the running process
     * @param user the user that is expected to own that process
     * @return true if it is, else false
     * @throws IOException on any error
     */
    protected boolean isProcessAlive(long pid, String user) throws IOException {
        if (Utils.IS_ON_WINDOWS) {
            return isWindowsProcessAlive(pid, user);
        }
        return isPosixProcessAlive(pid, user);
    }
    
    private boolean isWindowsProcessAlive(long pid, String user) throws IOException {
        boolean ret = false;
        ProcessBuilder pb = new ProcessBuilder("tasklist", "/fo", "list", "/fi", "pid eq " + pid, "/v");
        pb.redirectError(Redirect.INHERIT);
        Process p = pb.start();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String read;
            while ((read = in.readLine()) != null) {
                if (read.contains("User Name:")) { //Check for : in case someone called their user "User Name"
                    //This line contains the user name for the pid we're looking up
                    //Example line: "User Name:    exampleDomain\exampleUser"
                    List<String> userNameLineSplitOnWhitespace = Arrays.asList(read.split(":"));
                    if(userNameLineSplitOnWhitespace.size() == 2){
                        List<String> userAndMaybeDomain = Arrays.asList(userNameLineSplitOnWhitespace.get(1).trim().split("\\\\"));
                        String processUser = userAndMaybeDomain.size() == 2 ? userAndMaybeDomain.get(1) : userAndMaybeDomain.get(0);
                        if(user.equals(processUser)){
                            ret = true;
                        } else {
                            LOG.info("Found {} running as {}, but expected it to be {}", pid, processUser, user);
                        }
                    } else {
                        LOG.error("Received unexpected output from tasklist command. Expected one colon in user name line. Line was {}", read);
                    }
                    break;
                }
            }
        }
        return ret;
    }
    
    private boolean isPosixProcessAlive(long pid, String user) throws IOException {
        boolean ret = false;
        ProcessBuilder pb = new ProcessBuilder("ps", "-o", "user", "-p", String.valueOf(pid));
        pb.redirectError(Redirect.INHERIT);
        Process p = pb.start();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String first = in.readLine();
            assert("USER".equals(first));
            String processUser;
            while ((processUser = in.readLine()) != null) {
                if (user.equals(processUser)) {
                    ret = true;
                    break;
                } else {
                    LOG.info("Found {} running as {}, but expected it to be {}", pid, processUser, user);
                }
            }
        }
        return ret;
    }
    
    @Override
    public boolean areAllProcessesDead() throws IOException {
        Set<Long> pids = getAllPids();
        String user = getWorkerUser();
        
        boolean allDead = true;
        for (Long pid: pids) {
            if (!isProcessAlive(pid, user)) {
                LOG.debug("{}: PID {} is dead", _workerId, pid);
            } else {
                allDead = false;
                break;
            }
        }
        return allDead;
    }

    @Override
    public void cleanUp() throws IOException {
        cleanUpForRestart();
    }
    
    /**
     * Setup the container to run.  By default this creates the needed directories/links in the
     * local file system
     * PREREQUISITE: All needed blobs and topology, jars/configs have been downloaded and
     * placed in the appropriate locations
     * @throws IOException on any error
     */
    protected void setup() throws IOException {
        _type.assertFull();
        if (!_ops.doRequiredTopoFilesExist(_conf, _topologyId)) {
            LOG.info("Missing topology storm code, so can't launch  worker with assignment {} for this supervisor {} on port {} with id {}", _assignment,
                    _supervisorId, _port, _workerId);
            throw new IllegalStateException("Not all needed files are here!!!!");
        } 
        LOG.info("Setting up {}:{}", _supervisorId, _workerId);

        _ops.forceMkdir(new File(ConfigUtils.workerPidsRoot(_conf, _workerId)));
        _ops.forceMkdir(new File(ConfigUtils.workerTmpRoot(_conf, _workerId)));
        _ops.forceMkdir(new File(ConfigUtils.workerHeartbeatsRoot(_conf, _workerId)));
        
        File workerArtifacts = new File(ConfigUtils.workerArtifactsRoot(_conf, _topologyId, _port));
        if (!_ops.fileExists(workerArtifacts)) {
            _ops.forceMkdir(workerArtifacts);
            _ops.setupWorkerArtifactsDir(_topoConf, workerArtifacts);
        }
    
        String user = getWorkerUser();
        writeLogMetadata(user);
        saveWorkerUser(user);
        createArtifactsLink();
        createBlobstoreLinks();
    }
    
    /**
     * Write out the file used by the log viewer to allow/reject log access
     * @param user the user this is going to run as
     * @throws IOException on any error
     */
    @SuppressWarnings("unchecked")
    protected void writeLogMetadata(String user) throws IOException {
        _type.assertFull();
        Map<String, Object> data = new HashMap<>();
        data.put(Config.TOPOLOGY_SUBMITTER_USER, user);
        data.put("worker-id", _workerId);

        Set<String> logsGroups = new HashSet<>();
        if (_topoConf.get(Config.LOGS_GROUPS) != null) {
            List<String> groups = (List<String>) _topoConf.get(Config.LOGS_GROUPS);
            for (String group : groups){
                logsGroups.add(group);
            }
        }
        if (_topoConf.get(Config.TOPOLOGY_GROUPS) != null) {
            List<String> topGroups = (List<String>) _topoConf.get(Config.TOPOLOGY_GROUPS);
            logsGroups.addAll(topGroups);
        }
        data.put(Config.LOGS_GROUPS, logsGroups.toArray());

        Set<String> logsUsers = new HashSet<>();
        if (_topoConf.get(Config.LOGS_USERS) != null) {
            List<String> logUsers = (List<String>) _topoConf.get(Config.LOGS_USERS);
            for (String logUser : logUsers){
                logsUsers.add(logUser);
            }
        }
        if (_topoConf.get(Config.TOPOLOGY_USERS) != null) {
            List<String> topUsers = (List<String>) _topoConf.get(Config.TOPOLOGY_USERS);
            for (String logUser : topUsers){
                logsUsers.add(logUser);
            }
        }
        data.put(Config.LOGS_USERS, logsUsers.toArray());

        File file = ConfigUtils.getLogMetaDataFile(_conf, _topologyId, _port);

        Yaml yaml = new Yaml();
        try (Writer writer = _ops.getWriter(file)) {
            yaml.dump(data, writer);
        }
    }
    
    /**
     * Create symlink from the containers directory/artifacts to the artifacts directory
     * @throws IOException on any error
     */
    protected void createArtifactsLink() throws IOException {
        _type.assertFull();
        File workerDir = new File(ConfigUtils.workerRoot(_conf, _workerId));
        File topoDir = new File(ConfigUtils.workerArtifactsRoot(_conf, _topologyId, _port));
        if (_ops.fileExists(workerDir)) {
            LOG.debug("Creating symlinks for worker-id: {} topology-id: {} to its port artifacts directory", _workerId, _topologyId);
            _ops.createSymlink(new File(workerDir, "artifacts"), topoDir);
        }
    }
    
    /**
     * Create symlinks for each of the blobs from the container's directory to
     * corresponding links in the storm dist directory.
     * @throws IOException on any error.
     */
    protected void createBlobstoreLinks() throws IOException {
        _type.assertFull();
        String stormRoot = ConfigUtils.supervisorStormDistRoot(_conf, _topologyId);
        String workerRoot = ConfigUtils.workerRoot(_conf, _workerId);
        
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> blobstoreMap = (Map<String, Map<String, Object>>) _topoConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
        List<String> blobFileNames = new ArrayList<>();
        if (blobstoreMap != null) {
            for (Map.Entry<String, Map<String, Object>> entry : blobstoreMap.entrySet()) {
                String key = entry.getKey();
                Map<String, Object> blobInfo = entry.getValue();
                String ret = null;
                if (blobInfo != null && blobInfo.containsKey("localname")) {
                    ret = (String) blobInfo.get("localname");
                } else {
                    ret = key;
                }
                blobFileNames.add(ret);
            }
        }
        File targetResourcesDir = new File(stormRoot, ConfigUtils.RESOURCES_SUBDIR);
        List<String> resourceFileNames = new ArrayList<>();
        if (targetResourcesDir.exists()) {
            resourceFileNames.add(ConfigUtils.RESOURCES_SUBDIR);
        }
        resourceFileNames.addAll(blobFileNames);

        LOG.info("Creating symlinks for worker-id: {} storm-id: {} for files({}): {}", _workerId, _topologyId, resourceFileNames.size(), resourceFileNames);
        if(targetResourcesDir.exists()) {
            _ops.createSymlink(new File(workerRoot, ConfigUtils.RESOURCES_SUBDIR),  targetResourcesDir );
        } else {
            LOG.info("Topology jar for worker-id: {} storm-id: {} does not contain re sources directory {}." , _workerId, _topologyId, targetResourcesDir.toString() );
        }
        for (String fileName : blobFileNames) {
            _ops.createSymlink(new File(workerRoot, fileName),
                    new File(stormRoot, fileName));
        }
    }
    
    /**
     * @return all of the pids that are a part of this container.
     */
    protected Set<Long> getAllPids() throws IOException {
        Set<Long> ret = new HashSet<>();
        for (String listing: Utils.readDirContents(ConfigUtils.workerPidsRoot(_conf, _workerId))) {
            ret.add(Long.valueOf(listing));
        }
        
        return ret;
    }
    
    /** 
     * @return the user that some operations should be done as.
     * @throws IOException on any error
     */
    protected String getWorkerUser() throws IOException {
        LOG.info("GET worker-user for {}", _workerId);
        File file = new File(ConfigUtils.workerUserFile(_conf, _workerId));

        if (_ops.fileExists(file)) {
            return _ops.slurpString(file).trim();
        } else if (_topoConf != null) { 
            return (String) _topoConf.get(Config.TOPOLOGY_SUBMITTER_USER);
        }
        if (ConfigUtils.isLocalMode(_conf)) {
            return System.getProperty("user.name");
        } else {
            File f = new File(ConfigUtils.workerArtifactsRoot(_conf));
            if (f.exists()) {
                return Files.getOwner(f.toPath()).getName();
            }
            throw new IllegalStateException("Could not recover the user for " + _workerId);
        }
    }
    
    protected void saveWorkerUser(String user) throws IOException {
        _type.assertFull();
        LOG.info("SET worker-user {} {}", _workerId, user);
        _ops.dump(new File(ConfigUtils.workerUserFile(_conf, _workerId)), user);
    }
    
    protected void deleteSavedWorkerUser() throws IOException {
        LOG.info("REMOVE worker-user {}", _workerId);
        _ops.deleteIfExists(new File(ConfigUtils.workerUserFile(_conf, _workerId)));
    }
    
    /**
     * Clean up the container partly preparing for restart.
     * By default delete all of the temp directories we are going
     * to get a new worker_id anyways.
     * POST CONDITION: the workerId will be set to null
     * @throws IOException on any error
     */
    public void cleanUpForRestart() throws IOException {
        LOG.info("Cleaning up {}:{}", _supervisorId, _workerId);
        Set<Long> pids = getAllPids();
        String user = getWorkerUser();
        
        for (Long pid : pids) {
            File path = new File(ConfigUtils.workerPidPath(_conf, _workerId, pid));
            _ops.deleteIfExists(path, user, _workerId);
        }
        
        //Always make sure to clean up everything else before worker directory
        //is removed since that is what is going to trigger the retry for cleanup
        _ops.deleteIfExists(new File(ConfigUtils.workerHeartbeatsRoot(_conf, _workerId)), user, _workerId);
        _ops.deleteIfExists(new File(ConfigUtils.workerPidsRoot(_conf, _workerId)), user, _workerId);
        _ops.deleteIfExists(new File(ConfigUtils.workerTmpRoot(_conf, _workerId)), user, _workerId);
        _ops.deleteIfExists(new File(ConfigUtils.workerRoot(_conf, _workerId)), user, _workerId);
        deleteSavedWorkerUser();
        _workerId = null;
    }
    
    /**
     * Launch the process for the first time
     * PREREQUISITE: setup has run and passed
     * @throws IOException on any error
     */
    public abstract void launch() throws IOException;
    
    /**
     * Restart the processes in this container
     * PREREQUISITE: cleanUpForRestart has run and passed
     * @throws IOException on any error
     */
    public abstract void relaunch() throws IOException;

    /**
     * @return true if the main process exited, else false. This is just best effort return false if unknown.
     */
    public abstract boolean didMainProcessExit();

    /**
     * Run a profiling request
     * @param request the request to run
     * @param stop is this a stop request?
     * @return true if it succeeded, else false
     * @throws IOException on any error
     * @throws InterruptedException if running the command is interrupted.
     */
    public abstract boolean runProfiling(ProfileRequest request, boolean stop) throws IOException, InterruptedException;

    /**
     * @return the id of the container or null if there is no worker id right now.
     */
    public String getWorkerId() {
        return _workerId;
    }
}
