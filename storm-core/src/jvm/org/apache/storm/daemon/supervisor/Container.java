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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.container.ResourceIsolationInterface;
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
    private static final Logger LOG = LoggerFactory.getLogger(BasicContainer.class);
    protected final Map<String, Object> _conf;
    protected final Map<String, Object> _topoConf;
    protected String _workerId;
    protected final String _topologyId;
    protected final String _supervisorId;
    protected final int _port;
    protected final LocalAssignment _assignment;
    protected final AdvancedFSOps _ops;
    protected final ResourceIsolationInterface _resourceIsolationManager;
    
    //Exposed for testing
    protected Container(AdvancedFSOps ops, int port, LocalAssignment assignment,
            Map<String, Object> conf, Map<String, Object> topoConf, String supervisorId, 
            ResourceIsolationInterface resourceIsolationManager) throws IOException {
        assert((assignment == null && port <= 0) ||
                (assignment != null && port > 0));
        assert(conf != null);
        assert(ops != null);
        assert(supervisorId != null);
        
        _port = port;
        _ops = ops;
        _assignment = assignment;
        if (assignment != null) {
            _topologyId = assignment.get_topology_id();
        } else {
            _topologyId = null;
        }
        _conf = conf;
        _supervisorId = supervisorId;
        _resourceIsolationManager = resourceIsolationManager;
        if (topoConf == null) {
            _topoConf = readTopoConf();
        } else {
            _topoConf = topoConf;
        }
    }
    
    protected Map<String, Object> readTopoConf() throws IOException {
        assert(_topologyId != null);
        return ConfigUtils.readSupervisorStormConf(_conf, _topologyId);
    }
    
    protected Container(int port, LocalAssignment assignment, Map<String, Object> conf, 
            String supervisorId, ResourceIsolationInterface resourceIsolationManager) throws IOException {
        this(AdvancedFSOps.mk(conf), port, assignment, conf, null, supervisorId, resourceIsolationManager);
    }
    
    /**
     * Constructor to use when trying to recover a container from just the worker ID.
     * @param workerId the id of the worker
     * @param conf the config of the supervisor
     * @param supervisorId the id of the supervisor
     * @param resourceIsolationManager the isolation manager.
     * @throws IOException on any error
     */
    protected Container(String workerId, Map<String, Object> conf, 
            String supervisorId, ResourceIsolationInterface resourceIsolationManager) throws IOException {
        this(AdvancedFSOps.mk(conf), -1, null, conf, null, supervisorId, resourceIsolationManager);
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
        LOG.warn("{}: Reading heartbeat {}", _workerId, hb);
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
        ProcessBuilder pb = new ProcessBuilder("tasklist", "/nh", "/fi", "pid eq"+pid);
        pb.redirectError(Redirect.INHERIT);
        Process p = pb.start();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            if (in.readLine() != null) {
                ret = true;
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
                LOG.warn("{}: PID {} is dead", _workerId, pid);
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
        if (_port <= 0) {
            throw new IllegalStateException("Cannot setup a container recovered with just a worker id");
        }
        if (!_ops.doRequiredTopoFilesExist(_conf, _topologyId)) {
            LOG.info("Missing topology storm code, so can't launch  worker with assignment {} for this supervisor {} on port {} with id {}", _assignment,
                    _supervisorId, _port, _workerId);
            throw new IllegalStateException("Not all needed files are here!!!!");
        }
    
        _ops.forceMkdir(new File(ConfigUtils.workerPidsRoot(_conf, _workerId)));
        _ops.forceMkdir(new File(ConfigUtils.workerTmpRoot(_conf, _workerId)));
        _ops.forceMkdir(new File(ConfigUtils.workerHeartbeatsRoot(_conf, _workerId)));
        
        File workerArtifacts = new File(ConfigUtils.workerArtifactsRoot(_conf, _topologyId, _port));
        if (!_ops.fileExists(workerArtifacts)) {
            _ops.forceMkdir(workerArtifacts);
            _ops.setupStormCodeDir(_topoConf, workerArtifacts);
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
        if (_port <= 0) {
            throw new IllegalStateException("Cannot setup a container recovered with just a worker id");
        }
        Map<String, Object> data = new HashMap<>();
        data.put(Config.TOPOLOGY_SUBMITTER_USER, user);
        data.put("worker-id", _workerId);

        Set<String> logsGroups = new HashSet<>();
        //for supervisor-test
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
        if (_port <= 0) {
            throw new IllegalStateException("Cannot setup a container recovered with just a worker id");
        }
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
        List<String> resourceFileNames = new ArrayList<>();
        resourceFileNames.add(ConfigUtils.RESOURCES_SUBDIR);
        resourceFileNames.addAll(blobFileNames);
        LOG.info("Creating symlinks for worker-id: {} storm-id: {} for files({}): {}", _workerId, _topologyId, resourceFileNames.size(), resourceFileNames);
        _ops.createSymlink(new File(workerRoot, ConfigUtils.RESOURCES_SUBDIR), 
                new File(stormRoot, ConfigUtils.RESOURCES_SUBDIR));
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
        
        if (_resourceIsolationManager != null) {
            Set<Long> morePids = _resourceIsolationManager.getRunningPIDs(_workerId);
            if (morePids != null) {
                ret.addAll(morePids);
            }
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
        
        if (file.exists()) {
            try (InputStream in = new FileInputStream(file);
                    Reader reader = new InputStreamReader(in);
                    BufferedReader br = new BufferedReader(reader);) {
                StringBuilder sb = new StringBuilder();
                int r;
                while ((r = br.read()) != -1) {
                    char ch = (char) r;
                    sb.append(ch);
                }
                String ret = sb.toString().trim();
                return ret;
            }
        } else {
            return (String) _topoConf.get(Config.TOPOLOGY_SUBMITTER_USER);
        }
    }
    
    protected void saveWorkerUser(String user) throws IOException {
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
     * @throws IOException on any error
     */
    public void cleanUpForRestart() throws IOException {
        // and another API to cleanup with everything is dead
        LOG.info("Cleaning up {}:{}", _supervisorId, _workerId);
        Set<Long> pids = getAllPids();
        String user = getWorkerUser();
        
        for (Long pid : pids) {
            File path = new File(ConfigUtils.workerPidPath(_conf, _workerId, pid));
            _ops.deleteIfExists(path, user, _workerId);
        }
        
        //clean up for resource isolation if enabled
        if (_resourceIsolationManager != null) {
            _resourceIsolationManager.releaseResourcesForWorker(_workerId);
        }
        
        //Always make sure to clean up everything else before worker directory
        //is removed since that is what is going to trigger the retry for cleanup
        _ops.deleteIfExists(new File(ConfigUtils.workerHeartbeatsRoot(_conf, _workerId)), user, _workerId);
        _ops.deleteIfExists(new File(ConfigUtils.workerPidsRoot(_conf, _workerId)), user, _workerId);
        _ops.deleteIfExists(new File(ConfigUtils.workerTmpRoot(_conf, _workerId)), user, _workerId);
        _ops.deleteIfExists(new File(ConfigUtils.workerRoot(_conf, _workerId)), user, _workerId);
        deleteSavedWorkerUser();
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

}
