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
import com.codahale.metrics.Timer;
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
import java.util.Optional;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.container.ResourceIsolationInterface;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.WorkerMetricList;
import org.apache.storm.generated.WorkerMetricPoint;
import org.apache.storm.generated.WorkerMetrics;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.metricstore.MetricException;
import org.apache.storm.metricstore.WorkerMetricsProcessor;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.ServerConfigUtils;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * Represents a container that a worker will run in.
 */
public abstract class Container implements Killable {

    private static final Logger LOG = LoggerFactory.getLogger(Container.class);
    private static final String MEMORY_USED_METRIC = "UsedMemory";
    private static final String SYSTEM_COMPONENT_ID = "System";
    private static final String INVALID_EXECUTOR_ID = "-1";
    private static final String INVALID_STREAM_ID = "None";

    private final Meter numCleanupExceptions;
    private final Meter numKillExceptions;
    private final Meter numForceKillExceptions;
    private final Meter numForceKill;
    private final Timer shutdownDuration;
    private final Timer cleanupDuration;

    protected final Map<String, Object> _conf;
    protected final Map<String, Object> _topoConf; //Not set if RECOVER_PARTIAL
    protected final String _topologyId; //Not set if RECOVER_PARTIAL
    protected final String _supervisorId;
    protected final int _supervisorPort;
    protected final int _port; //Not set if RECOVER_PARTIAL
    protected final LocalAssignment _assignment; //Not set if RECOVER_PARTIAL
    protected final AdvancedFSOps _ops;
    protected final ResourceIsolationInterface _resourceIsolationManager;
    protected final boolean _symlinksDisabled;
    protected String _workerId;
    protected ContainerType _type;
    protected ContainerMemoryTracker containerMemoryTracker;
    private long lastMetricProcessTime = 0L;
    private Timer.Context shutdownTimer = null;

    /**
     * Create a new Container.
     *
     * @param type the type of container being made.
     * @param conf the supervisor config
     * @param supervisorId the ID of the supervisor this is a part of.
     * @param supervisorPort the thrift server port of the supervisor this is a part of.
     * @param port the port the container is on. Should be <= 0 if only a partial recovery @param assignment
     * the assignment for this container. Should be null if only a partial recovery.
     * @param resourceIsolationManager used to isolate resources for a container can be null if no isolation is used.
     * @param workerId the id of the worker to use. Must not be null if doing a partial recovery.
     * @param topoConf the config of the topology (mostly for testing) if null and not a partial recovery the real conf is read.
     * @param ops file system operations (mostly for testing) if null a new one is made
     * @param metricsRegistry The metrics registry.
     * @param containerMemoryTracker The shared memory tracker for the supervisor's containers
     * @throws IOException on any error.
     */
    protected Container(ContainerType type, Map<String, Object> conf, String supervisorId, int supervisorPort,
        int port, LocalAssignment assignment, ResourceIsolationInterface resourceIsolationManager,
        String workerId, Map<String, Object> topoConf, AdvancedFSOps ops,
        StormMetricsRegistry metricsRegistry, ContainerMemoryTracker containerMemoryTracker) throws IOException {
        assert (type != null);
        assert (conf != null);
        assert (supervisorId != null);

        _symlinksDisabled = (boolean) conf.getOrDefault(Config.DISABLE_SYMLINKS, false);

        if (ops == null) {
            ops = AdvancedFSOps.make(conf);
        }

        _workerId = workerId;
        _type = type;
        _port = port;
        _ops = ops;
        _conf = conf;
        _supervisorId = supervisorId;
        _supervisorPort = supervisorPort;
        _resourceIsolationManager = resourceIsolationManager;
        _assignment = assignment;

        if (_type.isOnlyKillable()) {
            assert (_assignment == null);
            assert (_port <= 0);
            assert (_workerId != null);
            _topologyId = null;
            _topoConf = null;
        } else {
            assert (assignment != null);
            assert (port > 0);
            _topologyId = assignment.get_topology_id();
            if (!_ops.doRequiredTopoFilesExist(_conf, _topologyId)) {
                LOG.info(
                    "Missing topology storm code, so can't launch  worker with assignment {} for this supervisor {} on port {} with id {}",
                    _assignment,
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
        this.numCleanupExceptions = metricsRegistry.registerMeter("supervisor:num-cleanup-exceptions");
        this.numKillExceptions = metricsRegistry.registerMeter("supervisor:num-kill-exceptions");
        this.numForceKillExceptions = metricsRegistry.registerMeter("supervisor:num-force-kill-exceptions");
        this.numForceKill = metricsRegistry.registerMeter("supervisor:num-workers-force-kill");
        this.shutdownDuration = metricsRegistry.registerTimer("supervisor:worker-shutdown-duration-ns");
        this.cleanupDuration = metricsRegistry.registerTimer("supervisor:worker-per-call-clean-up-duration-ns");
        this.containerMemoryTracker = containerMemoryTracker;
    }

    @Override
    public String toString() {
        return "topo:" + _topologyId + " worker:" + _workerId;
    }

    protected Map<String, Object> readTopoConf() throws IOException {
        assert (_topologyId != null);
        return ConfigUtils.readSupervisorStormConf(_conf, _topologyId);
    }

    /**
     * Kill a given process.
     *
     * @param pid the id of the process to kill
     * @throws IOException
     */
    protected void kill(long pid) throws IOException {
        ServerUtils.killProcessWithSigTerm(String.valueOf(pid));
    }

    /**
     * Kill a given process.
     *
     * @param pid the id of the process to kill
     * @throws IOException
     */
    protected void forceKill(long pid) throws IOException {
        ServerUtils.forceKillProcess(String.valueOf(pid));
    }

    @Override
    public void kill() throws IOException {
        LOG.info("Killing {}:{}", _supervisorId, _workerId);
        if (shutdownTimer == null) {
            shutdownTimer = shutdownDuration.time();
        }
        try {
            Set<Long> pids = getAllPids();

            for (Long pid : pids) {
                kill(pid);
            }
        } catch (IOException e) {
            numKillExceptions.mark();
            throw e;
        }
    }

    @Override
    public void forceKill() throws IOException {
        LOG.info("Force Killing {}:{}", _supervisorId, _workerId);
        numForceKill.mark();
        try {
            Set<Long> pids = getAllPids();

            for (Long pid : pids) {
                forceKill(pid);
            }
        } catch (IOException e) {
            numForceKillExceptions.mark();
            throw e;
        }
    }

    /**
     * Read the Heartbeat for the current container.
     *
     * @return the Heartbeat
     *
     * @throws IOException on any error
     */
    public LSWorkerHeartbeat readHeartbeat() throws IOException {
        LocalState localState = ConfigUtils.workerState(_conf, _workerId);
        LSWorkerHeartbeat hb = localState.getWorkerHeartBeat();
        LOG.trace("{}: Reading heartbeat {}", _workerId, hb);
        return hb;
    }

    /**
     * Is a process alive and running?.
     *
     * @param pid the PID of the running process
     * @param user the user that is expected to own that process
     * @return true if it is, else false
     *
     * @throws IOException on any error
     */
    protected boolean isProcessAlive(long pid, String user) throws IOException {
        if (ServerUtils.IS_ON_WINDOWS) {
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
                    if (userNameLineSplitOnWhitespace.size() == 2) {
                        List<String> userAndMaybeDomain = Arrays.asList(userNameLineSplitOnWhitespace.get(1).trim().split("\\\\"));
                        String processUser = userAndMaybeDomain.size() == 2 ? userAndMaybeDomain.get(1) : userAndMaybeDomain.get(0);
                        if (user.equals(processUser)) {
                            ret = true;
                        } else {
                            LOG.info("Found {} running as {}, but expected it to be {}", pid, processUser, user);
                        }
                    } else {
                        LOG.error("Received unexpected output from tasklist command. Expected one colon in user name line. Line was {}",
                            read);
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
            assert ("USER".equals(first));
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
        String user = getRunWorkerAsUser();

        boolean allDead = true;
        for (Long pid : pids) {
            LOG.debug("Checking if pid {} owner {} is alive", pid, user);
            if (!isProcessAlive(pid, user)) {
                LOG.debug("{}: PID {} is dead", _workerId, pid);
            } else {
                allDead = false;
                break;
            }
        }

        if (allDead && shutdownTimer != null) {
            shutdownTimer.stop();
            shutdownTimer = null;
        }

        return allDead;
    }

    @Override
    public void cleanUp() throws IOException {
        try (Timer.Context t = cleanupDuration.time()) {
            containerMemoryTracker.remove(_port);
            cleanUpForRestart();
        } catch (IOException e) {
            //This may or may not be reported depending on when process exits
            numCleanupExceptions.mark();
            throw e;
        }
    }

    /**
     * Setup the container to run. By default this creates the needed directories/links in the local file system PREREQUISITE: All needed
     * blobs and topology, jars/configs have been downloaded and placed in the appropriate locations
     *
     * @throws IOException on any error
     */
    protected void setup() throws IOException {
        _type.assertFull();
        if (!_ops.doRequiredTopoFilesExist(_conf, _topologyId)) {
            LOG.info("Missing topology storm code, so can't launch  worker with assignment {} for this supervisor {} on port {} with id {}",
                _assignment,
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
            _ops.setupWorkerArtifactsDir(_assignment.get_owner(), workerArtifacts);
        }

        String user = getWorkerUser();
        writeLogMetadata(user);
        saveWorkerUser(user);
        createArtifactsLink();
        createBlobstoreLinks();
    }

    /**
     * Write out the file used by the log viewer to allow/reject log access.
     *
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
        if (_topoConf.get(DaemonConfig.LOGS_GROUPS) != null) {
            List<String> groups = (List<String>) _topoConf.get(DaemonConfig.LOGS_GROUPS);
            for (String group : groups) {
                logsGroups.add(group);
            }
        }
        if (_topoConf.get(Config.TOPOLOGY_GROUPS) != null) {
            List<String> topGroups = (List<String>) _topoConf.get(Config.TOPOLOGY_GROUPS);
            logsGroups.addAll(topGroups);
        }
        data.put(DaemonConfig.LOGS_GROUPS, logsGroups.toArray());

        Set<String> logsUsers = new HashSet<>();
        if (_topoConf.get(DaemonConfig.LOGS_USERS) != null) {
            List<String> logUsers = (List<String>) _topoConf.get(DaemonConfig.LOGS_USERS);
            for (String logUser : logUsers) {
                logsUsers.add(logUser);
            }
        }
        if (_topoConf.get(Config.TOPOLOGY_USERS) != null) {
            List<String> topUsers = (List<String>) _topoConf.get(Config.TOPOLOGY_USERS);
            for (String logUser : topUsers) {
                logsUsers.add(logUser);
            }
        }
        data.put(DaemonConfig.LOGS_USERS, logsUsers.toArray());

        File file = ServerConfigUtils.getLogMetaDataFile(_conf, _topologyId, _port);

        Yaml yaml = new Yaml();
        try (Writer writer = _ops.getWriter(file)) {
            yaml.dump(data, writer);
        }
    }

    /**
     * Create symlink from the containers directory/artifacts to the artifacts directory.
     *
     * @throws IOException on any error
     */
    protected void createArtifactsLink() throws IOException {
        _type.assertFull();
        if (!_symlinksDisabled) {
            File workerDir = new File(ConfigUtils.workerRoot(_conf, _workerId));
            File topoDir = new File(ConfigUtils.workerArtifactsRoot(_conf, _topologyId, _port));
            if (_ops.fileExists(workerDir)) {
                LOG.debug("Creating symlinks for worker-id: {} topology-id: {} to its port artifacts directory", _workerId, _topologyId);
                _ops.createSymlink(new File(workerDir, "artifacts"), topoDir);
            }
        }
    }

    /**
     * Create symlinks for each of the blobs from the container's directory to corresponding links in the storm dist directory.
     *
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
        File targetResourcesDir = new File(stormRoot, ServerConfigUtils.RESOURCES_SUBDIR);
        List<String> resourceFileNames = new ArrayList<>();
        if (targetResourcesDir.exists()) {
            resourceFileNames.add(ServerConfigUtils.RESOURCES_SUBDIR);
        }
        resourceFileNames.addAll(blobFileNames);

        if (!_symlinksDisabled) {
            LOG.info("Creating symlinks for worker-id: {} storm-id: {} for files({}): {}", _workerId, _topologyId, resourceFileNames.size(),
                resourceFileNames);
            if (targetResourcesDir.exists()) {
                _ops.createSymlink(new File(workerRoot, ServerConfigUtils.RESOURCES_SUBDIR), targetResourcesDir);
            } else {
                LOG.info("Topology jar for worker-id: {} storm-id: {} does not contain re sources directory {}.", _workerId, _topologyId,
                    targetResourcesDir.toString());
            }
            for (String fileName : blobFileNames) {
                _ops.createSymlink(new File(workerRoot, fileName),
                    new File(stormRoot, fileName));
            }
        } else if (blobFileNames.size() > 0) {
            LOG.warn("Symlinks are disabled, no symlinks created for blobs {}", blobFileNames);
        }
    }

    /**
     * @return all of the pids that are a part of this container.
     */
    protected Set<Long> getAllPids() throws IOException {
        Set<Long> ret = new HashSet<>();
        for (String listing : ConfigUtils.readDirContents(ConfigUtils.workerPidsRoot(_conf, _workerId))) {
            ret.add(Long.valueOf(listing));
        }

        if (_resourceIsolationManager != null) {
            Set<Long> morePids = _resourceIsolationManager.getRunningPids(_workerId);
            assert (morePids != null);
            ret.addAll(morePids);
        }

        return ret;
    }

    /**
     * @return the user that some operations should be done as.
     *
     * @throws IOException on any error
     */
    protected String getWorkerUser() throws IOException {
        LOG.info("GET worker-user for {}", _workerId);
        File file = new File(ConfigUtils.workerUserFile(_conf, _workerId));

        if (_ops.fileExists(file)) {
            return _ops.slurpString(file).trim();
        } else if (_assignment != null && _assignment.is_set_owner()) {
            return _assignment.get_owner();
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

    /**
     * Returns the user that the worker process is running as.
     *
     * The default behavior is to launch the worker as the user supervisor is running as (e.g. 'storm')
     *
     * @return the user that the worker process is running as.
     */
    protected String getRunWorkerAsUser() {
        return System.getProperty("user.name");
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
     * Clean up the container partly preparing for restart. By default delete all of the temp directories we are going to get a new
     * worker_id anyways. POST CONDITION: the workerId will be set to null
     *
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
        _workerId = null;
    }

    /**
     * Check if the container is over its memory limit AND needs to be killed. This does not necessarily mean that it just went over the
     * limit.
     *
     * @throws IOException on any error
     */
    public boolean isMemoryLimitViolated(LocalAssignment withUpdatedLimits) throws IOException {
        updateMemoryAccounting();
        return false;
    }

    protected void updateMemoryAccounting() {
        _type.assertFull();
        long used = getMemoryUsageMb();
        long reserved = getMemoryReservationMb();
        containerMemoryTracker.setUsedMemoryMb(_port, _topologyId, used);
        containerMemoryTracker.setReservedMemoryMb(_port, _topologyId, reserved);
    }

    /**
     * Get the total memory used (on and off heap).
     */
    public long getTotalTopologyMemoryUsed() {
        updateMemoryAccounting();
        return containerMemoryTracker.getUsedMemoryMb(_topologyId);
    }

    /**
     * Get the total memory reserved.
     *
     * @param withUpdatedLimits the local assignment with shared memory
     * @return the total memory reserved.
     */
    public long getTotalTopologyMemoryReserved(LocalAssignment withUpdatedLimits) {
        updateMemoryAccounting();
        long ret =
            containerMemoryTracker.getReservedMemoryMb(_topologyId);
        if (withUpdatedLimits.is_set_total_node_shared()) {
            ret += withUpdatedLimits.get_total_node_shared();
        }
        return ret;
    }

    /**
     * Get the number of workers for this topology.
     */
    public long getTotalWorkersForThisTopology() {
        return containerMemoryTracker.getAssignedWorkerCount(_topologyId);
    }

    /**
     * Get the current memory usage of this container.
     */
    public long getMemoryUsageMb() {
        return 0;
    }

    /**
     * Get the current memory reservation of this container.
     */
    public long getMemoryReservationMb() {
        return 0;
    }

    /**
     * Launch the process for the first time. PREREQUISITE: setup has run and passed
     *
     * @throws IOException on any error
     */
    public abstract void launch() throws IOException;

    /**
     * Restart the processes in this container. PREREQUISITE: cleanUpForRestart has run and passed
     *
     * @throws IOException on any error
     */
    public abstract void relaunch() throws IOException;

    /**
     * Return true if the main process exited, else false. This is just best effort return false if unknown.
     */
    public abstract boolean didMainProcessExit();

    /**
     * Run a profiling request.
     *
     * @param request the request to run
     * @param stop is this a stop request?
     * @return true if it succeeded, else false
     *
     * @throws IOException on any error
     * @throws InterruptedException if running the command is interrupted.
     */
    public abstract boolean runProfiling(ProfileRequest request, boolean stop) throws IOException, InterruptedException;

    /**
     * Get the id of the container or null if there is no worker id right now.
     */
    public String getWorkerId() {
        return _workerId;
    }

    /**
     * Send worker metrics to Nimbus.
     */
    void processMetrics(OnlyLatestExecutor<Integer> exec, WorkerMetricsProcessor processor) {
        try {
            Optional<Long> usedMemoryForPort = containerMemoryTracker.getUsedMemoryMb(_port);
            if (usedMemoryForPort.isPresent()) {
                // Make sure we don't process too frequently.
                long nextMetricProcessTime = this.lastMetricProcessTime + 60L * 1000L;
                long currentTimeMsec = System.currentTimeMillis();
                if (currentTimeMsec < nextMetricProcessTime) {
                    return;
                }

                String hostname = Utils.hostname();

                // create metric for memory
                long timestamp = System.currentTimeMillis();
                WorkerMetricPoint workerMetric = new WorkerMetricPoint(MEMORY_USED_METRIC, timestamp, usedMemoryForPort.get(), SYSTEM_COMPONENT_ID,
                    INVALID_EXECUTOR_ID, INVALID_STREAM_ID);

                WorkerMetricList metricList = new WorkerMetricList();
                metricList.add_to_metrics(workerMetric);
                WorkerMetrics metrics = new WorkerMetrics(_topologyId, _port, hostname, metricList);

                exec.execute(_port, () -> {
                    try {
                        processor.processWorkerMetrics(_conf, metrics);
                    } catch (MetricException e) {
                        LOG.error("Failed to process metrics", e);
                    }
                });
            }
        } catch (Exception e) {
            LOG.error("Failed to process metrics", e);
        } finally {
            this.lastMetricProcessTime = System.currentTimeMillis();
        }
    }

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
}
