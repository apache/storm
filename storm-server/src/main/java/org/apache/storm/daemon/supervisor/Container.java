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
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
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
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServerConfigUtils;
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
    private static final Map<String, Integer> cachedUserToUidMap = new ConcurrentHashMap<>();

    private final Meter numCleanupExceptions;
    private final Meter numKillExceptions;
    private final Meter numForceKillExceptions;
    private final Meter numForceKill;
    private final Timer shutdownDuration;
    private final Timer cleanupDuration;
    protected final Map<String, Object> conf;
    protected final Map<String, Object> topoConf; //Not set if RECOVER_PARTIAL
    protected final String topologyId; //Not set if RECOVER_PARTIAL
    protected final String supervisorId;
    protected final int supervisorPort;
    protected final int port; //Not set if RECOVER_PARTIAL
    protected final LocalAssignment assignment; //Not set if RECOVER_PARTIAL
    protected final AdvancedFSOps ops;
    protected final ResourceIsolationInterface resourceIsolationManager;
    protected final boolean symlinksDisabled;
    protected String workerId;
    protected ContainerType type;
    protected ContainerMemoryTracker containerMemoryTracker;
    private long lastMetricProcessTime = 0L;
    private Timer.Context shutdownTimer = null;
    protected boolean runAsUser;
    private String cachedUser;

    /**
     * Create a new Container.
     *
     * @param type the type of container being made.
     * @param conf the supervisor config
     * @param supervisorId the ID of the supervisor this is a part of.
     * @param supervisorPort the thrift server port of the supervisor this is a part of.
     * @param port the port the container is on. Should be <= 0 if only a partial recovery @param assignment
     *     the assignment for this container. Should be null if only a partial recovery.
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

        symlinksDisabled = (boolean) conf.getOrDefault(Config.DISABLE_SYMLINKS, false);

        if (ops == null) {
            ops = AdvancedFSOps.make(conf);
        }

        this.workerId = workerId;
        this.type = type;
        this.port = port;
        this.ops = ops;
        this.conf = conf;
        this.supervisorId = supervisorId;
        this.supervisorPort = supervisorPort;
        this.resourceIsolationManager = resourceIsolationManager;
        this.assignment = assignment;

        runAsUser = ObjectReader.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false);
        if (runAsUser && Utils.isOnWindows()) {
            throw new UnsupportedOperationException("ERROR: Windows doesn't support running workers as different users yet");
        }

        if (this.type.isOnlyKillable()) {
            assert (this.assignment == null);
            assert (this.port <= 0);
            assert (this.workerId != null);
            topologyId = null;
            this.topoConf = null;
        } else {
            assert (assignment != null);
            assert (port > 0);
            topologyId = assignment.get_topology_id();
            if (!this.ops.doRequiredTopoFilesExist(this.conf, topologyId)) {
                LOG.info(
                    "Missing topology storm code, so can't launch  worker with assignment {} for this supervisor {} on port {} with id {}",
                        this.assignment,
                        this.supervisorId, this.port, this.workerId);
                throw new ContainerRecoveryException("Missing required topology files...");
            }
            if (topoConf == null) {
                this.topoConf = readTopoConf();
            } else {
                //For testing...
                this.topoConf = topoConf;
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
        return "topo:" + topologyId + " worker:" + workerId;
    }

    protected Map<String, Object> readTopoConf() throws IOException {
        assert (topologyId != null);
        return ConfigUtils.readSupervisorStormConf(conf, topologyId);
    }

    @Override
    public void kill() throws IOException {
        LOG.info("Killing {}:{}", supervisorId, workerId);
        if (shutdownTimer == null) {
            shutdownTimer = shutdownDuration.time();
        }
        try {
            if (resourceIsolationManager != null) {
                resourceIsolationManager.kill(getWorkerUser(), workerId);
            }
        } catch (IOException e) {
            numKillExceptions.mark();
            throw e;
        }
    }

    @Override
    public void forceKill() throws IOException {
        LOG.info("Force Killing {}:{}", supervisorId, workerId);
        numForceKill.mark();
        try {
            if (resourceIsolationManager != null) {
                resourceIsolationManager.forceKill(getWorkerUser(), workerId);
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
        LocalState localState = ConfigUtils.workerState(conf, workerId);
        LSWorkerHeartbeat hb = localState.getWorkerHeartBeat();
        LOG.trace("{}: Reading heartbeat {}", workerId, hb);
        return hb;
    }

    @Override
    public boolean areAllProcessesDead() throws IOException {
        boolean allDead = true;
        if (resourceIsolationManager != null) {
            allDead = resourceIsolationManager.areAllProcessesDead(getWorkerUser(), workerId);
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
            containerMemoryTracker.remove(port);
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
        type.assertFull();
        if (!ops.doRequiredTopoFilesExist(conf, topologyId)) {
            LOG.info("Missing topology storm code, so can't launch  worker with assignment {} for this supervisor {} on port {} with id {}",
                    assignment,
                    supervisorId, port, workerId);
            throw new IllegalStateException("Not all needed files are here!!!!");
        }
        LOG.info("Setting up {}:{}", supervisorId, workerId);

        ops.forceMkdir(new File(ConfigUtils.workerPidsRoot(conf, workerId)));
        ops.forceMkdir(new File(ConfigUtils.workerTmpRoot(conf, workerId)));
        ops.forceMkdir(new File(ConfigUtils.workerHeartbeatsRoot(conf, workerId)));

        File workerArtifacts = new File(ConfigUtils.workerArtifactsRoot(conf, topologyId, port));
        if (!ops.fileExists(workerArtifacts)) {
            ops.forceMkdir(workerArtifacts);
            ops.setupWorkerArtifactsDir(assignment.get_owner(), workerArtifacts);
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
    protected void writeLogMetadata(String user) throws IOException {
        type.assertFull();
        Map<String, Object> data = new HashMap<>();
        data.put(Config.TOPOLOGY_SUBMITTER_USER, user);
        data.put("worker-id", workerId);

        Set<String> logsGroups = new HashSet<>();
        if (topoConf.get(DaemonConfig.LOGS_GROUPS) != null) {
            List<String> groups = ObjectReader.getStrings(topoConf.get(DaemonConfig.LOGS_GROUPS));
            logsGroups.addAll(groups);
        }
        if (topoConf.get(Config.TOPOLOGY_GROUPS) != null) {
            List<String> topGroups = ObjectReader.getStrings(topoConf.get(Config.TOPOLOGY_GROUPS));
            logsGroups.addAll(topGroups);
        }
        data.put(DaemonConfig.LOGS_GROUPS, logsGroups.toArray());

        Set<String> logsUsers = new HashSet<>();
        if (topoConf.get(DaemonConfig.LOGS_USERS) != null) {
            List<String> logUsers = ObjectReader.getStrings(topoConf.get(DaemonConfig.LOGS_USERS));
            logsUsers.addAll(logUsers);
        }
        if (topoConf.get(Config.TOPOLOGY_USERS) != null) {
            List<String> topUsers = ObjectReader.getStrings(topoConf.get(Config.TOPOLOGY_USERS));
            logsUsers.addAll(topUsers);
        }
        data.put(DaemonConfig.LOGS_USERS, logsUsers.toArray());

        if (topoConf.get(Config.TOPOLOGY_WORKER_TIMEOUT_SECS) != null) {
            int topoTimeout = ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_WORKER_TIMEOUT_SECS));
            int defaultWorkerTimeout = ObjectReader.getInt(conf.get(Config.SUPERVISOR_WORKER_TIMEOUT_SECS));
            topoTimeout = Math.max(topoTimeout, defaultWorkerTimeout);
            data.put(Config.TOPOLOGY_WORKER_TIMEOUT_SECS, topoTimeout);
        }

        File file = ServerConfigUtils.getLogMetaDataFile(conf, topologyId, port);

        Yaml yaml = new Yaml();
        try (Writer writer = ops.getWriter(file)) {
            yaml.dump(data, writer);
        }
    }

    /**
     * Create symlink from the containers directory/artifacts to the artifacts directory.
     *
     * @throws IOException on any error
     */
    protected void createArtifactsLink() throws IOException {
        type.assertFull();
        if (!symlinksDisabled) {
            File workerDir = new File(ConfigUtils.workerRoot(conf, workerId));
            File topoDir = new File(ConfigUtils.workerArtifactsRoot(conf, topologyId, port));
            if (ops.fileExists(workerDir)) {
                LOG.debug("Creating symlinks for worker-id: {} topology-id: {} to its port artifacts directory", workerId, topologyId);
                ops.createSymlink(new File(ConfigUtils.workerArtifactsSymlink(conf, workerId)), topoDir);
            }
        }
    }

    /**
     * Create symlinks for each of the blobs from the container's directory to corresponding links in the storm dist directory.
     *
     * @throws IOException on any error.
     */
    protected void createBlobstoreLinks() throws IOException {
        type.assertFull();
        String stormRoot = ConfigUtils.supervisorStormDistRoot(conf, topologyId);
        String workerRoot = ConfigUtils.workerRoot(conf, workerId);

        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> blobstoreMap = (Map<String, Map<String, Object>>) topoConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
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

        if (!symlinksDisabled) {
            LOG.info("Creating symlinks for worker-id: {} storm-id: {} for files({}): {}", workerId, topologyId, resourceFileNames.size(),
                resourceFileNames);
            if (targetResourcesDir.exists()) {
                ops.createSymlink(new File(workerRoot, ServerConfigUtils.RESOURCES_SUBDIR), targetResourcesDir);
            } else {
                LOG.info("Topology jar for worker-id: {} storm-id: {} does not contain re sources directory {}.", workerId, topologyId,
                    targetResourcesDir.toString());
            }
            for (String fileName : blobFileNames) {
                ops.createSymlink(new File(workerRoot, fileName),
                    new File(stormRoot, fileName));
            }
        } else if (blobFileNames.size() > 0) {
            LOG.warn("Symlinks are disabled, no symlinks created for blobs {}", blobFileNames);
        }
    }

    /**
     * Get the user of the worker.
     * @return the user that some operations should be done as.
     * @throws IOException on any error
     */
    protected String getWorkerUser() throws IOException {
        if (cachedUser != null) {
            return cachedUser;
        }

        LOG.info("GET worker-user for {}", workerId);
        File file = new File(ConfigUtils.workerUserFile(conf, workerId));
        if (ops.fileExists(file)) {
            cachedUser = ops.slurpString(file).trim();
            if (!StringUtils.isBlank(cachedUser)) {
                return cachedUser;
            }
        }

        if (assignment != null && assignment.is_set_owner()) {
            cachedUser = assignment.get_owner();
            if (!StringUtils.isBlank(cachedUser)) {
                return cachedUser;
            }
        }

        if (ConfigUtils.isLocalMode(conf)) {
            cachedUser = System.getProperty("user.name");
            return cachedUser;
        } else {
            File f = new File(ConfigUtils.workerArtifactsRoot(conf));
            if (f.exists()) {
                cachedUser = Files.getOwner(f.toPath()).getName();
                if (!StringUtils.isBlank(cachedUser)) {
                    return cachedUser;
                }
            }
            throw new IllegalStateException("Could not recover the user for " + workerId);
        }
    }

    protected void saveWorkerUser(String user) throws IOException {
        type.assertFull();
        LOG.info("SET worker-user {} {}", workerId, user);
        ops.dump(new File(ConfigUtils.workerUserFile(conf, workerId)), user);
    }

    protected void deleteSavedWorkerUser() throws IOException {
        LOG.info("REMOVE worker-user {}", workerId);
        ops.deleteIfExists(new File(ConfigUtils.workerUserFile(conf, workerId)));
    }

    /**
     * Clean up the container partly preparing for restart. By default delete all of the temp directories we are going to get a new
     * worker_id anyways. POST CONDITION: the workerId will be set to null
     *
     * @throws IOException on any error
     */
    public void cleanUpForRestart() throws IOException {
        LOG.info("Cleaning up {}:{}", supervisorId, workerId);
        String user = getWorkerUser();

        //clean up for resource isolation if enabled
        if (resourceIsolationManager != null) {
            resourceIsolationManager.cleanup(user, workerId, port);
        }

        //Always make sure to clean up everything else before worker directory
        //is removed since that is what is going to trigger the retry for cleanup
        ops.deleteIfExists(new File(ConfigUtils.workerHeartbeatsRoot(conf, workerId)), user, workerId);
        ops.deleteIfExists(new File(ConfigUtils.workerPidsRoot(conf, workerId)), user, workerId);
        ops.deleteIfExists(new File(ConfigUtils.workerTmpRoot(conf, workerId)), user, workerId);
        ops.deleteIfExists(new File(ConfigUtils.workerRoot(conf, workerId)), user, workerId);
        deleteSavedWorkerUser();
        workerId = null;
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
        type.assertFull();
        long used = getMemoryUsageMb();
        long reserved = getMemoryReservationMb();
        containerMemoryTracker.setUsedMemoryMb(port, topologyId, used);
        containerMemoryTracker.setReservedMemoryMb(port, topologyId, reserved);
    }

    /**
     * Get the total memory used (on and off heap).
     */
    public long getTotalTopologyMemoryUsed() {
        updateMemoryAccounting();
        return containerMemoryTracker.getUsedMemoryMb(topologyId);
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
            containerMemoryTracker.getReservedMemoryMb(topologyId);
        if (withUpdatedLimits.is_set_total_node_shared()) {
            ret += withUpdatedLimits.get_total_node_shared();
        }
        return ret;
    }

    /**
     * Get the number of workers for this topology.
     */
    public long getTotalWorkersForThisTopology() {
        return containerMemoryTracker.getAssignedWorkerCount(topologyId);
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
        return workerId;
    }

    /**
     * Send worker metrics to Nimbus.
     */
    void processMetrics(OnlyLatestExecutor<Integer> exec, WorkerMetricsProcessor processor) {
        try {
            Optional<Long> usedMemoryForPort = containerMemoryTracker.getUsedMemoryMb(port);
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
                WorkerMetricPoint workerMetric = new WorkerMetricPoint(MEMORY_USED_METRIC,
                        timestamp,
                        usedMemoryForPort.get(),
                        SYSTEM_COMPONENT_ID,
                        INVALID_EXECUTOR_ID, INVALID_STREAM_ID);

                WorkerMetricList metricList = new WorkerMetricList();
                metricList.add_to_metrics(workerMetric);
                WorkerMetrics metrics = new WorkerMetrics(topologyId, port, hostname, metricList);

                exec.execute(port, () -> {
                    try {
                        processor.processWorkerMetrics(conf, metrics);
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

    public enum ContainerType {
        LAUNCH(false, false),
        RECOVER_FULL(true, false),
        RECOVER_PARTIAL(true, true);

        private final boolean recovery;
        private final boolean onlyKillable;

        ContainerType(boolean recovery, boolean onlyKillable) {
            this.recovery = recovery;
            this.onlyKillable = onlyKillable;
        }

        public boolean isRecovery() {
            return recovery;
        }

        public void assertFull() {
            if (onlyKillable) {
                throw new IllegalStateException("Container is only Killable.");
            }
        }

        public boolean isOnlyKillable() {
            return onlyKillable;
        }
    }
}
