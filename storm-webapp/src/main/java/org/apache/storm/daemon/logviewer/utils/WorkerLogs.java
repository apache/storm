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

package org.apache.storm.daemon.logviewer.utils;

import static java.util.stream.Collectors.toCollection;
import static org.apache.storm.Config.SUPERVISOR_RUN_WORKER_AS_USER;
import static org.apache.storm.Config.TOPOLOGY_SUBMITTER_USER;

import com.codahale.metrics.Meter;
import com.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.storm.Config;
import org.apache.storm.daemon.supervisor.ClientSupervisorUtils;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.daemon.utils.PathUtil;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.utils.LruMap;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServerConfigUtils;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.jooq.lambda.Unchecked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that knows about how to operate with worker log directory.
 */
public class WorkerLogs {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerLogs.class);

    public static final String WORKER_YAML = "worker.yaml";
    
    private final Meter numSetPermissionsExceptions;
    
    private final Map<String, Object> stormConf;
    private final Path logRootDir;
    private final DirectoryCleaner directoryCleaner;
    private final LruMap<String, Integer> mapTopologyIdToHeartbeatTimeout;

    /**
     * Constructor.
     *
     * @param stormConf storm configuration
     * @param logRootDir the log root directory
     * @param metricsRegistry The logviewer metrics registry
     */
    public WorkerLogs(Map<String, Object> stormConf, Path logRootDir, StormMetricsRegistry metricsRegistry) {
        this.stormConf = stormConf;
        this.logRootDir = logRootDir.toAbsolutePath().normalize();
        this.numSetPermissionsExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_SET_PERMISSION_EXCEPTIONS);
        this.directoryCleaner = new DirectoryCleaner(metricsRegistry);
        this.mapTopologyIdToHeartbeatTimeout = new LruMap<>(200);
    }

    /**
     * Set permission of log file so that logviewer can serve the file.
     *
     * @param fileName log file
     */
    public void setLogFilePermission(String fileName) throws IOException {
        Path absFile = logRootDir.resolve(fileName).toAbsolutePath().normalize();
        if (!absFile.startsWith(logRootDir)) {
            return;
        }
        boolean runAsUser = ObjectReader.getBoolean(stormConf.get(SUPERVISOR_RUN_WORKER_AS_USER), false);
        Path parent = logRootDir.resolve(fileName).getParent();
        Optional<Path> mdFile = (parent == null) ? Optional.empty() : getMetadataFileForWorkerLogDir(parent);
        Optional<String> topoOwner = mdFile.isPresent()
                ? Optional.of(getTopologyOwnerFromMetadataFile(mdFile.get().toAbsolutePath().normalize()))
                : Optional.empty();

        if (runAsUser && topoOwner.isPresent() && absFile.toFile().exists() && !Files.isReadable(absFile)) {
            LOG.debug("Setting permissions on file {} with topo-owner {}", fileName, topoOwner);
            try {
                ClientSupervisorUtils.processLauncherAndWait(stormConf, topoOwner.get(),
                        Lists.newArrayList("blob", absFile.toAbsolutePath().normalize().toString()), null,
                        "setup group read permissions for file: " + fileName);
            } catch (IOException e) {
                numSetPermissionsExceptions.mark();
                throw e;
            }
        }
    }

    /**
     * Return a list of all log files from worker directories in root log directory.
     */
    public List<Path> getAllLogsForRootDir() throws IOException {
        List<Path> files = new ArrayList<>();
        Set<Path> topoDirFiles = getAllWorkerDirs();
        if (topoDirFiles != null) {
            for (Path portDir : topoDirFiles) {
                files.addAll(directoryCleaner.getFilesForDir(portDir));
            }
        }

        return files;
    }

    /**
     * Return a set of all worker directories in all topology directories in root log directory.
     */
    public Set<Path> getAllWorkerDirs() {
        try (Stream<Path> topoDirs = Files.list(logRootDir)) {
            return topoDirs
                .filter(Files::isDirectory)
                .flatMap(Unchecked.function(Files::list)) //Worker dirs
                .filter(Files::isDirectory)
                .collect(Collectors.toCollection(TreeSet::new));
        } catch (IOException e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    /**
     * Return a sorted set of paths that were written by workers that are now active.
     */
    public SortedSet<Path> getAliveWorkerDirs() throws IOException {
        Set<String> aliveIds = getAliveIds(Time.currentTimeSecs());
        Set<Path> logDirs = getAllWorkerDirs();
        return getLogDirs(logDirs, (wid) -> aliveIds.contains(wid));
    }

    /**
     * Return a metadata file (worker.yaml) for given worker log directory.
     * @param logDir worker log directory
     */
    public Optional<Path> getMetadataFileForWorkerLogDir(Path logDir) throws IOException {
        Path metaFile = logDir.resolve(WORKER_YAML);
        if (metaFile.toFile().exists()) {
            return Optional.of(metaFile);
        } else {
            LOG.warn("Could not find {} to clean up for {}", metaFile.toAbsolutePath().normalize(), logDir);
            return Optional.empty();
        }
    }

    /**
     * Return worker id from worker meta file.
     *
     * @param metaFile metadata file
     */
    public String getWorkerIdFromMetadataFile(Path metaFile) {
        Map<String, Object> map = (Map<String, Object>) Utils.readYamlFile(metaFile.toString());
        return ObjectReader.getString(map == null ? null : map.get("worker-id"), null);
    }

    /**
     * Return topology owner from worker meta file.
     *
     * @param metaFile metadata file
     */
    public String getTopologyOwnerFromMetadataFile(Path metaFile) {
        Map<String, Object> map = (Map<String, Object>) Utils.readYamlFile(metaFile.toString());
        return ObjectReader.getString(map.get(TOPOLOGY_SUBMITTER_USER), null);
    }

    /**
     * Retrieve the set of alive worker IDs.
     *
     * @param nowSecs current time in seconds
     */
    public Set<String> getAliveIds(int nowSecs) throws IOException {
        return SupervisorUtils.readWorkerHeartbeats(stormConf).entrySet().stream()
                .filter(entry -> Objects.nonNull(entry.getValue()) && !isTimedOut(nowSecs, entry))
                .map(Map.Entry::getKey)
                .collect(toCollection(TreeSet::new));
    }

    private boolean isTimedOut(int nowSecs, Map.Entry<String, LSWorkerHeartbeat> entry) {
        LSWorkerHeartbeat hb = entry.getValue();
        int workerLogTimeout = getTopologyTimeout(hb);
        return (nowSecs - hb.get_time_secs()) >= workerLogTimeout;
    }

    private int getTopologyTimeout(LSWorkerHeartbeat hb) {
        String topoId = hb.get_topology_id();
        Integer cachedTimeout = mapTopologyIdToHeartbeatTimeout.get(topoId);
        if (cachedTimeout != null) {
            return cachedTimeout;
        } else {
            int timeout = getWorkerLogTimeout(stormConf, topoId, hb.get_port());
            mapTopologyIdToHeartbeatTimeout.put(topoId, timeout);
            return timeout;
        }
    }

    private int getWorkerLogTimeout(Map<String, Object> conf, String topologyId, int port) {
        int defaultWorkerLogTimeout = ObjectReader.getInt(conf.get(Config.SUPERVISOR_WORKER_TIMEOUT_SECS));
        File file = ServerConfigUtils.getLogMetaDataFile(conf, topologyId, port);
        Map<String, Object> map = (Map<String, Object>) Utils.readYamlFile(file.getAbsolutePath());
        if (map == null) {
            return defaultWorkerLogTimeout;
        }

        return (Integer) map.getOrDefault(Config.TOPOLOGY_WORKER_TIMEOUT_SECS, defaultWorkerLogTimeout);
    }

    /**
     * Finds directories for specific worker ids that can be cleaned up.
     *
     * @param logDirs directories to check whether they're worker directories or not
     * @param predicate a check on a worker id to see if the log dir should be included
     * @return directories that can be cleaned up.
     */
    public SortedSet<Path> getLogDirs(Set<Path> logDirs, Predicate<String> predicate) {
        // we could also make this static, but not to do it due to mock
        TreeSet<Path> ret = new TreeSet<>();
        for (Path logDir: logDirs) {
            String workerId = "";
            try {
                Optional<Path> metaFile = getMetadataFileForWorkerLogDir(logDir);
                if (metaFile.isPresent()) {
                    workerId = getWorkerIdFromMetadataFile(metaFile.get().toAbsolutePath().normalize());
                    if (workerId == null) {
                        workerId = "";
                    }
                }
            } catch (IOException e) {
                LOG.warn("Error trying to find worker.yaml in {}", logDir, e);
            }
            if (predicate.test(workerId)) {
                ret.add(logDir);
            }
        }
        return ret;
    }

    /**
     * Return the path of the worker log with the format of topoId/port/worker.log.*
     *
     * @param file worker log
     */
    public static String getTopologyPortWorkerLog(Path file) {
        return PathUtil.truncatePathToLastElements(file.toAbsolutePath().normalize(), 3).toString();
    }

}
