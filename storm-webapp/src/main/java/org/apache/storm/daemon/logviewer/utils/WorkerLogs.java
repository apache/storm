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
import static java.util.stream.Collectors.toMap;
import static org.apache.storm.Config.SUPERVISOR_RUN_WORKER_AS_USER;
import static org.apache.storm.Config.TOPOLOGY_SUBMITTER_USER;

import com.codahale.metrics.Meter;
import com.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.storm.daemon.supervisor.ClientSupervisorUtils;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.daemon.utils.PathUtil;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.utils.ObjectReader;
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
    private final File logRootDir;
    private final DirectoryCleaner directoryCleaner;

    /**
     * Constructor.
     *
     * @param stormConf storm configuration
     * @param logRootDir the log root directory
     * @param metricsRegistry The logviewer metrics registry
     */
    public WorkerLogs(Map<String, Object> stormConf, File logRootDir, StormMetricsRegistry metricsRegistry) {
        this.stormConf = stormConf;
        this.logRootDir = logRootDir;
        this.numSetPermissionsExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_SET_PERMISSION_EXCEPTIONS);
        this.directoryCleaner = new DirectoryCleaner(metricsRegistry);
    }

    /**
     * Set permission of log file so that logviewer can serve the file.
     *
     * @param fileName log file
     */
    public void setLogFilePermission(String fileName) throws IOException {
        File file = new File(logRootDir, fileName).getCanonicalFile();
        boolean runAsUser = ObjectReader.getBoolean(stormConf.get(SUPERVISOR_RUN_WORKER_AS_USER), false);
        File parent = new File(logRootDir, fileName).getParentFile();
        Optional<File> mdFile = (parent == null) ? Optional.empty() : getMetadataFileForWorkerLogDir(parent);
        Optional<String> topoOwner = mdFile.isPresent()
                ? Optional.of(getTopologyOwnerFromMetadataFile(mdFile.get().getCanonicalPath()))
                : Optional.empty();

        if (runAsUser && topoOwner.isPresent() && file.exists() && !Files.isReadable(file.toPath())) {
            LOG.debug("Setting permissions on file {} with topo-owner {}", fileName, topoOwner);
            try {
                ClientSupervisorUtils.processLauncherAndWait(stormConf, topoOwner.get(),
                        Lists.newArrayList("blob", file.getCanonicalPath()), null,
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
    public List<File> getAllLogsForRootDir() throws IOException {
        List<File> files = new ArrayList<>();
        Set<File> topoDirFiles = getAllWorkerDirs();
        if (topoDirFiles != null) {
            for (File portDir : topoDirFiles) {
                files.addAll(directoryCleaner.getFilesForDir(portDir));
            }
        }

        return files;
    }

    /**
     * Return a set of all worker directories in root log directory.
     */
    public Set<File> getAllWorkerDirs() {
        File[] rootDirFiles = logRootDir.listFiles();
        if (rootDirFiles != null) {
            return Arrays.stream(rootDirFiles).flatMap(topoDir -> {
                File[] topoFiles = topoDir.listFiles();
                return topoFiles != null ? Arrays.stream(topoFiles) : Stream.empty();
            }).collect(toCollection(TreeSet::new));
        }

        return new TreeSet<>();
    }

    /**
     * Return a sorted set of java.io.Files that were written by workers that are now active.
     */
    public SortedSet<File> getAliveWorkerDirs() {
        Set<String> aliveIds = getAliveIds(Time.currentTimeSecs());
        Set<File> logDirs = getAllWorkerDirs();
        return getLogDirs(logDirs, (wid) -> aliveIds.contains(wid));
    }

    /**
     * Return a metadata file (worker.yaml) for given worker log directory.
     * @param logDir worker log directory
     */
    public Optional<File> getMetadataFileForWorkerLogDir(File logDir) throws IOException {
        File metaFile = new File(logDir, WORKER_YAML);
        if (metaFile.exists()) {
            return Optional.of(metaFile);
        } else {
            LOG.warn("Could not find {} to clean up for {}", metaFile.getCanonicalPath(), logDir);
            return Optional.empty();
        }
    }

    /**
     * Return worker id from worker meta file.
     *
     * @param metaFile metadata file
     */
    public String getWorkerIdFromMetadataFile(String metaFile) {
        Map<String, Object> map = (Map<String, Object>) Utils.readYamlFile(metaFile);
        return ObjectReader.getString(map == null ? null : map.get("worker-id"), null);
    }

    /**
     * Return topology owner from worker meta file.
     *
     * @param metaFile metadata file
     */
    public String getTopologyOwnerFromMetadataFile(String metaFile) {
        Map<String, Object> map = (Map<String, Object>) Utils.readYamlFile(metaFile);
        return ObjectReader.getString(map.get(TOPOLOGY_SUBMITTER_USER), null);
    }

    /**
     * Retrieve the set of alive worker IDs.
     *
     * @param nowSecs current time in seconds
     */
    public Set<String> getAliveIds(int nowSecs) {
        return SupervisorUtils.readWorkerHeartbeats(stormConf).entrySet().stream()
                .filter(entry -> Objects.nonNull(entry.getValue())
                        && !SupervisorUtils.isWorkerHbTimedOut(nowSecs, entry.getValue(), stormConf))
                .map(Map.Entry::getKey)
                .collect(toCollection(TreeSet::new));
    }

    /**
     * Finds directories for specific worker ids that can be cleaned up.
     *
     * @param logDirs directories to check whether they're worker directories or not
     * @param predicate a check on a worker id to see if the log dir should be included
     * @return directories that can be cleaned up.
     */
    public SortedSet<File> getLogDirs(Set<File> logDirs, Predicate<String> predicate) {
        // we could also make this static, but not to do it due to mock
        TreeSet<File> ret = new TreeSet<>();
        for (File logDir: logDirs) {
            String workerId = "";
            try {
                Optional<File> metaFile = getMetadataFileForWorkerLogDir(logDir);
                if (metaFile.isPresent()) {
                    workerId = getWorkerIdFromMetadataFile(metaFile.get().getCanonicalPath());
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
    public static String getTopologyPortWorkerLog(File file) {
        try {
            return PathUtil.truncatePathToLastElements(file.getCanonicalFile().toPath(), 3).toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
