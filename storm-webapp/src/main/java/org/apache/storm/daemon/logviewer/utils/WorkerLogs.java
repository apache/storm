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
import static org.apache.storm.daemon.utils.ListFunctionalSupport.takeLast;

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
import java.util.stream.Stream;

import org.apache.storm.daemon.supervisor.ClientSupervisorUtils;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.jooq.lambda.Unchecked;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that knows about how to operate with worker log directory.
 */
public class WorkerLogs {
    private static final Logger LOG = LoggerFactory.getLogger(LogCleaner.class);

    public static final String WORKER_YAML = "worker.yaml";
    private final Map<String, Object> stormConf;
    private final File logRootDir;

    /**
     * Constructor.
     *
     * @param stormConf storm configuration
     * @param logRootDir the log root directory
     */
    public WorkerLogs(Map<String, Object> stormConf, File logRootDir) {
        this.stormConf = stormConf;
        this.logRootDir = logRootDir;
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
            ClientSupervisorUtils.processLauncherAndWait(stormConf, topoOwner.get(),
                    Lists.newArrayList("blob", file.getCanonicalPath()), null,
                    "setup group read permissions for file: " + fileName);
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
                files.addAll(DirectoryCleaner.getFilesForDir(portDir));
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
    public SortedSet<String> getAliveWorkerDirs() throws Exception {
        Set<String> aliveIds = getAliveIds(Time.currentTimeSecs());
        Set<File> logDirs = getAllWorkerDirs();
        Map<String, File> idToDir = identifyWorkerLogDirs(logDirs);

        return idToDir.entrySet().stream()
                .filter(entry -> aliveIds.contains(entry.getKey()))
                .map(Unchecked.function(entry -> entry.getValue().getCanonicalPath()))
                .collect(toCollection(TreeSet::new));
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
        return ObjectReader.getString(map.get("worker-id"), null);
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
    public Set<String> getAliveIds(int nowSecs) throws Exception {
        return SupervisorUtils.readWorkerHeartbeats(stormConf).entrySet().stream()
                .filter(entry -> Objects.nonNull(entry.getValue())
                        && !SupervisorUtils.isWorkerHbTimedOut(nowSecs, entry.getValue(), stormConf))
                .map(Map.Entry::getKey)
                .collect(toCollection(TreeSet::new));
    }

    /**
     * Finds a worker ID for each directory in set and return it as map.
     *
     * @param logDirs directories to check whether they're worker directories or not
     * @return the pair of worker ID, directory. worker ID will be an empty string if the directory is not a worker directory.
     */
    public Map<String, File> identifyWorkerLogDirs(Set<File> logDirs) {
        // we could also make this static, but not to do it due to mock
        return logDirs.stream().map(Unchecked.function(logDir -> {
            Optional<File> metaFile = getMetadataFileForWorkerLogDir(logDir);

            return metaFile.map(Unchecked.function(m -> new Tuple2<>(getWorkerIdFromMetadataFile(m.getCanonicalPath()), logDir)))
                    .orElse(new Tuple2<>("", logDir));
        })).collect(toMap(Tuple2::v1, Tuple2::v2));
    }

    /**
     * Return the path of the worker log with the format of topoId/port/worker.log.*
     *
     * @param file worker log
     */
    public static String getTopologyPortWorkerLog(File file) {
        try {
            String[] splitted = file.getCanonicalPath().split(Utils.FILE_PATH_SEPARATOR);
            List<String> split = takeLast(Arrays.asList(splitted), 3);

            return String.join(Utils.FILE_PATH_SEPARATOR, split);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
