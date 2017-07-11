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

package org.apache.storm.daemon.wip.logviewer.utils;

import org.apache.commons.io.IOUtils;
import org.apache.storm.StormTimer;
import org.apache.storm.daemon.DirectoryCleaner;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.jooq.lambda.Unchecked;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BinaryOperator;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toMap;
import static org.apache.storm.DaemonConfig.LOGVIEWER_CLEANUP_AGE_MINS;
import static org.apache.storm.DaemonConfig.LOGVIEWER_CLEANUP_INTERVAL_SECS;
import static org.apache.storm.DaemonConfig.LOGVIEWER_MAX_PER_WORKER_LOGS_SIZE_MB;
import static org.apache.storm.DaemonConfig.LOGVIEWER_MAX_SUM_WORKER_LOGS_SIZE_MB;

public class LogCleaner implements Runnable, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(LogCleaner.class);
    public static final String WORKER_YAML = "worker.yaml";

    private final Map<String, Object> stormConf;
    private final Integer intervalSecs;
    private final String logRootDir;
    private StormTimer logviewerCleanupTimer;
    private final long maxSumWorkerLogsSizeMb;
    private long maxPerWorkerLogsSizeMb;

    public LogCleaner(Map<String, Object> stormConf) {
        String logRootDir = ConfigUtils.workerArtifactsRoot(stormConf);

        this.stormConf = stormConf;
        this.intervalSecs = ObjectReader.getInt(stormConf.get(LOGVIEWER_CLEANUP_INTERVAL_SECS), null);
        this.logRootDir = logRootDir;

        maxSumWorkerLogsSizeMb = ObjectReader.getInt(stormConf.get(LOGVIEWER_MAX_SUM_WORKER_LOGS_SIZE_MB));
        maxPerWorkerLogsSizeMb = ObjectReader.getInt(stormConf.get(LOGVIEWER_MAX_PER_WORKER_LOGS_SIZE_MB));
        maxPerWorkerLogsSizeMb = Math.min(maxPerWorkerLogsSizeMb, (long) (maxSumWorkerLogsSizeMb * 0.5));

        LOG.info("configured max total size of worker logs: {} MB, max total size of worker logs per directory: {} MB",
                maxSumWorkerLogsSizeMb, maxPerWorkerLogsSizeMb);
    }

    public void start() {
        if (intervalSecs != null) {
            LOG.debug("starting log cleanup thread at interval: {}", intervalSecs);

            logviewerCleanupTimer = new StormTimer("logviewer-cleanup", (t, e) -> {
                LOG.error("Error when doing logs cleanup", e);
                Utils.exitProcess(20, "Error when doing log cleanup");
            });

            logviewerCleanupTimer.scheduleRecurring(0, intervalSecs, this);
        } else {
            LOG.warn("The interval for log cleanup is not set. Skip starting log cleanup thread.");
        }

    }

    public void close() {
        if (logviewerCleanupTimer != null) {
            try {
                logviewerCleanupTimer.close();
            } catch (Exception ex) {
                throw Utils.wrapInRuntime(ex);
            }
        }
    }

    /**
     * Delete old log dirs for which the workers are no longer alive.
     */
    public void run() {
        try {
            int nowSecs = Time.currentTimeSecs();
            Set<File> oldLogDirs = selectDirsForCleanup(nowSecs * 1000, logRootDir);

            DirectoryCleaner cleaner = new DirectoryCleaner();
            SortedSet<File> deadWorkerDirs = getDeadWorkerDirs(nowSecs, oldLogDirs);

            LOG.debug("log cleanup: now={} old log dirs {} dead worker dirs {}", nowSecs,
                    oldLogDirs.stream().map(File::getName).collect(joining(",")),
                    deadWorkerDirs.stream().map(File::getName).collect(joining(",")));

            deadWorkerDirs.forEach(Unchecked.consumer(dir -> {
                String path = dir.getCanonicalPath();
                LOG.info("Cleaning up: Removing {}", path);

                try {
                    Utils.forceDelete(path);
                    cleanupEmptyTopoDirectory(dir);
                } catch (Exception ex) {
                    LOG.error(ex.getMessage(), ex);
                }
            }));

            perWorkerDirCleanup(new File(logRootDir), maxPerWorkerLogsSizeMb * 1024 * 1024, cleaner);
            globalLogCleanup(new File(logRootDir), maxSumWorkerLogsSizeMb * 1024 * 1024, cleaner);
        } catch (Exception ex) {
            LOG.error("Exception while cleaning up old log.", ex);
        }
    }

    /**
     * Delete the oldest files in each overloaded worker log dir.
     */
    private void perWorkerDirCleanup(File rootDir, long size, DirectoryCleaner cleaner) {
        WorkerLogs.getAllWorkerDirs(rootDir).forEach(Unchecked.consumer(dir -> {
            cleaner.deleteOldestWhileTooLarge(Collections.singletonList(dir), size, true, null);
        }));
    }

    /**
     * Delete the oldest files in overloaded worker-artifacts globally.
     */
    private void globalLogCleanup(File rootDir, long size, DirectoryCleaner cleaner) throws Exception {
        List<File> workerDirs = new ArrayList<>(WorkerLogs.getAllWorkerDirs(rootDir));
        Set<String> aliveWorkerDirs = new HashSet<>(getAliveWorkerDirs(rootDir));

        cleaner.deleteOldestWhileTooLarge(workerDirs, size, false, aliveWorkerDirs);
    }

    /**
     * Return a sorted set of java.io.Files that were written by workers that are now active.
     */
    private SortedSet<String> getAliveWorkerDirs(File rootDir) throws Exception {
        Set<String> aliveIds = getAliveIds(Time.currentTimeSecs());
        Set<File> logDirs = WorkerLogs.getAllWorkerDirs(rootDir);
        Map<String, File> idToDir = identifyWorkerLogDirs(logDirs);

        return idToDir.entrySet().stream()
                .filter(entry -> aliveIds.contains(entry.getKey()))
                .map(Unchecked.function(entry -> entry.getValue().getCanonicalPath()))
                .collect(toCollection(TreeSet::new));
    }

    private Map<String, File> identifyWorkerLogDirs(Set<File> logDirs) {
        return logDirs.stream().map(Unchecked.function(logDir -> {
            Optional<File> metaFile = getMetadataFileForWorkerLogDir(logDir);

            return metaFile.map(Unchecked.function(m -> new Tuple2<>(getWorkerIdFromMetadataFile(m.getCanonicalPath()), logDir)))
                    .orElse(new Tuple2<>("", logDir));
        })).collect(toMap(Tuple2::v1, Tuple2::v2));
    }

    private Optional<File> getMetadataFileForWorkerLogDir(File logDir) throws IOException {
        File metaFile = new File(logDir, WORKER_YAML);
        if (metaFile.exists()) {
            return Optional.of(metaFile);
        } else {
            LOG.warn("Could not find {} to clean up for {}", metaFile.getCanonicalPath(), logDir);
            return Optional.empty();
        }
    }

    private String getWorkerIdFromMetadataFile(String metaFile) {
        Map<String, Object> map = (Map<String, Object>)  Utils.readYamlFile(metaFile);
        return ObjectReader.getString(map.get("worker-id"), null);
    }

    private Set<String> getAliveIds(int nowSecs) throws Exception {
        return SupervisorUtils.readWorkerHeartbeats(stormConf).entrySet().stream()
                .filter(entry -> Objects.nonNull(entry.getValue())
                        && !SupervisorUtils.isWorkerHbTimedOut(nowSecs, entry.getValue(), stormConf))
                .map(Map.Entry::getKey)
                .collect(toCollection(TreeSet::new));
    }

    /**
     * Delete the topo dir if it contains zero port dirs.
     */
    private void cleanupEmptyTopoDirectory(File dir) throws IOException {
        File topoDir = dir.getParentFile();
        if (topoDir.listFiles().length == 0) {
            Utils.forceDelete(topoDir.getCanonicalPath());
        }
    }

    /**
     * Return a sorted set of java.io.Files that were written by workers that are now dead.
     */
    private SortedSet<File> getDeadWorkerDirs(int nowSecs, Set<File> logDirs) throws Exception {
        if (logDirs.isEmpty()) {
            return new TreeSet<>();
        } else {
            Set<String> aliveIds = getAliveIds(nowSecs);
            Map<String, File> idToDir = identifyWorkerLogDirs(logDirs);

            return idToDir.entrySet().stream()
                    .filter(entry -> !aliveIds.contains(entry.getKey()))
                    .map(Map.Entry::getValue)
                    .collect(toCollection(TreeSet::new));
        }
    }

    private Set<File> selectDirsForCleanup(int nowMillis, String rootDir) {
        FileFilter fileFilter = mkFileFilterForLogCleanup(nowMillis);

        return Arrays.stream(new File(rootDir).listFiles())
                .flatMap(topoDir -> Arrays.stream(topoDir.listFiles(fileFilter)))
                .collect(toCollection(TreeSet::new));
    }

    private FileFilter mkFileFilterForLogCleanup(int nowMillis) {
        final int cutoffAgeMillis = cleanupCutoffAgeMillis(nowMillis);
        return file -> !file.isFile() && lastModifiedTimeWorkerLogdir(file) <= cutoffAgeMillis;
    }

    /**
     * Return the last modified time for all log files in a worker's log dir.
     * Using stream rather than File.listFiles is to avoid large mem usage
     * when a directory has too many files.
     */
    private long lastModifiedTimeWorkerLogdir(File logDir) {
        Optional<DirectoryStream<Path>> dirStreamOptional = getStreamForDir(logDir);
        long dirModified = logDir.lastModified();

        if (!dirStreamOptional.isPresent()) {
            return dirModified;
        }

        DirectoryStream<Path> dirStream = dirStreamOptional.get();
        try {
            return StreamSupport.stream(dirStream.spliterator(), false)
                    .reduce(dirModified, (maximum, path) -> {
                        long curr = path.toFile().lastModified();
                        return curr > maximum ? curr : maximum;
                    }, BinaryOperator.maxBy(Long::compareTo));
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            return dirModified;
        } finally {
            if (DirectoryStream.class.isInstance(dirStream)) {
                IOUtils.closeQuietly(dirStream);
            }
        }
    }

    private Optional<DirectoryStream<Path>> getStreamForDir(File file) {
        try {
            return Optional.of(Files.newDirectoryStream(file.toPath()));
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            return Optional.empty();
        }
    }

    private int cleanupCutoffAgeMillis(int nowMillis) {
        return nowMillis - (ObjectReader.getInt(stormConf.get(LOGVIEWER_CLEANUP_AGE_MINS)));
    }
}
