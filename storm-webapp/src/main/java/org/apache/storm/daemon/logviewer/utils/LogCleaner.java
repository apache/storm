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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

import static org.apache.storm.DaemonConfig.LOGVIEWER_CLEANUP_AGE_MINS;
import static org.apache.storm.DaemonConfig.LOGVIEWER_CLEANUP_INTERVAL_SECS;
import static org.apache.storm.DaemonConfig.LOGVIEWER_MAX_PER_WORKER_LOGS_SIZE_MB;
import static org.apache.storm.DaemonConfig.LOGVIEWER_MAX_SUM_WORKER_LOGS_SIZE_MB;

import com.google.common.annotations.VisibleForTesting;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BinaryOperator;
import java.util.stream.StreamSupport;

import org.apache.commons.io.IOUtils;
import org.apache.storm.StormTimer;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.jooq.lambda.Unchecked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cleans dead workers logs and directories.
 */
public class LogCleaner implements Runnable, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(LogCleaner.class);

    private final Map<String, Object> stormConf;
    private final Integer intervalSecs;
    private final File logRootDir;
    private final DirectoryCleaner directoryCleaner;
    private final WorkerLogs workerLogs;

    private StormTimer logviewerCleanupTimer;
    private final long maxSumWorkerLogsSizeMb;
    private long maxPerWorkerLogsSizeMb;

    /**
     * Constuctor.
     *
     * @param stormConf configuration map for Storm cluster
     * @param workerLogs {@link WorkerLogs} instance
     * @param directoryCleaner {@link DirectoryCleaner} instance
     * @param logRootDir root log directory
     */
    public LogCleaner(Map<String, Object> stormConf, WorkerLogs workerLogs, DirectoryCleaner directoryCleaner,
                      File logRootDir) {
        this.stormConf = stormConf;
        this.intervalSecs = ObjectReader.getInt(stormConf.get(LOGVIEWER_CLEANUP_INTERVAL_SECS), null);
        this.logRootDir = logRootDir;
        this.workerLogs = workerLogs;
        this.directoryCleaner = directoryCleaner;

        maxSumWorkerLogsSizeMb = ObjectReader.getInt(stormConf.get(LOGVIEWER_MAX_SUM_WORKER_LOGS_SIZE_MB));
        maxPerWorkerLogsSizeMb = ObjectReader.getInt(stormConf.get(LOGVIEWER_MAX_PER_WORKER_LOGS_SIZE_MB));
        maxPerWorkerLogsSizeMb = Math.min(maxPerWorkerLogsSizeMb, (long) (maxSumWorkerLogsSizeMb * 0.5));

        LOG.info("configured max total size of worker logs: {} MB, max total size of worker logs per directory: {} MB",
                maxSumWorkerLogsSizeMb, maxPerWorkerLogsSizeMb);
    }

    /**
     * Start log cleanup thread.
     */
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

    @Override
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
    @Override
    public void run() {
        try {
            int nowSecs = Time.currentTimeSecs();
            Set<File> oldLogDirs = selectDirsForCleanup(nowSecs * 1000);

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

            perWorkerDirCleanup(maxPerWorkerLogsSizeMb * 1024 * 1024);
            globalLogCleanup(maxSumWorkerLogsSizeMb * 1024 * 1024);
        } catch (Exception ex) {
            LOG.error("Exception while cleaning up old log.", ex);
        }
    }

    /**
     * Delete the oldest files in each overloaded worker log dir.
     */
    @VisibleForTesting
    List<Integer> perWorkerDirCleanup(long size) {
        return workerLogs.getAllWorkerDirs().stream()
                .map(Unchecked.function(dir ->
                        directoryCleaner.deleteOldestWhileTooLarge(Collections.singletonList(dir), size, true, null)))
                .collect(toList());
    }

    /**
     * Delete the oldest files in overloaded worker-artifacts globally.
     */
    @VisibleForTesting
    int globalLogCleanup(long size) throws Exception {
        List<File> workerDirs = new ArrayList<>(workerLogs.getAllWorkerDirs());
        Set<String> aliveWorkerDirs = new HashSet<>(workerLogs.getAliveWorkerDirs());

        return directoryCleaner.deleteOldestWhileTooLarge(workerDirs, size, false, aliveWorkerDirs);
    }

    /**
     * Delete the topo dir if it contains zero port dirs.
     */
    @VisibleForTesting
    void cleanupEmptyTopoDirectory(File dir) throws IOException {
        File topoDir = dir.getParentFile();
        if (topoDir.listFiles().length == 0) {
            Utils.forceDelete(topoDir.getCanonicalPath());
        }
    }

    /**
     * Return a sorted set of java.io.Files that were written by workers that are now dead.
     */
    @VisibleForTesting
    SortedSet<File> getDeadWorkerDirs(int nowSecs, Set<File> logDirs) throws Exception {
        if (logDirs.isEmpty()) {
            return new TreeSet<>();
        } else {
            Set<String> aliveIds = workerLogs.getAliveIds(nowSecs);
            Map<String, File> idToDir = workerLogs.identifyWorkerLogDirs(logDirs);

            return idToDir.entrySet().stream()
                    .filter(entry -> !aliveIds.contains(entry.getKey()))
                    .map(Map.Entry::getValue)
                    .collect(toCollection(TreeSet::new));
        }
    }

    @VisibleForTesting
    Set<File> selectDirsForCleanup(long nowMillis) {
        FileFilter fileFilter = mkFileFilterForLogCleanup(nowMillis);

        return Arrays.stream(logRootDir.listFiles())
                .flatMap(topoDir -> Arrays.stream(topoDir.listFiles(fileFilter)))
                .collect(toCollection(TreeSet::new));
    }

    @VisibleForTesting
    FileFilter mkFileFilterForLogCleanup(long nowMillis) {
        final long cutoffAgeMillis = cleanupCutoffAgeMillis(nowMillis);
        return file -> !file.isFile() && lastModifiedTimeWorkerLogdir(file) <= cutoffAgeMillis;
    }

    /**
     * Return the last modified time for all log files in a worker's log dir.
     * Using stream rather than File.listFiles is to avoid large mem usage
     * when a directory has too many files.
     */
    private long lastModifiedTimeWorkerLogdir(File logDir) {
        long dirModified = logDir.lastModified();

        DirectoryStream<Path> dirStream = null;
        try {
            dirStream = directoryCleaner.getStreamForDirectory(logDir);
        } catch (IOException e) {
            return dirModified;
        }

        if (dirStream == null) {
            return dirModified;
        }

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

    @VisibleForTesting
    long cleanupCutoffAgeMillis(long nowMillis) {
        return nowMillis - (ObjectReader.getInt(stormConf.get(LOGVIEWER_CLEANUP_AGE_MINS)));
    }
}
