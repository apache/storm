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
import static java.util.stream.Collectors.toList;

import static org.apache.storm.DaemonConfig.LOGVIEWER_CLEANUP_AGE_MINS;
import static org.apache.storm.DaemonConfig.LOGVIEWER_CLEANUP_INTERVAL_SECS;
import static org.apache.storm.DaemonConfig.LOGVIEWER_MAX_PER_WORKER_LOGS_SIZE_MB;
import static org.apache.storm.DaemonConfig.LOGVIEWER_MAX_SUM_WORKER_LOGS_SIZE_MB;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.storm.StormTimer;
import org.apache.storm.metric.StormMetricsRegistry;
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

    private final Timer cleanupRoutineDuration;
    private final Histogram numFilesCleanedUp;
    private final Histogram diskSpaceFreed;
    private final Meter numFileRemovalExceptions;
    private final Meter numCleanupExceptions;
    
    private final Map<String, Object> stormConf;
    private final Integer intervalSecs;
    private final Path logRootDir;
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
     * @param metricsRegistry The logviewer metrics registry
     */
    public LogCleaner(Map<String, Object> stormConf, WorkerLogs workerLogs, DirectoryCleaner directoryCleaner,
                      Path logRootDir, StormMetricsRegistry metricsRegistry) {
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
        //Switch to CachedGauge if this starts to hurt performance
        metricsRegistry.registerGauge("logviewer:worker-log-dir-size", () -> sizeOfDir(logRootDir));
        this.cleanupRoutineDuration = metricsRegistry.registerTimer("logviewer:cleanup-routine-duration-ms");
        this.numFilesCleanedUp = metricsRegistry.registerHistogram("logviewer:num-files-cleaned-up");
        this.diskSpaceFreed = metricsRegistry.registerHistogram("logviewer:disk-space-freed-in-bytes");
        this.numFileRemovalExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_FILE_REMOVAL_EXCEPTIONS);
        this.numCleanupExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_CLEANUP_EXCEPTIONS);
    }
    
    private long sizeOfDir(Path dir) {
        try {
            return Files.walk(dir)
                .filter(Files::isRegularFile)
                .mapToLong(p -> p.toFile().length())
                .sum();
        } catch (IOException e) {
            //This is only used for logging/metrics. Don't crash the process over it.
            LOG.debug("Failed to get size of directory {}", dir);
            return 0;
        }
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
        int numFilesCleaned = 0;
        long diskSpaceCleaned = 0L;
        try (Timer.Context t = cleanupRoutineDuration.time()) {
            final long nowMills = Time.currentTimeMillis();
            Set<Path> oldLogDirs = selectDirsForCleanup(nowMills);

            final long nowSecs = TimeUnit.MILLISECONDS.toSeconds(nowMills);
            SortedSet<Path> deadWorkerDirs = getDeadWorkerDirs((int) nowSecs, oldLogDirs);

            LOG.debug("log cleanup: now={} old log dirs {} dead worker dirs {}", nowSecs,
                    oldLogDirs.stream().map(p -> p.getFileName().toString()).collect(joining(",")),
                    deadWorkerDirs.stream().map(p -> p.getFileName().toString()).collect(joining(",")));

            for (Path dir : deadWorkerDirs) {
                Path path = dir.toAbsolutePath().normalize();
                long sizeInBytes = sizeOfDir(dir);
                LOG.info("Cleaning up: Removing {}, {} KB", path, sizeInBytes * 1e-3);

                try {
                    Utils.forceDelete(path.toString());
                    cleanupEmptyTopoDirectory(dir);
                    numFilesCleaned++;
                    diskSpaceCleaned += sizeInBytes;
                } catch (Exception ex) {
                    numFileRemovalExceptions.mark();
                    LOG.error(ex.getMessage(), ex);
                }
            }

            final List<DeletionMeta> perWorkerDirCleanupMeta = perWorkerDirCleanup(maxPerWorkerLogsSizeMb * 1024 * 1024);
            numFilesCleaned += perWorkerDirCleanupMeta.stream().mapToInt(meta -> meta.deletedFiles).sum();
            diskSpaceCleaned += perWorkerDirCleanupMeta.stream().mapToLong(meta -> meta.deletedSize).sum();
            final DeletionMeta globalLogCleanupMeta = globalLogCleanup(maxSumWorkerLogsSizeMb * 1024 * 1024);
            numFilesCleaned += globalLogCleanupMeta.deletedFiles;
            diskSpaceCleaned += globalLogCleanupMeta.deletedSize;
        } catch (Exception ex) {
            numCleanupExceptions.mark();
            LOG.error("Exception while cleaning up old log.", ex);
        }
        numFilesCleanedUp.update(numFilesCleaned);
        diskSpaceFreed.update(diskSpaceCleaned);
    }

    /**
     * Delete the oldest files in each overloaded worker log dir.
     */
    @VisibleForTesting
    List<DeletionMeta> perWorkerDirCleanup(long size) {
        return workerLogs.getAllWorkerDirs().stream()
                .map(Unchecked.function(dir ->
                        directoryCleaner.deleteOldestWhileTooLarge(Collections.singletonList(dir), size, true, null)))
                .collect(toList());
    }

    /**
     * Delete the oldest files in overloaded worker-artifacts globally.
     */
    @VisibleForTesting
    DeletionMeta globalLogCleanup(long size) throws Exception {
        List<Path> workerDirs = new ArrayList<>(workerLogs.getAllWorkerDirs());
        Set<Path> aliveWorkerDirs = workerLogs.getAliveWorkerDirs();

        return directoryCleaner.deleteOldestWhileTooLarge(workerDirs, size, false, aliveWorkerDirs);
    }

    /**
     * Delete the topo dir if it contains zero port dirs.
     */
    @VisibleForTesting
    void cleanupEmptyTopoDirectory(Path dir) throws IOException {
        Path topoDir = dir.getParent();
        try (Stream<Path> topoDirContent = Files.list(topoDir)) {
            if (!topoDirContent.findAny().isPresent()) {
                Utils.forceDelete(topoDir.toAbsolutePath().normalize().toString());
            }
        }
    }

    /**
     * Return a sorted set of paths that were written by workers that are now dead.
     */
    @VisibleForTesting
    SortedSet<Path> getDeadWorkerDirs(int nowSecs, Set<Path> logDirs) throws Exception {
        if (logDirs.isEmpty()) {
            return new TreeSet<>();
        } else {
            Set<String> aliveIds = workerLogs.getAliveIds(nowSecs);
            return workerLogs.getLogDirs(logDirs, (wid) -> !aliveIds.contains(wid));
        }
    }

    @VisibleForTesting
    Set<Path> selectDirsForCleanup(long nowMillis) {
        Predicate<Path> fileFilter = mkFileFilterForLogCleanup(nowMillis);
        try (Stream<Path> fileList = Files.list(logRootDir)) {
            return fileList
                .flatMap(Unchecked.function(Files::list))
                .filter(fileFilter)
                .collect(Collectors.toCollection(TreeSet::new));
        } catch (IOException e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    @VisibleForTesting
    Predicate<Path> mkFileFilterForLogCleanup(long nowMillis) {
        //It seems safer not to follow symlinks, since we don't expect them here
        return file -> Files.isDirectory(file, LinkOption.NOFOLLOW_LINKS)
            && lastModifiedTimeWorkerLogdir(file) <= cleanupCutoffAgeMillis(nowMillis);
    }

    /**
     * Return the most recent last modified time for all log files in a worker's log dir.
     * Using stream rather than File.listFiles is to avoid large mem usage
     * when a directory has too many files.
     */
    private long lastModifiedTimeWorkerLogdir(Path logDir) {
        try {
            long dirModified = Files.getLastModifiedTime(logDir).toMillis();
            
            try (DirectoryStream<Path> dirStream = directoryCleaner.getStreamForDirectory(logDir)) {
                return StreamSupport.stream(dirStream.spliterator(), false)
                    .map(Unchecked.function(p -> Files.getLastModifiedTime(p).toMillis()))
                    .reduce(dirModified, BinaryOperator.maxBy(Long::compareTo));
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
                return dirModified;
            }
        } catch (IOException e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    @VisibleForTesting
    long cleanupCutoffAgeMillis(long nowMillis) {
        final Integer intervalMins = ObjectReader.getInt(stormConf.get(LOGVIEWER_CLEANUP_AGE_MINS));
        return nowMillis - TimeUnit.MINUTES.toMillis(intervalMins);
    }
}
