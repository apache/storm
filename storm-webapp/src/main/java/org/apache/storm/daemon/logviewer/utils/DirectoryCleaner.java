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

package org.apache.storm.daemon.logviewer.utils;

import com.codahale.metrics.Meter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Pattern;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.metric.StormMetricsRegistry;

import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide methods to help Logviewer to clean up
 * files in directories and to get a list of files without
 * worrying about excessive memory usage.
 */
public class DirectoryCleaner {
    private static final Logger LOG = LoggerFactory.getLogger(DirectoryCleaner.class);
    // used to recognize the pattern of active log files, we may remove the "current" from this list
    private static final Pattern ACTIVE_LOG_PATTERN = Pattern.compile(".*\\.(log|err|out|current|yaml|pid|metrics)$");
    // used to recognize the pattern of some meta files in a worker log directory
    private static final Pattern META_LOG_PATTERN = Pattern.compile(".*\\.(yaml|pid)$");

    private static final int PQ_SIZE = 1024; // max number of files to delete for every round
    private static final int MAX_ROUNDS = 512; // max rounds of scanning the dirs
    public static final int MAX_NUMBER_OF_FILES_FOR_DIR = 1024;

    private final Meter numFileOpenExceptions;

    public DirectoryCleaner(StormMetricsRegistry metricsRegistry) {
        this.numFileOpenExceptions = metricsRegistry.registerMeter(ExceptionMeterNames.NUM_FILE_OPEN_EXCEPTIONS);
    }

    /**
     * Creates DirectoryStream for give directory.
     *
     * @param dir File instance representing specific directory
     * @return DirectoryStream
     */
    public DirectoryStream<Path> getStreamForDirectory(Path dir) throws IOException {
        try {
            return Files.newDirectoryStream(dir);
        } catch (IOException e) {
            numFileOpenExceptions.mark();
            throw e;
        }
    }

    /**
     * If totalSize of files exceeds the either the per-worker quota or global quota,
     * Logviewer deletes oldest inactive log files in a worker directory or in all worker dirs.
     * We use the parameter forPerDir to switch between the two deletion modes.
     *
     * @param dirs the list of directories to be scanned for deletion
     * @param quota the per-dir quota or the total quota for the all directories
     * @param forPerDir if true, deletion happens for a single dir; otherwise, for all directories globally
     * @param activeDirs only for global deletion, we want to skip the active logs in activeDirs
     * @return number of files deleted
     */
    public DeletionMeta deleteOldestWhileTooLarge(List<Path> dirs,
                                                  long quota, boolean forPerDir, Set<Path> activeDirs) throws IOException {
        long totalSize = 0;
        for (Path dir : dirs) {
            try (DirectoryStream<Path> stream = getStreamForDirectory(dir)) {
                for (Path path : stream) {
                    totalSize += Files.size(path);
                }
            }
        }
        LOG.debug("totalSize: {} quota: {}", totalSize, quota);
        long toDeleteSize = totalSize - quota;
        if (toDeleteSize <= 0) {
            return DeletionMeta.EMPTY;
        }

        int deletedFiles = 0;
        long deletedSize = 0;
        // the oldest pq_size files in this directory will be placed in PQ, with the newest at the root
        PriorityQueue<Pair<Path, FileTime>> pq = new PriorityQueue<>(PQ_SIZE, 
            Comparator.comparing((Pair<Path, FileTime> p) -> p.getRight()).reversed());
        int round = 0;
        final Set<Path> excluded = new HashSet<>();
        while (toDeleteSize > 0) {
            LOG.debug("To delete size is {}, start a new round of deletion, round: {}", toDeleteSize, round);
            for (Path dir : dirs) {
                try (DirectoryStream<Path> stream = getStreamForDirectory(dir)) {
                    for (Path path : stream) {
                        if (!excluded.contains(path)) {
                            if (isFileEligibleToSkipDelete(forPerDir, activeDirs, dir, path)) {
                                excluded.add(path);
                            } else {
                                Pair<Path, FileTime> p = Pair.of(path, Files.getLastModifiedTime(path));
                                if (pq.size() < PQ_SIZE) {
                                    pq.offer(p);
                                } else if (p.getRight().toMillis() < pq.peek().getRight().toMillis()) {
                                    pq.poll();
                                    pq.offer(p);
                                }
                            }
                        }
                    }
                }
            }
            if (!pq.isEmpty()) {
                // need to reverse the order of elements in PQ to delete files from oldest to newest
                Stack<Pair<Path, FileTime>> stack = new Stack<>();
                while (!pq.isEmpty()) {
                    stack.push(pq.poll());
                }
                while (!stack.isEmpty() && toDeleteSize > 0) {
                    Pair<Path, FileTime> pair = stack.pop();
                    Path file = pair.getLeft();
                    final String canonicalPath = file.toAbsolutePath().normalize().toString();
                    final long fileSize = Files.size(file);
                    final long lastModified = pair.getRight().toMillis();
                    //Original implementation doesn't actually check if delete succeeded or not.
                    try {
                        Utils.forceDelete(file.toString());
                        LOG.info("Delete file: {}, size: {}, lastModified: {}", canonicalPath, fileSize, lastModified);
                        toDeleteSize -= fileSize;
                        deletedSize += fileSize;
                        deletedFiles++;
                    } catch (IOException e) {
                        excluded.add(file);
                    }
                }
                pq.clear();
                round++;
                if (round >= MAX_ROUNDS) {
                    if (forPerDir) {
                        LOG.warn("Reach the MAX_ROUNDS: {} during per-dir deletion, you may have too many files in "
                                + "a single directory : {}, will delete the rest files in next interval.",
                            MAX_ROUNDS, dirs.get(0).toAbsolutePath().normalize());
                    } else {
                        LOG.warn("Reach the MAX_ROUNDS: {} during global deletion, you may have too many files, "
                            + "will delete the rest files in next interval.", MAX_ROUNDS);
                    }
                    break;
                }
            } else {
                LOG.warn("No more files able to delete this round, but {} is over quota by {} MB",
                    forPerDir ? "this directory" : "root directory", toDeleteSize * 1e-6);
                LOG.warn("No more files eligible to be deleted this round, but {} is over {} quota by {} MB",
                        forPerDir ? "worker directory: " + dirs.get(0).toAbsolutePath().normalize() : "log root directory",
                        forPerDir ? "per-worker" : "global", toDeleteSize * 1e-6);
            }
        }
        return new DeletionMeta(deletedSize, deletedFiles);
    }

    private boolean isFileEligibleToSkipDelete(boolean forPerDir, Set<Path> activeDirs, Path dir, Path file) throws IOException {
        if (forPerDir) {
            return ACTIVE_LOG_PATTERN.matcher(file.getFileName().toString()).matches();
        } else { // for global cleanup
            // for an active worker's dir, make sure for the last "/"
            return activeDirs.contains(dir) ? ACTIVE_LOG_PATTERN.matcher(file.getFileName().toString()).matches() :
                META_LOG_PATTERN.matcher(file.getFileName().toString()).matches();
        }
    }

    /**
     * Lists files in directory.
     * Note that to avoid memory problem, we only return the first 1024 files in a directory.
     *
     * @param dir directory to get file list
     * @return files in directory
     */
    public List<Path> getFilesForDir(Path dir) throws IOException {
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path path : stream) {
                files.add(path);
                if (files.size() >= MAX_NUMBER_OF_FILES_FOR_DIR) {
                    break;
                }
            }
        } catch (IOException e) {
            numFileOpenExceptions.mark();
            throw e;
        }
        return files;
    }

}
