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

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Pattern;

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
    private static final Pattern ACTIVE_LOG_PATTERN = Pattern.compile(".*\\.(log|err|out|current|yaml|pid)$");
    // used to recognize the pattern of some meta files in a worker log directory
    private static final Pattern META_LOG_PATTERN = Pattern.compile(".*\\.(yaml|pid)$");// max number of files to delete for every round

    private static final int PQ_SIZE = 1024;
    private static final int MAX_ROUNDS = 512; // max rounds of scanning the dirs
    public static final int MAX_NUMBER_OF_FILES_FOR_DIR = 1024;

    // not defining this as static is to allow for mocking in tests

    /**
     * Creates DirectoryStream for give directory.
     *
     * @param dir File instance representing specific directory
     * @return DirectoryStream
     */
    public DirectoryStream<Path> getStreamForDirectory(File dir) throws IOException {
        return Files.newDirectoryStream(dir.toPath());
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
    public int deleteOldestWhileTooLarge(List<File> dirs,
                        long quota, boolean forPerDir, Set<String> activeDirs) throws IOException {
        long totalSize = 0;
        int deletedFiles = 0;

        for (File dir : dirs) {
            try (DirectoryStream<Path> stream = getStreamForDirectory(dir)) {
                for (Path path : stream) {
                    File file = path.toFile();
                    totalSize += file.length();
                }
            }
        }

        LOG.debug("totalSize: {} quota: {}", totalSize, quota);
        long toDeleteSize = totalSize - quota;
        if (toDeleteSize <= 0) {
            return deletedFiles;
        }

        Comparator<File> comparator = new Comparator<File>() {
            public int compare(File f1, File f2) {
                if (f1.lastModified() > f2.lastModified()) {
                    return -1;
                } else {
                    return 1;
                }
            }
        };
        // the oldest pq_size files in this directory will be placed in PQ, with the newest at the root
        PriorityQueue<File> pq = new PriorityQueue<File>(PQ_SIZE, comparator);
        int round = 0;
        while (toDeleteSize > 0) {
            LOG.debug("To delete size is {}, start a new round of deletion, round: {}", toDeleteSize, round);
            for (File dir : dirs) {
                try (DirectoryStream<Path> stream = getStreamForDirectory(dir)) {
                    for (Path path : stream) {
                        File file = path.toFile();
                        if (isFileEligibleToSkipDelete(forPerDir, activeDirs, dir, file)) {
                            continue;
                        }
                        if (pq.size() < PQ_SIZE) {
                            pq.offer(file);
                        } else {
                            if (file.lastModified() < pq.peek().lastModified()) {
                                pq.poll();
                                pq.offer(file);
                            }
                        }
                    }
                }
            }
            // need to reverse the order of elements in PQ to delete files from oldest to newest
            Stack<File> stack = new Stack<File>();
            while (!pq.isEmpty()) {
                File file = pq.poll();
                stack.push(file);
            }
            while (!stack.isEmpty() && toDeleteSize > 0) {
                File file = stack.pop();
                toDeleteSize -= file.length();
                LOG.info("Delete file: {}, size: {}, lastModified: {}", file.getCanonicalPath(), file.length(), file.lastModified());
                file.delete();
                deletedFiles++;
            }
            pq.clear();
            round++;
            if (round >= MAX_ROUNDS) {
                if (forPerDir) {
                    LOG.warn("Reach the MAX_ROUNDS: {} during per-dir deletion, you may have too many files in "
                                    + "a single directory : {}, will delete the rest files in next interval.",
                            MAX_ROUNDS, dirs.get(0).getCanonicalPath());
                } else {
                    LOG.warn("Reach the MAX_ROUNDS: {} during global deletion, you may have too many files, "
                            + "will delete the rest files in next interval.", MAX_ROUNDS);
                }
                break;
            }
        }
        return deletedFiles;
    }

    private boolean isFileEligibleToSkipDelete(boolean forPerDir, Set<String> activeDirs, File dir, File file) throws IOException {
        if (forPerDir) {
            if (ACTIVE_LOG_PATTERN.matcher(file.getName()).matches()) {
                return true;
            }
        } else { // for global cleanup
            if (activeDirs.contains(dir.getCanonicalPath())) { // for an active worker's dir, make sure for the last "/"
                if (ACTIVE_LOG_PATTERN.matcher(file.getName()).matches()) {
                    return true;
                }
            } else {
                if (META_LOG_PATTERN.matcher(file.getName()).matches()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Lists files in directory.
     * Note that to avoid memory problem, we only return the first 1024 files in a directory.
     *
     * @param dir directory to get file list
     * @return files in directory
     */
    public static List<File> getFilesForDir(File dir) throws IOException {
        List<File> files = new ArrayList<File>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir.toPath())) {
            for (Path path : stream) {
                files.add(path.toFile());
                if (files.size() >= MAX_NUMBER_OF_FILES_FOR_DIR) {
                    break;
                }
            }
        }
        return files;
    }
}
