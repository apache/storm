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

import static java.util.stream.Collectors.toList;
import static org.apache.storm.Config.SUPERVISOR_WORKER_TIMEOUT_SECS;
import static org.apache.storm.DaemonConfig.LOGVIEWER_CLEANUP_AGE_MINS;
import static org.apache.storm.DaemonConfig.LOGVIEWER_CLEANUP_INTERVAL_SECS;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import java.util.function.Predicate;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.shade.io.netty.util.internal.ThreadLocalRandom;
import org.apache.storm.testing.TmpPath;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.jooq.lambda.Seq;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.collections.Sets;

public class LogCleanerTest {
    
    /**
     * Log file filter selects the correct worker-log dirs for purge.
     */
    @Test
    public void testMkFileFilterForLogCleanup() throws IOException {
        // this is to read default value for other configurations
        Map<String, Object> conf = Utils.readStormConfig();
        conf.put(LOGVIEWER_CLEANUP_AGE_MINS, 60);
        conf.put(LOGVIEWER_CLEANUP_INTERVAL_SECS, 300);

        StormMetricsRegistry metricRegistry = new StormMetricsRegistry();
        WorkerLogs workerLogs = new WorkerLogs(conf, Paths.get(""), metricRegistry);

        LogCleaner logCleaner = new LogCleaner(conf, workerLogs, new DirectoryCleaner(metricRegistry), null, metricRegistry);

        final long nowMillis = Time.currentTimeMillis();
        final long cutoffMillis = logCleaner.cleanupCutoffAgeMillis(nowMillis);
        final long oldMtimeMillis = cutoffMillis - 500;
        final long newMtimeMillis = cutoffMillis + 500;
       
        try (TmpPath testDir = new TmpPath()) {
            Files.createDirectories(testDir.getFile().toPath());

            List<Path> matchingFiles = Arrays.asList(
                createDir(testDir.getFile().toPath(), "3031", oldMtimeMillis),
                createDir(testDir.getFile().toPath(), "3032", oldMtimeMillis),
                createDir(testDir.getFile().toPath(), "7077", oldMtimeMillis)
            );
            List<Path> excludedFiles = Arrays.asList(
                createFile(testDir.getFile().toPath(), "oldlog-1-2-worker-.log", oldMtimeMillis),
                createFile(testDir.getFile().toPath(), "newlog-1-2-worker-.log", newMtimeMillis),
                createFile(testDir.getFile().toPath(), "some-old-file.txt", oldMtimeMillis),
                createFile(testDir.getFile().toPath(), "olddir-1-2-worker.log", newMtimeMillis),
                createFile(testDir.getFile().toPath(), "metadata", newMtimeMillis),
                createFile(testDir.getFile().toPath(), "newdir", newMtimeMillis)
            );

            Predicate<Path> fileFilter = logCleaner.mkFileFilterForLogCleanup(nowMillis);

            matchingFiles.forEach(p -> assertTrue("Missing " + p.getFileName(), fileFilter.test(p)));
            excludedFiles.forEach(p -> assertFalse("Not excluded " + p.getFileName(), fileFilter.test(p)));
        }
    }

    private Path createFile(Path parentDir, String name, long lastModifiedMs) {
        return createFile(parentDir, name, lastModifiedMs, 0);
    }
    
    private Path createFile(Path parentDir, String name, long lastModifiedMs, int sizeBytes) {
        try {
            Path path = Files.createFile(parentDir.resolve(name));
            Files.setLastModifiedTime(path, FileTime.fromMillis(lastModifiedMs));
            if (sizeBytes != 0) {
                byte[] randBytes = new byte[sizeBytes];
                ThreadLocalRandom.current().nextBytes(randBytes);
                Files.write(path, randBytes);
            }
            return path;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
    
    private Path createDir(Path parentDir, String name) {
        return createDir(parentDir, name, Time.currentTimeMillis());
    }
    
    private Path createDir(Path parentDir, String name, long lastModifiedMs) {
        try {
            Path path = Files.createDirectories(parentDir.resolve(name));
            Files.setLastModifiedTime(path, FileTime.fromMillis(lastModifiedMs));
            return path;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * cleaner deletes oldest files in each worker dir if files are larger than per-dir quota.
     */
    @Test
    public void testPerWorkerDirectoryCleanup() throws IOException {
        long nowMillis = Time.currentTimeMillis();

        try (TmpPath testDir = new TmpPath()) {
            Files.createDirectories(testDir.getFile().toPath());
            Path rootDir = createDir(testDir.getFile().toPath(), "workers-artifacts");
            Path topo1Dir = createDir(rootDir, "topo1");
            Path topo2Dir = createDir(rootDir, "topo2");
            Path port1Dir = createDir(topo1Dir, "port1");
            Path port2Dir = createDir(topo1Dir, "port2");
            Path port3Dir = createDir(topo2Dir, "port3");

            Seq.range(0, 10)
                .forEach(idx -> createFile(port1Dir, "A" + idx, nowMillis + 100 * idx, 200));
            Seq.range(0, 10)
                .forEach(idx -> createFile(port2Dir, "B" + idx, nowMillis + 100 * idx, 200));
            Seq.range(0, 10)
                .forEach(idx -> createFile(port3Dir, "C" + idx, nowMillis + 100 * idx, 200));

            Map<String, Object> conf = Utils.readStormConfig();
            StormMetricsRegistry metricRegistry = new StormMetricsRegistry();
            WorkerLogs workerLogs = new WorkerLogs(conf, rootDir, metricRegistry);
            LogCleaner logCleaner = new LogCleaner(conf, workerLogs, new DirectoryCleaner(metricRegistry), rootDir, metricRegistry);

            List<Integer> deletedFiles = logCleaner.perWorkerDirCleanup(1200)
                .stream()
                .map(deletionMeta -> deletionMeta.deletedFiles)
                .collect(toList());
            assertEquals(Integer.valueOf(4), deletedFiles.get(0));
            assertEquals(Integer.valueOf(4), deletedFiles.get(1));
            assertEquals(Integer.valueOf(4), deletedFiles.get(deletedFiles.size() - 1));
        }
    }

    @Test
    public void testGlobalLogCleanup() throws Exception {
        long nowMillis = Time.currentTimeMillis();

        try (TmpPath testDir = new TmpPath()) {
            Files.createDirectories(testDir.getFile().toPath());
            Path rootDir = createDir(testDir.getFile().toPath(), "workers-artifacts");
            Path topo1Dir = createDir(rootDir, "topo1");
            Path topo2Dir = createDir(rootDir, "topo2");
            // note that port1Dir is active worker containing active logs
            Path port1Dir = createDir(topo1Dir, "port1");
            Path port2Dir = createDir(topo1Dir, "port2");
            Path port3Dir = createDir(topo2Dir, "port3");

            Seq.range(0, 10)
                .forEach(idx -> createFile(port1Dir, "A" + idx + ".log", nowMillis + 100 * idx, 200));
            Seq.range(0, 10)
                .forEach(idx -> createFile(port2Dir, "B" + idx, nowMillis + 100 * idx, 200));
            Seq.range(0, 10)
                .forEach(idx -> createFile(port3Dir, "C" + idx, nowMillis + 100 * idx, 200));

            Map<String, Object> conf = Utils.readStormConfig();
            StormMetricsRegistry metricRegistry = new StormMetricsRegistry();
            WorkerLogs stubbedWorkerLogs = new WorkerLogs(conf, rootDir, metricRegistry) {
                @Override
                public SortedSet<Path> getAliveWorkerDirs() {
                    return new TreeSet<>(Collections.singletonList(port1Dir));
                }
            };

            LogCleaner logCleaner = new LogCleaner(conf, stubbedWorkerLogs, new DirectoryCleaner(metricRegistry), rootDir, metricRegistry);
            int deletedFiles = logCleaner.globalLogCleanup(2400).deletedFiles;
            assertEquals(18, deletedFiles);
        }
    }

    /**
     * return directories for workers that are not alive.
     */
    @Test
    public void testGetDeadWorkerDirs() throws Exception {
        Map<String, Object> stormConf = Utils.readStormConfig();
        stormConf.put(SUPERVISOR_WORKER_TIMEOUT_SECS, 5);

        LSWorkerHeartbeat hb = new LSWorkerHeartbeat();
        hb.set_time_secs(1);

        Map<String, LSWorkerHeartbeat> idToHb = Collections.singletonMap("42", hb);
        int nowSecs = 2;
        try (TmpPath testDir = new TmpPath()) {
            Path unexpectedDir1 = createDir(testDir.getFile().toPath(), "dir1");
            Path expectedDir2 = createDir(testDir.getFile().toPath(), "dir2");
            Path expectedDir3 = createDir(testDir.getFile().toPath(), "dir3");
            Set<Path> logDirs = Sets.newSet(unexpectedDir1, expectedDir2, expectedDir3);
            SupervisorUtils mockedSupervisorUtils = mock(SupervisorUtils.class);
            SupervisorUtils.setInstance(mockedSupervisorUtils);

            Map<String, Object> conf = Utils.readStormConfig();
            StormMetricsRegistry metricRegistry = new StormMetricsRegistry();
            WorkerLogs stubbedWorkerLogs = new WorkerLogs(conf, Paths.get(""), metricRegistry) {
                @Override
                public SortedSet<Path> getLogDirs(Set<Path> logDirs, Predicate<String> predicate) {
                    TreeSet<Path> ret = new TreeSet<>();
                    if (predicate.test("42")) {
                        ret.add(unexpectedDir1);
                    }
                    if (predicate.test("007")) {
                        ret.add(expectedDir2);
                    }
                    if (predicate.test("")) {
                        ret.add(expectedDir3);
                    }

                    return ret;
                }
            };

            LogCleaner logCleaner = new LogCleaner(conf, stubbedWorkerLogs, new DirectoryCleaner(metricRegistry), null, metricRegistry);

            when(mockedSupervisorUtils.readWorkerHeartbeatsImpl(anyMap())).thenReturn(idToHb);
            assertEquals(Sets.newSet(expectedDir2, expectedDir3), logCleaner.getDeadWorkerDirs(nowSecs, logDirs));
        } finally {
            SupervisorUtils.resetInstance();
        }
    }

    /**
     * cleanup function forceDeletes files of dead workers.
     */
    @Test
    public void testCleanupFn() throws IOException {
        try (TmpPath dir1 = new TmpPath(); TmpPath dir2 = new TmpPath()) {
            Files.createDirectory(dir1.getFile().toPath());
            Files.createDirectory(dir2.getFile().toPath());

            Map<String, Object> conf = Utils.readStormConfig();
            StormMetricsRegistry metricRegistry = new StormMetricsRegistry();
            WorkerLogs stubbedWorkerLogs = new WorkerLogs(conf, Paths.get(""), metricRegistry);

            LogCleaner logCleaner = new LogCleaner(conf, stubbedWorkerLogs, new DirectoryCleaner(metricRegistry), null, metricRegistry) {
                @Override
                Set<Path> selectDirsForCleanup(long nowMillis) {
                    return Collections.emptySet();
                }

                @Override
                SortedSet<Path> getDeadWorkerDirs(int nowSecs, Set<Path> logDirs) throws Exception {
                    SortedSet<Path> dirs = new TreeSet<>();
                    dirs.add(dir1.getFile().toPath());
                    dirs.add(dir2.getFile().toPath());
                    return dirs;
                }

                @Override
                void cleanupEmptyTopoDirectory(Path dir) throws IOException {
                }
            };

            logCleaner.run();

            assertThat(Files.exists(dir1.getFile().toPath()), is(false));
            assertThat(Files.exists(dir2.getFile().toPath()), is(false));
        }
    }
}
