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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyListOf;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMapOf;
import static org.mockito.ArgumentMatchers.anySetOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import java.util.function.Predicate;
import org.apache.storm.daemon.logviewer.testsupport.MockDirectoryBuilder;
import org.apache.storm.daemon.logviewer.testsupport.MockRemovableFileBuilder;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.jooq.lambda.Seq;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

public class LogCleanerTest {
    /**
     * Log file filter selects the correct worker-log dirs for purge.
     */
    @Test
    public void testMkFileFilterForLogCleanup() throws IOException {
        DirectoryCleaner mockDirectoryCleaner = mock(DirectoryCleaner.class);
        when(mockDirectoryCleaner.getStreamForDirectory(any(File.class))).thenAnswer(invocationOnMock -> {
            File file = (File) invocationOnMock.getArguments()[0];
            List<Path> paths = Arrays.stream(file.listFiles()).map(f -> mkMockPath(f)).collect(toList());
            return mkDirectoryStream(paths);
        });

        // this is to read default value for other configurations
        Map<String, Object> conf = Utils.readStormConfig();
        conf.put(LOGVIEWER_CLEANUP_AGE_MINS, 60);
        conf.put(LOGVIEWER_CLEANUP_INTERVAL_SECS, 300);

        StormMetricsRegistry metricRegistry = new StormMetricsRegistry();
        WorkerLogs workerLogs = new WorkerLogs(conf, null, metricRegistry);

        LogCleaner logCleaner = new LogCleaner(conf, workerLogs, mockDirectoryCleaner, null, metricRegistry);

        final long nowMillis = Time.currentTimeMillis();
        final long cutoffMillis = logCleaner.cleanupCutoffAgeMillis(nowMillis);
        final long oldMtimeMillis = cutoffMillis - 500;
        final long newMtimeMillis = cutoffMillis + 500;

        List<File> matchingFiles = new ArrayList<>();
        matchingFiles.add(new MockDirectoryBuilder().setDirName("3031").setMtime(oldMtimeMillis).build());
        matchingFiles.add(new MockDirectoryBuilder().setDirName("3032").setMtime(oldMtimeMillis).build());
        matchingFiles.add(new MockDirectoryBuilder().setDirName("7077").setMtime(oldMtimeMillis).build());

        List<File> excludedFiles = new ArrayList<>();
        excludedFiles.add(new MockRemovableFileBuilder().setFileName("oldlog-1-2-worker-.log").setMtime(oldMtimeMillis).build());
        excludedFiles.add(new MockRemovableFileBuilder().setFileName("newlog-1-2-worker-.log").setMtime(newMtimeMillis).build());
        excludedFiles.add(new MockRemovableFileBuilder().setFileName("some-old-file.txt").setMtime(oldMtimeMillis).build());
        excludedFiles.add(new MockRemovableFileBuilder().setFileName("olddir-1-2-worker.log").setMtime(newMtimeMillis).build());
        excludedFiles.add(new MockDirectoryBuilder().setDirName("metadata").setMtime(newMtimeMillis).build());
        excludedFiles.add(new MockDirectoryBuilder().setDirName("newdir").setMtime(newMtimeMillis).build());

        FileFilter fileFilter = logCleaner.mkFileFilterForLogCleanup(nowMillis);

        assertTrue(matchingFiles.stream().allMatch(fileFilter::accept));
        assertTrue(excludedFiles.stream().noneMatch(fileFilter::accept));
    }

    /**
     * cleaner deletes oldest files in each worker dir if files are larger than per-dir quota.
     */
    @Test
    public void testPerWorkerDirectoryCleanup() throws IOException {
        Utils prevUtils = null;
        try {
            Utils mockUtils = mock(Utils.class);
            prevUtils = Utils.setInstance(mockUtils);

            DirectoryCleaner mockDirectoryCleaner = mock(DirectoryCleaner.class);
            when(mockDirectoryCleaner.getStreamForDirectory(any())).thenAnswer(invocationOnMock -> {
                File file = (File) invocationOnMock.getArguments()[0];
                List<Path> paths = Arrays.stream(file.listFiles()).map(f -> mkMockPath(f)).collect(toList());
                return mkDirectoryStream(paths);
            });
            when(mockDirectoryCleaner.deleteOldestWhileTooLarge(any(), anyLong(), anyBoolean(), any()))
                    .thenCallRealMethod();

            long nowMillis = Time.currentTimeMillis();

            List<File> files1 = Seq.range(0, 10).map(idx -> new MockRemovableFileBuilder().setFileName("A" + idx)
                    .setMtime(nowMillis + (100 * idx)).setLength(200).build())
                    .collect(toList());
            List<File> files2 = Seq.range(0, 10).map(idx -> new MockRemovableFileBuilder().setFileName("B" + idx)
                    .setMtime(nowMillis + (100 * idx)).setLength(200).build())
                    .collect(toList());
            List<File> files3 = Seq.range(0, 10).map(idx -> new MockRemovableFileBuilder().setFileName("C" + idx)
                    .setMtime(nowMillis + (100 * idx)).setLength(200).build())
                    .collect(toList());
            File port1Dir = new MockDirectoryBuilder().setDirName("/workers-artifacts/topo1/port1")
                    .setFiles(files1.toArray(new File[]{})).build();
            File port2Dir = new MockDirectoryBuilder().setDirName("/workers-artifacts/topo1/port2")
                    .setFiles(files2.toArray(new File[]{})).build();
            File port3Dir = new MockDirectoryBuilder().setDirName("/workers-artifacts/topo2/port3")
                    .setFiles(files3.toArray(new File[]{})).build();

            File[] topo1Files = new File[] { port1Dir, port2Dir };
            File[] topo2Files = new File[] { port3Dir };
            File topo1Dir = new MockDirectoryBuilder().setDirName("/workers-artifacts/topo1")
                    .setFiles(topo1Files).build();
            File topo2Dir = new MockDirectoryBuilder().setDirName("/workers-artifacts/topo2")
                    .setFiles(topo2Files).build();

            File[] rootFiles = new File[] { topo1Dir, topo2Dir };
            File rootDir = new MockDirectoryBuilder().setDirName("/workers-artifacts")
                    .setFiles(rootFiles).build();

            Map<String, Object> conf = Utils.readStormConfig();
            StormMetricsRegistry metricRegistry = new StormMetricsRegistry();
            WorkerLogs workerLogs = new WorkerLogs(conf, rootDir, metricRegistry);
            LogCleaner logCleaner = new LogCleaner(conf, workerLogs, mockDirectoryCleaner, rootDir, metricRegistry);

            List<Integer> deletedFiles = logCleaner.perWorkerDirCleanup(1200)
                .stream()
                .map(deletionMeta -> deletionMeta.deletedFiles)
                .collect(toList());
            assertEquals(Integer.valueOf(4), deletedFiles.get(0));
            assertEquals(Integer.valueOf(4), deletedFiles.get(1));
            assertEquals(Integer.valueOf(4), deletedFiles.get(deletedFiles.size() - 1));
        } finally {
            Utils.setInstance(prevUtils);
        }
    }

    @Test
    public void testGlobalLogCleanup() throws Exception {
        Utils prevUtils = null;
        try {
            Utils mockUtils = mock(Utils.class);
            prevUtils = Utils.setInstance(mockUtils);

            DirectoryCleaner mockDirectoryCleaner = mock(DirectoryCleaner.class);
            when(mockDirectoryCleaner.getStreamForDirectory(any(File.class))).thenAnswer(invocationOnMock -> {
                File file = (File) invocationOnMock.getArguments()[0];
                List<Path> paths = Arrays.stream(file.listFiles()).map(f -> mkMockPath(f)).collect(toList());
                return mkDirectoryStream(paths);
            });
            when(mockDirectoryCleaner.deleteOldestWhileTooLarge(anyListOf(File.class), anyLong(), anyBoolean(), anySetOf(File.class)))
                    .thenCallRealMethod();

            long nowMillis = Time.currentTimeMillis();

            List<File> files1 = Seq.range(0, 10).map(idx -> new MockRemovableFileBuilder().setFileName("A" + idx + ".log")
                    .setMtime(nowMillis + (100 * idx)).setLength(200).build())
                    .collect(toList());
            List<File> files2 = Seq.range(0, 10).map(idx -> new MockRemovableFileBuilder().setFileName("B" + idx)
                    .setMtime(nowMillis + (100 * idx)).setLength(200).build())
                    .collect(toList());
            List<File> files3 = Seq.range(0, 10).map(idx -> new MockRemovableFileBuilder().setFileName("C" + idx)
                    .setMtime(nowMillis + (100 * idx)).setLength(200).build())
                    .collect(toList());

            // note that port1Dir is active worker containing active logs
            File port1Dir = new MockDirectoryBuilder().setDirName("/workers-artifacts/topo1/port1")
                    .setFiles(files1.toArray(new File[]{})).build();
            File port2Dir = new MockDirectoryBuilder().setDirName("/workers-artifacts/topo1/port2")
                    .setFiles(files2.toArray(new File[]{})).build();
            File port3Dir = new MockDirectoryBuilder().setDirName("/workers-artifacts/topo2/port3")
                    .setFiles(files3.toArray(new File[]{})).build();

            File[] topo1Files = new File[] { port1Dir, port2Dir };
            File[] topo2Files = new File[] { port3Dir };
            File topo1Dir = new MockDirectoryBuilder().setDirName("/workers-artifacts/topo1")
                    .setFiles(topo1Files).build();
            File topo2Dir = new MockDirectoryBuilder().setDirName("/workers-artifacts/topo2")
                    .setFiles(topo2Files).build();

            File[] rootFiles = new File[] { topo1Dir, topo2Dir };
            File rootDir = new MockDirectoryBuilder().setDirName("/workers-artifacts")
                    .setFiles(rootFiles).build();

            Map<String, Object> conf = Utils.readStormConfig();
            StormMetricsRegistry metricRegistry = new StormMetricsRegistry();
            WorkerLogs stubbedWorkerLogs = new WorkerLogs(conf, rootDir, metricRegistry) {
                @Override
                public SortedSet<File> getAliveWorkerDirs() {
                    return new TreeSet<>(Collections.singletonList(new File("/workers-artifacts/topo1/port1")));
                }
            };

            LogCleaner logCleaner = new LogCleaner(conf, stubbedWorkerLogs, mockDirectoryCleaner, rootDir, metricRegistry);
            int deletedFiles = logCleaner.globalLogCleanup(2400).deletedFiles;
            assertEquals(18, deletedFiles);
        } finally {
            Utils.setInstance(prevUtils);
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
        File unexpectedDir1 = new MockDirectoryBuilder().setDirName("dir1").build();
        File expectedDir2 = new MockDirectoryBuilder().setDirName("dir2").build();
        File expectedDir3 = new MockDirectoryBuilder().setDirName("dir3").build();
        Set<File> logDirs = Sets.newSet(unexpectedDir1, expectedDir2, expectedDir3);

        try {
            SupervisorUtils mockedSupervisorUtils = mock(SupervisorUtils.class);
            SupervisorUtils.setInstance(mockedSupervisorUtils);

            Map<String, Object> conf = Utils.readStormConfig();
            StormMetricsRegistry metricRegistry = new StormMetricsRegistry();
            WorkerLogs stubbedWorkerLogs = new WorkerLogs(conf, null, metricRegistry) {
                @Override
                public SortedSet<File> getLogDirs(Set<File> logDirs, Predicate<String> predicate) {
                    TreeSet<File> ret = new TreeSet<>();
                    if (predicate.test("42")) {
                        ret.add(unexpectedDir1);
                    }
                    if (predicate.test("007")) {
                        ret.add(expectedDir2);
                    }
                    if(predicate.test("")) {
                        ret.add(expectedDir3);
                    }

                    return ret;
                }
            };

            LogCleaner logCleaner = new LogCleaner(conf, stubbedWorkerLogs, new DirectoryCleaner(metricRegistry), null, metricRegistry);

            when(mockedSupervisorUtils.readWorkerHeartbeatsImpl(anyMapOf(String.class, Object.class))).thenReturn(idToHb);
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
        File mockFile1 = new MockRemovableFileBuilder().setFileName("delete-me1").build();
        File mockFile2 = new MockRemovableFileBuilder().setFileName("delete-me2").build();

        Utils prevUtils = null;
        try {
            Utils mockUtils = mock(Utils.class);
            prevUtils = Utils.setInstance(mockUtils);

            List<String> forceDeleteArgs = new ArrayList<>();
            doAnswer(invocationOnMock -> {
                String path = (String) invocationOnMock.getArguments()[0];
                forceDeleteArgs.add(path);
                return null;
            }).when(mockUtils).forceDelete(anyString());


            Map<String, Object> conf = Utils.readStormConfig();
            StormMetricsRegistry metricRegistry = new StormMetricsRegistry();
            WorkerLogs stubbedWorkerLogs = new WorkerLogs(conf, null, metricRegistry);

            LogCleaner logCleaner = new LogCleaner(conf, stubbedWorkerLogs, new DirectoryCleaner(metricRegistry), null, metricRegistry) {
                @Override
                Set<File> selectDirsForCleanup(long nowMillis) {
                    return Collections.emptySet();
                }

                @Override
                SortedSet<File> getDeadWorkerDirs(int nowSecs, Set<File> logDirs) throws Exception {
                    SortedSet<File> dirs = new TreeSet<>();
                    //Test maybe flawed, as those weren't actually mocked dirs but mocked regular files
                    dirs.add(mockFile1);
                    dirs.add(mockFile2);
                    return dirs;
                }

                @Override
                void cleanupEmptyTopoDirectory(File dir) throws IOException {
                }
            };

            logCleaner.run();

            assertEquals(2, forceDeleteArgs.size());
            assertEquals(mockFile1.getCanonicalPath(), forceDeleteArgs.get(0));
            assertEquals(mockFile2.getCanonicalPath(), forceDeleteArgs.get(1));
        } finally {
            Utils.setInstance(prevUtils);
        }
    }

    private Path mkMockPath(File file) {
        Path mockPath = mock(Path.class);
        when(mockPath.toFile()).thenReturn(file);
        return mockPath;
    }

    private DirectoryStream<Path> mkDirectoryStream(List<Path> listOfPaths) {
        return new DirectoryStream<Path>() {
            @Override
            public Iterator<Path> iterator() {
                return listOfPaths.iterator();
            }

            @Override
            public void close() throws IOException {
            }
        };
    }
}
