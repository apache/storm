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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
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

import org.apache.storm.daemon.logviewer.testsupport.MockDirectoryBuilder;
import org.apache.storm.daemon.logviewer.testsupport.MockFileBuilder;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.generated.LSWorkerHeartbeat;
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

        WorkerLogs workerLogs = new WorkerLogs(conf, null);

        LogCleaner logCleaner = new LogCleaner(conf, workerLogs, mockDirectoryCleaner, null);

        final long nowMillis = Time.currentTimeMillis();
        final long cutoffMillis = logCleaner.cleanupCutoffAgeMillis(nowMillis);
        final long oldMtimeMillis = cutoffMillis - 500;
        final long newMtimeMillis = cutoffMillis + 500;

        List<File> matchingFiles = new ArrayList<>();
        matchingFiles.add(new MockDirectoryBuilder().setDirName("3031").setMtime(oldMtimeMillis).build());
        matchingFiles.add(new MockDirectoryBuilder().setDirName("3032").setMtime(oldMtimeMillis).build());
        matchingFiles.add(new MockDirectoryBuilder().setDirName("7077").setMtime(oldMtimeMillis).build());

        List<File> excludedFiles = new ArrayList<>();
        excludedFiles.add(new MockFileBuilder().setFileName("oldlog-1-2-worker-.log").setMtime(oldMtimeMillis).build());
        excludedFiles.add(new MockFileBuilder().setFileName("newlog-1-2-worker-.log").setMtime(newMtimeMillis).build());
        excludedFiles.add(new MockFileBuilder().setFileName("some-old-file.txt").setMtime(oldMtimeMillis).build());
        excludedFiles.add(new MockFileBuilder().setFileName("olddir-1-2-worker.log").setMtime(newMtimeMillis).build());
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
            when(mockDirectoryCleaner.getStreamForDirectory(any(File.class))).thenAnswer(invocationOnMock -> {
                File file = (File) invocationOnMock.getArguments()[0];
                List<Path> paths = Arrays.stream(file.listFiles()).map(f -> mkMockPath(f)).collect(toList());
                return mkDirectoryStream(paths);
            });
            when(mockDirectoryCleaner.deleteOldestWhileTooLarge(anyListOf(File.class), anyLong(), anyBoolean(), anySetOf(String.class)))
                    .thenCallRealMethod();

            long nowMillis = Time.currentTimeMillis();

            List<File> files1 = Seq.range(0, 10).map(idx -> new MockFileBuilder().setFileName("A" + idx)
                    .setMtime(nowMillis + (100 * idx)).setLength(200).build())
                    .collect(toList());
            List<File> files2 = Seq.range(0, 10).map(idx -> new MockFileBuilder().setFileName("B" + idx)
                    .setMtime(nowMillis + (100 * idx)).setLength(200).build())
                    .collect(toList());
            List<File> files3 = Seq.range(0, 10).map(idx -> new MockFileBuilder().setFileName("C" + idx)
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
            WorkerLogs workerLogs = new WorkerLogs(conf, rootDir);
            LogCleaner logCleaner = new LogCleaner(conf, workerLogs, mockDirectoryCleaner, rootDir);

            List<Integer> deletedFiles = logCleaner.perWorkerDirCleanup(1200);
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
            when(mockDirectoryCleaner.deleteOldestWhileTooLarge(anyListOf(File.class), anyLong(), anyBoolean(), anySetOf(String.class)))
                    .thenCallRealMethod();

            long nowMillis = Time.currentTimeMillis();

            List<File> files1 = Seq.range(0, 10).map(idx -> new MockFileBuilder().setFileName("A" + idx + ".log")
                    .setMtime(nowMillis + (100 * idx)).setLength(200).build())
                    .collect(toList());
            List<File> files2 = Seq.range(0, 10).map(idx -> new MockFileBuilder().setFileName("B" + idx)
                    .setMtime(nowMillis + (100 * idx)).setLength(200).build())
                    .collect(toList());
            List<File> files3 = Seq.range(0, 10).map(idx -> new MockFileBuilder().setFileName("C" + idx)
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
            WorkerLogs stubbedWorkerLogs = new WorkerLogs(conf, rootDir) {
                @Override
                public SortedSet<String> getAliveWorkerDirs() throws Exception {
                    return new TreeSet<>(Collections.singletonList("/workers-artifacts/topo1/port1"));
                }
            };

            LogCleaner logCleaner = new LogCleaner(conf, stubbedWorkerLogs, mockDirectoryCleaner, rootDir);
            int deletedFiles = logCleaner.globalLogCleanup(2400);
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
            WorkerLogs stubbedWorkerLogs = new WorkerLogs(conf, null) {
                @Override
                public Map<String, File> identifyWorkerLogDirs(Set<File> logDirs) {
                    Map<String, File> ret = new HashMap<>();
                    ret.put("42", unexpectedDir1);
                    ret.put("007", expectedDir2);
                    // this tests a directory with no yaml file thus no worker id
                    ret.put("", expectedDir3);

                    return ret;
                }
            };

            LogCleaner logCleaner = new LogCleaner(conf, stubbedWorkerLogs, new DirectoryCleaner(), null);

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
        File mockFile1 = new MockFileBuilder().setFileName("delete-me1").build();
        File mockFile2 = new MockFileBuilder().setFileName("delete-me2").build();

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
            WorkerLogs stubbedWorkerLogs = new WorkerLogs(conf, null);

            LogCleaner logCleaner = new LogCleaner(conf, stubbedWorkerLogs, new DirectoryCleaner(), null) {
                @Override
                Set<File> selectDirsForCleanup(long nowMillis) {
                    return Collections.emptySet();
                }

                @Override
                SortedSet<File> getDeadWorkerDirs(int nowSecs, Set<File> logDirs) throws Exception {
                    SortedSet<File> dirs = new TreeSet<>();
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
